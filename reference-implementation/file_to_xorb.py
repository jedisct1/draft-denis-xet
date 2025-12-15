#!/usr/bin/env python3
"""Convert a large file into XET xorbs with compression and deduplication.

This tool chunks a file using content-defined chunking (Gearhash),
deduplicates chunks, and produces xorb files with LZ4 compression.
"""

import argparse
import hashlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from chunking import chunk_stream
from hashing import (
    compute_chunk_hash,
    compute_file_hash,
    compute_verification_hash,
    hash_to_string,
    is_global_dedup_eligible,
)
from xorb import XorbBuilder, Xorb, serialize_xorb
from shard import (
    ShardBuilder,
    FileDataSequenceEntry,
    serialize_shard_for_upload,
)
from constants import (
    COMPRESSION_LZ4,
    MAX_XORB_SIZE,
    MAX_XORB_CHUNKS,
)


@dataclass
class ChunkLocation:
    """Tracks where a chunk is stored."""

    xorb_index: int
    chunk_index: int


@dataclass
class XorbInfo:
    """Information about a built xorb."""

    xorb: Xorb
    serialized: bytes
    chunk_hashes: list[bytes]
    chunk_sizes: list[int]


@dataclass
class FileProcessingResult:
    """Result of processing a file into xorbs."""

    file_hash: bytes
    file_size: int
    sha256_hash: bytes
    chunk_count: int
    unique_chunk_count: int
    xorbs: list[XorbInfo]
    chunk_locations: list[ChunkLocation]
    dedup_savings: int


class XorbCreator:
    """Creates xorbs from a file with deduplication."""

    def __init__(
        self,
        compression_type: int = COMPRESSION_LZ4,
        max_xorb_size: int = MAX_XORB_SIZE,
        max_xorb_chunks: int = MAX_XORB_CHUNKS,
        verbose: bool = False,
    ):
        self.compression_type = compression_type
        self.max_xorb_size = max_xorb_size
        self.max_xorb_chunks = max_xorb_chunks
        self.verbose = verbose

        self.chunk_dedup: dict[bytes, ChunkLocation] = {}
        self.xorbs: list[XorbInfo] = []
        self.current_builder: Optional[XorbBuilder] = None
        self.current_chunk_hashes: list[bytes] = []
        self.current_chunk_sizes: list[int] = []

    def _log(self, msg: str):
        if self.verbose:
            print(f"  {msg}", file=sys.stderr)

    def _new_builder(self):
        """Create a new xorb builder."""
        self.current_builder = XorbBuilder(
            max_size=self.max_xorb_size,
            max_chunks=self.max_xorb_chunks,
            compression_type=self.compression_type,
        )
        self.current_chunk_hashes = []
        self.current_chunk_sizes = []

    def _finalize_current_xorb(self):
        """Finalize the current xorb and add to list."""
        if self.current_builder and not self.current_builder.is_empty():
            xorb = self.current_builder.build()
            serialized = serialize_xorb(xorb, self.compression_type)
            xorb_info = XorbInfo(
                xorb=xorb,
                serialized=serialized,
                chunk_hashes=self.current_chunk_hashes[:],
                chunk_sizes=self.current_chunk_sizes[:],
            )
            self.xorbs.append(xorb_info)
            self._log(
                f"Xorb #{len(self.xorbs)}: {len(xorb.chunks)} chunks, "
                f"{len(serialized):,} bytes serialized"
            )
        self.current_builder = None

    def _add_chunk_to_xorb(self, chunk_data: bytes, chunk_hash: bytes) -> ChunkLocation:
        """Add a unique chunk to the current xorb, creating new xorb if needed."""
        if self.current_builder is None:
            self._new_builder()

        if not self.current_builder.can_add(chunk_data):
            self._finalize_current_xorb()
            self._new_builder()

        chunk_index = len(self.current_chunk_hashes)
        self.current_builder.add(chunk_data, chunk_hash)
        self.current_chunk_hashes.append(chunk_hash)
        self.current_chunk_sizes.append(len(chunk_data))

        return ChunkLocation(
            xorb_index=len(self.xorbs),
            chunk_index=chunk_index,
        )

    def process_file(self, filepath: str) -> FileProcessingResult:
        """Process a file into xorbs with deduplication."""
        filepath = Path(filepath)
        file_size = filepath.stat().st_size

        self._log(f"Processing: {filepath}")
        self._log(f"File size: {file_size:,} bytes")

        all_chunk_hashes: list[bytes] = []
        all_chunk_sizes: list[int] = []
        chunk_locations: list[ChunkLocation] = []
        dedup_savings = 0
        sha256 = hashlib.sha256()

        with open(filepath, "rb") as f:
            chunk_count = 0
            for chunk in chunk_stream(f):
                chunk_hash = compute_chunk_hash(chunk.data)
                all_chunk_hashes.append(chunk_hash)
                all_chunk_sizes.append(chunk.size)
                sha256.update(chunk.data)

                if chunk_hash in self.chunk_dedup:
                    location = self.chunk_dedup[chunk_hash]
                    dedup_savings += chunk.size
                    self._log(
                        f"  Chunk {chunk_count}: DEDUP (saved {chunk.size:,} bytes)"
                    )
                else:
                    location = self._add_chunk_to_xorb(chunk.data, chunk_hash)
                    self.chunk_dedup[chunk_hash] = location

                chunk_locations.append(location)
                chunk_count += 1

                if chunk_count % 100 == 0:
                    self._log(f"  Processed {chunk_count} chunks...")

        self._finalize_current_xorb()

        file_hash = compute_file_hash(all_chunk_hashes, all_chunk_sizes)

        self._log(f"Total chunks: {chunk_count}")
        self._log(f"Unique chunks: {len(self.chunk_dedup)}")
        self._log(f"Dedup savings: {dedup_savings:,} bytes")
        self._log(f"Xorbs created: {len(self.xorbs)}")

        return FileProcessingResult(
            file_hash=file_hash,
            file_size=file_size,
            sha256_hash=sha256.digest(),
            chunk_count=chunk_count,
            unique_chunk_count=len(self.chunk_dedup),
            xorbs=self.xorbs,
            chunk_locations=chunk_locations,
            dedup_savings=dedup_savings,
        )


def build_reconstruction_terms(
    result: FileProcessingResult,
) -> tuple[list[FileDataSequenceEntry], list[bytes]]:
    """Build file reconstruction terms from chunk locations.

    Groups consecutive chunks from the same xorb into single terms.
    Also computes verification hashes for each term.
    """
    if not result.chunk_locations:
        return [], []

    terms: list[FileDataSequenceEntry] = []
    all_chunk_hashes: list[bytes] = []

    for loc in result.chunk_locations:
        xorb_info = result.xorbs[loc.xorb_index]
        all_chunk_hashes.append(xorb_info.chunk_hashes[loc.chunk_index])

    current_xorb_idx = result.chunk_locations[0].xorb_index
    current_start = result.chunk_locations[0].chunk_index
    current_end = current_start + 1
    current_bytes = result.xorbs[current_xorb_idx].chunk_sizes[current_start]

    for i, loc in enumerate(result.chunk_locations[1:], start=1):
        xorb_info = result.xorbs[loc.xorb_index]

        same_xorb = loc.xorb_index == current_xorb_idx
        consecutive = loc.chunk_index == current_end

        if same_xorb and consecutive:
            current_end += 1
            current_bytes += xorb_info.chunk_sizes[loc.chunk_index]
        else:
            xorb_hash = result.xorbs[current_xorb_idx].xorb.xorb_hash
            terms.append(
                FileDataSequenceEntry(
                    cas_hash=xorb_hash,
                    cas_flags=0,
                    unpacked_segment_bytes=current_bytes,
                    chunk_index_start=current_start,
                    chunk_index_end=current_end,
                )
            )
            current_xorb_idx = loc.xorb_index
            current_start = loc.chunk_index
            current_end = current_start + 1
            current_bytes = xorb_info.chunk_sizes[loc.chunk_index]

    xorb_hash = result.xorbs[current_xorb_idx].xorb.xorb_hash
    terms.append(
        FileDataSequenceEntry(
            cas_hash=xorb_hash,
            cas_flags=0,
            unpacked_segment_bytes=current_bytes,
            chunk_index_start=current_start,
            chunk_index_end=current_end,
        )
    )

    verification_hashes: list[bytes] = []
    file_chunk_idx = 0
    for term in terms:
        chunk_range_count = term.chunk_index_end - term.chunk_index_start
        range_end = file_chunk_idx + chunk_range_count
        vh = compute_verification_hash(all_chunk_hashes, file_chunk_idx, range_end)
        verification_hashes.append(vh)
        file_chunk_idx = range_end

    return terms, verification_hashes


def write_xorbs(result: FileProcessingResult, output_dir: Path, verbose: bool = False):
    """Write xorb files to output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for i, xorb_info in enumerate(result.xorbs):
        xorb_hash_str = hash_to_string(xorb_info.xorb.xorb_hash)
        xorb_path = output_dir / f"{xorb_hash_str}.xorb"
        with open(xorb_path, "wb") as f:
            f.write(xorb_info.serialized)
        if verbose:
            print(f"  Written: {xorb_path.name} ({len(xorb_info.serialized):,} bytes)")


def write_shard(result: FileProcessingResult, output_path: Path, verbose: bool = False):
    """Write shard metadata file."""
    terms, verification_hashes = build_reconstruction_terms(result)

    builder = ShardBuilder()

    for i, xorb_info in enumerate(result.xorbs):
        chunk_hashes = xorb_info.chunk_hashes
        chunk_sizes = xorb_info.chunk_sizes

        dedup_eligible = [
            is_global_dedup_eligible(h, i == 0) for i, h in enumerate(chunk_hashes)
        ]

        builder.add_cas_block(
            cas_hash=xorb_info.xorb.xorb_hash,
            chunk_hashes=chunk_hashes,
            chunk_sizes=chunk_sizes,
            serialized_size=len(xorb_info.serialized),
            dedup_eligible=dedup_eligible,
        )

    builder.add_file(
        file_hash=result.file_hash,
        terms=terms,
        verification_hashes=verification_hashes,
        sha256_hash=result.sha256_hash,
    )

    shard = builder.build()
    shard_data = serialize_shard_for_upload(shard)

    with open(output_path, "wb") as f:
        f.write(shard_data)

    if verbose:
        print(f"  Written: {output_path.name} ({len(shard_data):,} bytes)")


def main():
    parser = argparse.ArgumentParser(
        description="Convert a file into XET xorbs with compression and deduplication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s large_file.bin -o ./output
  %(prog)s data.tar -o ./xorbs -v
  %(prog)s --no-shard model.safetensors -o ./cas
        """,
    )
    parser.add_argument("file", help="Input file to process")
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output directory for xorb files",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--no-shard",
        action="store_true",
        help="Don't write shard metadata file",
    )
    parser.add_argument(
        "--compression",
        choices=["none", "lz4", "lz4-grouped"],
        default="lz4",
        help="Compression type (default: lz4)",
    )

    args = parser.parse_args()

    compression_map = {
        "none": 0,
        "lz4": 1,
        "lz4-grouped": 2,
    }
    compression_type = compression_map[args.compression]

    if not os.path.isfile(args.file):
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output)

    creator = XorbCreator(
        compression_type=compression_type,
        verbose=args.verbose,
    )

    if args.verbose:
        print("Processing file...", file=sys.stderr)

    result = creator.process_file(args.file)

    file_hash_str = hash_to_string(result.file_hash)
    sha256_hex = result.sha256_hash.hex()

    print(f"\nFile hash:     {file_hash_str}")
    print(f"SHA-256:       {sha256_hex}")
    print(f"File size:     {result.file_size:,} bytes")
    print(f"Chunks:        {result.chunk_count}")
    print(f"Unique chunks: {result.unique_chunk_count}")
    print(f"Xorbs:         {len(result.xorbs)}")

    if result.dedup_savings > 0:
        ratio = result.dedup_savings / result.file_size * 100
        print(f"Dedup savings: {result.dedup_savings:,} bytes ({ratio:.1f}%)")

    total_xorb_size = sum(len(x.serialized) for x in result.xorbs)
    compression_ratio = (
        (1 - total_xorb_size / result.file_size) * 100 if result.file_size > 0 else 0
    )
    print(
        f"Total xorb size: {total_xorb_size:,} bytes ({compression_ratio:.1f}% compression)"
    )

    if args.verbose:
        print("\nWriting xorbs...", file=sys.stderr)

    write_xorbs(result, output_dir, verbose=args.verbose)

    if not args.no_shard:
        shard_path = output_dir / f"{file_hash_str}.shard"
        if args.verbose:
            print("Writing shard...", file=sys.stderr)
        write_shard(result, shard_path, verbose=args.verbose)

    print(f"\nOutput directory: {output_dir}")


if __name__ == "__main__":
    main()
