"""XET Upload and Download Protocols

Implements the complete upload and download procedures for XET.
"""

import hashlib
from dataclasses import dataclass, field
from typing import Optional, Callable

from chunking import chunk_data
from hashing import (
    compute_chunk_hash,
    compute_file_hash,
    compute_verification_hash,
    compute_xorb_hash,
    is_global_dedup_eligible,
)
from xorb import XorbBuilder, serialize_xorb, extract_chunk_range
from shard import (
    ShardBuilder,
    FileDataSequenceEntry,
    serialize_shard_for_upload,
)
from api import CASClient, DeduplicationCache


@dataclass
class ChunkInfo:
    """Information about a chunk."""

    data: bytes
    hash: bytes
    size: int
    file_index: int  # Index within the file
    is_first_chunk: bool


@dataclass
class ChunkLocation:
    """Location of a chunk (either in a new xorb or existing one)."""

    xorb_hash: bytes
    chunk_index: int
    size: int
    is_new: bool = True


@dataclass
class XorbInfo:
    """Information about a xorb to be uploaded."""

    xorb_hash: bytes
    serialized_data: bytes
    chunk_hashes: list[bytes]
    chunk_sizes: list[int]


@dataclass
class FileUploadInfo:
    """Information for uploading a single file."""

    data: bytes
    chunks: list[ChunkInfo] = field(default_factory=list)
    chunk_locations: list[ChunkLocation] = field(default_factory=list)
    file_hash: bytes = b""
    sha256_hash: bytes = b""


class UploadSession:
    """Manages the upload of one or more files."""

    def __init__(
        self,
        client: CASClient,
        enable_global_dedup: bool = True,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ):
        """Initialize upload session.

        Args:
            client: CAS API client.
            enable_global_dedup: Whether to query global dedup API.
            progress_callback: Optional callback(stage, current, total).
        """
        self.client = client
        self.enable_global_dedup = enable_global_dedup
        self.progress_callback = progress_callback

        # Session state
        self.files: list[FileUploadInfo] = []
        self.xorbs: list[XorbInfo] = []
        self.dedup_cache = DeduplicationCache()
        self._local_chunk_locations: dict[bytes, ChunkLocation] = {}

    def add_file(self, data: bytes) -> int:
        """Add a file to the upload session.

        Args:
            data: File contents.

        Returns:
            Index of the file in the session.
        """
        info = FileUploadInfo(data=data)

        # Compute SHA-256 for metadata
        info.sha256_hash = hashlib.sha256(data).digest()

        # Chunk the file
        chunks = chunk_data(data)
        for i, chunk in enumerate(chunks):
            chunk_hash = compute_chunk_hash(chunk.data)
            info.chunks.append(
                ChunkInfo(
                    data=chunk.data,
                    hash=chunk_hash,
                    size=chunk.size,
                    file_index=i,
                    is_first_chunk=(i == 0),
                )
            )

        # Compute file hash
        chunk_hashes = [c.hash for c in info.chunks]
        chunk_sizes = [c.size for c in info.chunks]
        info.file_hash = compute_file_hash(chunk_hashes, chunk_sizes)

        self.files.append(info)
        return len(self.files) - 1

    def _report_progress(self, stage: str, current: int, total: int):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(stage, current, total)

    def _perform_deduplication(self):
        """Perform deduplication for all chunks."""
        # Collect all unique chunks across files
        all_chunks: list[tuple[int, ChunkInfo]] = []  # (file_idx, chunk)
        seen_hashes: set[bytes] = set()

        for file_idx, file_info in enumerate(self.files):
            for chunk in file_info.chunks:
                if chunk.hash not in seen_hashes:
                    all_chunks.append((file_idx, chunk))
                    seen_hashes.add(chunk.hash)

        total = len(all_chunks)
        for i, (file_idx, chunk) in enumerate(all_chunks):
            self._report_progress("dedup", i + 1, total)

            # Check local session cache first
            if chunk.hash in self._local_chunk_locations:
                continue

            # Check global dedup if eligible and enabled
            if self.enable_global_dedup:
                if is_global_dedup_eligible(chunk.hash, chunk.is_first_chunk):
                    shard = self.client.query_chunk_dedup(chunk.hash)
                    if shard:
                        # Found in global dedup - add to cache
                        self.dedup_cache.add_from_shard(shard)

    def _form_xorbs(self):
        """Form xorbs from non-deduplicated chunks."""
        builder = XorbBuilder()

        def flush_builder():
            if builder.is_empty():
                return

            xorb = builder.build()
            serialized = serialize_xorb(xorb)

            chunk_hashes = [c.chunk_hash for c in xorb.chunks]
            chunk_sizes = [len(c.data) for c in xorb.chunks]
            xorb_hash = compute_xorb_hash(chunk_hashes, chunk_sizes)

            # Record locations for all chunks in this xorb
            for idx, c in enumerate(xorb.chunks):
                loc = ChunkLocation(
                    xorb_hash=xorb_hash, chunk_index=idx, size=len(c.data), is_new=True
                )
                self._local_chunk_locations[c.chunk_hash] = loc

            self.xorbs.append(
                XorbInfo(
                    xorb_hash=xorb_hash,
                    serialized_data=serialized,
                    chunk_hashes=chunk_hashes,
                    chunk_sizes=chunk_sizes,
                )
            )

            builder.reset()

        # Process all files in order
        for file_info in self.files:
            for chunk in file_info.chunks:
                # Skip if already located (deduplicated or in earlier xorb)
                if chunk.hash in self._local_chunk_locations:
                    continue

                # Check dedup cache
                cached = self.dedup_cache.get(chunk.hash)
                if cached:
                    xorb_hash, chunk_idx = cached
                    self._local_chunk_locations[chunk.hash] = ChunkLocation(
                        xorb_hash=xorb_hash,
                        chunk_index=chunk_idx,
                        size=chunk.size,
                        is_new=False,
                    )
                    continue

                # Try to add to current xorb
                if not builder.add(chunk.data, chunk.hash):
                    # Current xorb is full, flush it
                    flush_builder()
                    builder.add(chunk.data, chunk.hash)

        # Flush any remaining chunks
        flush_builder()

    def _build_file_terms(
        self, file_info: FileUploadInfo
    ) -> list[FileDataSequenceEntry]:
        """Build reconstruction terms for a file.

        Groups consecutive chunks from the same xorb into single terms.
        """
        terms = []
        current_xorb: Optional[bytes] = None
        current_start = 0
        current_end = 0
        current_size = 0

        for chunk in file_info.chunks:
            loc = self._local_chunk_locations[chunk.hash]

            if current_xorb is None:
                # Start new term
                current_xorb = loc.xorb_hash
                current_start = loc.chunk_index
                current_end = loc.chunk_index + 1
                current_size = loc.size
            elif loc.xorb_hash == current_xorb and loc.chunk_index == current_end:
                # Extend current term
                current_end = loc.chunk_index + 1
                current_size += loc.size
            else:
                # Emit current term, start new one
                terms.append(
                    FileDataSequenceEntry(
                        cas_hash=current_xorb,
                        cas_flags=0,
                        unpacked_segment_bytes=current_size,
                        chunk_index_start=current_start,
                        chunk_index_end=current_end,
                    )
                )
                current_xorb = loc.xorb_hash
                current_start = loc.chunk_index
                current_end = loc.chunk_index + 1
                current_size = loc.size

        # Emit final term
        if current_xorb is not None:
            terms.append(
                FileDataSequenceEntry(
                    cas_hash=current_xorb,
                    cas_flags=0,
                    unpacked_segment_bytes=current_size,
                    chunk_index_start=current_start,
                    chunk_index_end=current_end,
                )
            )

        return terms

    def _compute_verification_hashes(
        self, file_info: FileUploadInfo, terms: list[FileDataSequenceEntry]
    ) -> list[bytes]:
        """Compute verification hashes for file terms."""
        # Build a map from (xorb_hash, chunk_index) to chunk_hash
        xorb_chunks: dict[bytes, list[bytes]] = {}
        for xorb in self.xorbs:
            xorb_chunks[xorb.xorb_hash] = xorb.chunk_hashes

        # For existing xorbs (from dedup), we need the file's chunk hashes
        # indexed by their location
        file_chunk_map: dict[tuple[bytes, int], bytes] = {}
        for chunk in file_info.chunks:
            loc = self._local_chunk_locations[chunk.hash]
            file_chunk_map[(loc.xorb_hash, loc.chunk_index)] = chunk.hash

        verification_hashes = []
        for term in terms:
            # Get chunk hashes for this term's range
            if term.cas_hash in xorb_chunks:
                chunk_hashes = xorb_chunks[term.cas_hash][
                    term.chunk_index_start : term.chunk_index_end
                ]
            else:
                # Deduplicated xorb - use file's chunk hashes
                chunk_hashes = []
                for idx in range(term.chunk_index_start, term.chunk_index_end):
                    chunk_hashes.append(file_chunk_map[(term.cas_hash, idx)])

            vh = compute_verification_hash(chunk_hashes, 0, len(chunk_hashes))
            verification_hashes.append(vh)

        return verification_hashes

    def upload(self) -> list[bytes]:
        """Execute the upload.

        Returns:
            List of file hashes for uploaded files.

        Raises:
            Various exceptions on upload errors.
        """
        # Step 1: Chunking (already done in add_file)

        # Step 2: Deduplication
        self._perform_deduplication()

        # Step 3: Xorb formation
        self._form_xorbs()

        # Step 4: Upload xorbs
        total_xorbs = len(self.xorbs)
        for i, xorb in enumerate(self.xorbs):
            self._report_progress("xorb_upload", i + 1, total_xorbs)
            self.client.upload_xorb(xorb.xorb_hash, xorb.serialized_data)

        # Step 5: Build shard
        shard_builder = ShardBuilder()

        # Add file blocks
        for file_info in self.files:
            terms = self._build_file_terms(file_info)
            verification_hashes = self._compute_verification_hashes(file_info, terms)

            shard_builder.add_file(
                file_hash=file_info.file_hash,
                terms=terms,
                verification_hashes=verification_hashes,
                sha256_hash=file_info.sha256_hash,
            )

        # Add CAS blocks for new xorbs
        for xorb in self.xorbs:
            # Determine dedup eligibility for each chunk
            dedup_eligible = []
            for i, chunk_hash in enumerate(xorb.chunk_hashes):
                is_first = i == 0  # First chunk of xorb is eligible
                eligible = is_global_dedup_eligible(chunk_hash, is_first)
                dedup_eligible.append(eligible)

            shard_builder.add_cas_block(
                cas_hash=xorb.xorb_hash,
                chunk_hashes=xorb.chunk_hashes,
                chunk_sizes=xorb.chunk_sizes,
                serialized_size=len(xorb.serialized_data),
                dedup_eligible=dedup_eligible,
            )

        shard = shard_builder.build()

        # Step 6: Upload shard
        shard_data = serialize_shard_for_upload(shard)
        self._report_progress("shard_upload", 1, 1)
        self.client.upload_shard(shard_data)

        return [f.file_hash for f in self.files]


class DownloadSession:
    """Manages downloading files."""

    def __init__(
        self,
        client: CASClient,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ):
        """Initialize download session.

        Args:
            client: CAS API client.
            progress_callback: Optional callback(stage, current, total).
        """
        self.client = client
        self.progress_callback = progress_callback

    def _report_progress(self, stage: str, current: int, total: int):
        """Report progress if callback is set."""
        if self.progress_callback:
            self.progress_callback(stage, current, total)

    def download(
        self, file_hash: bytes, byte_range: Optional[tuple[int, int]] = None
    ) -> bytes:
        """Download a file.

        Args:
            file_hash: 32-byte file hash.
            byte_range: Optional (start, end) byte range (end inclusive).

        Returns:
            File contents (or requested range).
        """
        # Step 1: Query reconstruction
        self._report_progress("query", 0, 1)
        recon = self.client.get_reconstruction(file_hash, byte_range)
        self._report_progress("query", 1, 1)

        # Step 2: Download xorb data
        # Group terms by xorb to avoid redundant downloads
        xorb_data: dict[bytes, bytes] = {}
        xorb_ranges: dict[bytes, set[tuple[int, int]]] = {}

        for term in recon.terms:
            if term.xorb_hash not in xorb_ranges:
                xorb_ranges[term.xorb_hash] = set()
            xorb_ranges[term.xorb_hash].add(
                (term.chunk_range.start, term.chunk_range.end)
            )

        total_xorbs = len(xorb_ranges)
        for i, xorb_hash in enumerate(xorb_ranges.keys()):
            self._report_progress("download", i + 1, total_xorbs)

            # Find fetch info covering our needed ranges
            fetch_infos = recon.fetch_info.get(xorb_hash, [])

            for fi in fetch_infos:
                # Download the URL range
                data = self.client.download_xorb_range(fi.url, fi.url_range)
                if xorb_hash not in xorb_data:
                    xorb_data[xorb_hash] = data
                else:
                    # Append if multiple ranges (would need proper handling)
                    xorb_data[xorb_hash] = data

        # Step 3: Extract chunks and assemble file
        result = bytearray()

        for term_idx, term in enumerate(recon.terms):
            data = xorb_data[term.xorb_hash]
            chunks = extract_chunk_range(
                data, term.chunk_range.start, term.chunk_range.end
            )

            chunk_data = b"".join(chunks)

            # Apply offset for first term
            if term_idx == 0 and recon.offset_into_first_range > 0:
                chunk_data = chunk_data[recon.offset_into_first_range :]

            result.extend(chunk_data)

        # Truncate for range queries
        if byte_range:
            requested_length = byte_range[1] - byte_range[0] + 1
            result = result[:requested_length]

        return bytes(result)


def upload_file(
    client: CASClient, data: bytes, enable_global_dedup: bool = True
) -> bytes:
    """Convenience function to upload a single file.

    Args:
        client: CAS API client.
        data: File contents.
        enable_global_dedup: Whether to query global dedup.

    Returns:
        32-byte file hash.
    """
    session = UploadSession(client, enable_global_dedup=enable_global_dedup)
    session.add_file(data)
    hashes = session.upload()
    return hashes[0]


def download_file(
    client: CASClient, file_hash: bytes, byte_range: Optional[tuple[int, int]] = None
) -> bytes:
    """Convenience function to download a file.

    Args:
        client: CAS API client.
        file_hash: 32-byte file hash.
        byte_range: Optional (start, end) byte range.

    Returns:
        File contents.
    """
    session = DownloadSession(client)
    return session.download(file_hash, byte_range)
