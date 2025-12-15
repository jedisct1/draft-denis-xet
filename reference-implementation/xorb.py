"""XET Xorb Format

Implements xorb serialization and deserialization for XET.
"""

import struct
from dataclasses import dataclass
from typing import Optional

import lz4.frame

from constants import (
    MAX_XORB_SIZE,
    MAX_XORB_CHUNKS,
    COMPRESSION_NONE,
    COMPRESSION_LZ4,
    COMPRESSION_BYTE_GROUPING_4_LZ4,
)
from hashing import compute_chunk_hash, compute_xorb_hash


@dataclass
class ChunkEntry:
    """A chunk entry within a xorb."""

    data: bytes  # Uncompressed chunk data
    chunk_hash: bytes  # 32-byte hash
    compressed_data: Optional[bytes] = None  # Compressed data (if any)
    compression_type: int = COMPRESSION_NONE


@dataclass
class Xorb:
    """A xorb container."""

    chunks: list[ChunkEntry]
    xorb_hash: bytes = b""

    def __post_init__(self):
        if not self.xorb_hash:
            self.compute_hash()

    def compute_hash(self):
        """Compute and set the xorb hash."""
        chunk_hashes = [c.chunk_hash for c in self.chunks]
        chunk_sizes = [len(c.data) for c in self.chunks]
        self.xorb_hash = compute_xorb_hash(chunk_hashes, chunk_sizes)


def byte_group_4(data: bytes) -> bytes:
    """Apply 4-byte grouping transformation.

    Reorganizes bytes by position within 4-byte groups for better compression.
    Original:  [A0 A1 A2 A3 | B0 B1 B2 B3 | ...]
    Grouped:   [A0 B0 C0 ... | A1 B1 C1 ... | A2 B2 C2 ... | A3 B3 C3 ...]

    Args:
        data: Input bytes.

    Returns:
        Grouped bytes.
    """
    n = len(data)
    groups = [bytearray() for _ in range(4)]

    for i in range(n):
        groups[i % 4].append(data[i])

    return bytes(groups[0] + groups[1] + groups[2] + groups[3])


def byte_ungroup_4(grouped_data: bytes, original_length: int) -> bytes:
    """Reverse 4-byte grouping transformation.

    Args:
        grouped_data: Grouped bytes.
        original_length: Original data length.

    Returns:
        Original byte order.
    """
    n = original_length
    base_size = n // 4
    remainder = n % 4

    # Calculate group sizes
    sizes = [base_size + (1 if i < remainder else 0) for i in range(4)]

    # Extract groups
    groups = []
    offset = 0
    for size in sizes:
        groups.append(grouped_data[offset : offset + size])
        offset += size

    # Interleave back to original order
    data = bytearray()
    for i in range(n):
        group_idx = i % 4
        pos_in_group = i // 4
        data.append(groups[group_idx][pos_in_group])

    return bytes(data)


def compress_chunk(
    data: bytes, compression_type: int = COMPRESSION_LZ4
) -> tuple[bytes, int]:
    """Compress chunk data.

    Args:
        data: Uncompressed chunk data.
        compression_type: Desired compression type.

    Returns:
        Tuple of (compressed_data, actual_compression_type).
        If compression increases size, returns (data, COMPRESSION_NONE).
    """
    if compression_type == COMPRESSION_NONE:
        return data, COMPRESSION_NONE

    if compression_type == COMPRESSION_LZ4:
        compressed = lz4.frame.compress(data)
        if len(compressed) >= len(data):
            return data, COMPRESSION_NONE
        return compressed, COMPRESSION_LZ4

    if compression_type == COMPRESSION_BYTE_GROUPING_4_LZ4:
        grouped = byte_group_4(data)
        compressed = lz4.frame.compress(grouped)
        if len(compressed) >= len(data):
            return data, COMPRESSION_NONE
        return compressed, COMPRESSION_BYTE_GROUPING_4_LZ4

    raise ValueError(f"Unknown compression type: {compression_type}")


def decompress_chunk(
    compressed_data: bytes, compression_type: int, uncompressed_size: int
) -> bytes:
    """Decompress chunk data.

    Args:
        compressed_data: Compressed chunk data.
        compression_type: Compression type from header.
        uncompressed_size: Expected uncompressed size.

    Returns:
        Decompressed chunk data.
    """
    if compression_type == COMPRESSION_NONE:
        return compressed_data

    if compression_type == COMPRESSION_LZ4:
        return lz4.frame.decompress(compressed_data)

    if compression_type == COMPRESSION_BYTE_GROUPING_4_LZ4:
        grouped = lz4.frame.decompress(compressed_data)
        return byte_ungroup_4(grouped, uncompressed_size)

    raise ValueError(f"Unknown compression type: {compression_type}")


def _encode_u24_le(value: int) -> bytes:
    """Encode a value as 3-byte little-endian."""
    return struct.pack("<I", value)[:3]


def _decode_u24_le(data: bytes) -> int:
    """Decode 3-byte little-endian to integer."""
    return struct.unpack("<I", data + b"\x00")[0]


def serialize_xorb(xorb: Xorb, compression_type: int = COMPRESSION_LZ4) -> bytes:
    """Serialize a xorb to binary format.

    Xorb format:
    - Sequence of chunk entries, each with:
      - 8-byte header
      - Variable-length compressed data

    Chunk header (8 bytes):
    - Byte 0: Version (must be 0)
    - Bytes 1-3: Compressed size (little-endian)
    - Byte 4: Compression type
    - Bytes 5-7: Uncompressed size (little-endian)

    Args:
        xorb: The xorb to serialize.
        compression_type: Default compression type to use.

    Returns:
        Serialized xorb bytes.

    Raises:
        ValueError: If xorb exceeds size/count limits.
    """
    if len(xorb.chunks) > MAX_XORB_CHUNKS:
        raise ValueError(
            f"Xorb has {len(xorb.chunks)} chunks, max is {MAX_XORB_CHUNKS}"
        )

    result = bytearray()

    for chunk in xorb.chunks:
        # Compress if not already compressed
        if chunk.compressed_data is None:
            compressed, actual_type = compress_chunk(chunk.data, compression_type)
            chunk.compressed_data = compressed
            chunk.compression_type = actual_type
        else:
            compressed = chunk.compressed_data
            actual_type = chunk.compression_type

        compressed_size = len(compressed)
        uncompressed_size = len(chunk.data)

        # Build header
        header = bytearray(8)
        header[0] = 0  # Version
        header[1:4] = _encode_u24_le(compressed_size)
        header[4] = actual_type
        header[5:8] = _encode_u24_le(uncompressed_size)

        result.extend(header)
        result.extend(compressed)

    if len(result) > MAX_XORB_SIZE:
        raise ValueError(
            f"Serialized xorb is {len(result)} bytes, max is {MAX_XORB_SIZE}"
        )

    return bytes(result)


def deserialize_xorb(data: bytes) -> Xorb:
    """Deserialize binary xorb data.

    Args:
        data: Serialized xorb bytes.

    Returns:
        Xorb object with decompressed chunks.

    Raises:
        ValueError: If format is invalid.
    """
    chunks = []
    offset = 0

    while offset < len(data):
        if offset + 8 > len(data):
            raise ValueError(f"Truncated header at offset {offset}")

        header = data[offset : offset + 8]
        version = header[0]
        if version != 0:
            raise ValueError(f"Unknown chunk version {version} at offset {offset}")

        compressed_size = _decode_u24_le(header[1:4])
        compression_type = header[4]
        uncompressed_size = _decode_u24_le(header[5:8])

        offset += 8

        if offset + compressed_size > len(data):
            raise ValueError(f"Truncated chunk data at offset {offset}")

        compressed_data = data[offset : offset + compressed_size]
        offset += compressed_size

        # Decompress
        chunk_data = decompress_chunk(
            compressed_data, compression_type, uncompressed_size
        )
        chunk_hash = compute_chunk_hash(chunk_data)

        chunks.append(
            ChunkEntry(
                data=chunk_data,
                chunk_hash=chunk_hash,
                compressed_data=compressed_data,
                compression_type=compression_type,
            )
        )

    xorb = Xorb(chunks=chunks)
    return xorb


def extract_chunk_range(data: bytes, start_index: int, end_index: int) -> list[bytes]:
    """Extract a range of chunks from serialized xorb data.

    Args:
        data: Serialized xorb bytes.
        start_index: Start chunk index (inclusive).
        end_index: End chunk index (exclusive).

    Returns:
        List of decompressed chunk data.
    """
    chunks = []
    offset = 0
    chunk_index = 0

    while offset < len(data) and chunk_index < end_index:
        if offset + 8 > len(data):
            raise ValueError(f"Truncated header at offset {offset}")

        header = data[offset : offset + 8]
        version = header[0]
        if version != 0:
            raise ValueError(f"Unknown chunk version {version}")

        compressed_size = _decode_u24_le(header[1:4])
        compression_type = header[4]
        uncompressed_size = _decode_u24_le(header[5:8])

        offset += 8
        compressed_data = data[offset : offset + compressed_size]
        offset += compressed_size

        if chunk_index >= start_index:
            chunk_data = decompress_chunk(
                compressed_data, compression_type, uncompressed_size
            )
            chunks.append(chunk_data)

        chunk_index += 1

    return chunks


class XorbBuilder:
    """Builder for creating xorbs from chunks."""

    def __init__(
        self,
        max_size: int = MAX_XORB_SIZE,
        max_chunks: int = MAX_XORB_CHUNKS,
        compression_type: int = COMPRESSION_LZ4,
    ):
        self.max_size = max_size
        self.max_chunks = max_chunks
        self.compression_type = compression_type
        self.chunks: list[ChunkEntry] = []
        self.current_size = 0

    def can_add(self, chunk_data: bytes) -> bool:
        """Check if a chunk can be added without exceeding limits."""
        if len(self.chunks) >= self.max_chunks:
            return False

        # Estimate compressed size (conservative: assume no compression benefit)
        estimated_entry_size = 8 + len(chunk_data)
        return self.current_size + estimated_entry_size <= self.max_size

    def add(self, chunk_data: bytes, chunk_hash: Optional[bytes] = None) -> bool:
        """Add a chunk to the xorb.

        Args:
            chunk_data: Raw chunk bytes.
            chunk_hash: Pre-computed hash (optional, will compute if not provided).

        Returns:
            True if chunk was added, False if it would exceed limits.
        """
        if not self.can_add(chunk_data):
            return False

        if chunk_hash is None:
            chunk_hash = compute_chunk_hash(chunk_data)

        compressed, actual_type = compress_chunk(chunk_data, self.compression_type)

        entry = ChunkEntry(
            data=chunk_data,
            chunk_hash=chunk_hash,
            compressed_data=compressed,
            compression_type=actual_type,
        )

        self.chunks.append(entry)
        self.current_size += 8 + len(compressed)
        return True

    def build(self) -> Xorb:
        """Build the xorb from added chunks."""
        return Xorb(chunks=self.chunks)

    def is_empty(self) -> bool:
        """Check if no chunks have been added."""
        return len(self.chunks) == 0

    def reset(self):
        """Reset the builder for a new xorb."""
        self.chunks = []
        self.current_size = 0
