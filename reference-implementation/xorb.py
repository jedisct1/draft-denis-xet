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


MAIN_FOOTER_IDENT = b"XETBLOB"
HASH_FOOTER_IDENT = b"XBLBHSH"
BOUNDARY_FOOTER_IDENT = b"XBLBBND"
FOOTER_BUFFER_SIZE = 16
UNIQUENESS_NONCE_SIZE = 4
FOOTER_TRAILER_SIZE = 12 + FOOTER_BUFFER_SIZE
INFO_LENGTH_SIZE = 4


@dataclass
class _FooterInfo:
    """Parsed CasObjectInfo footer metadata."""

    xorb_hash: bytes
    num_chunks: int
    chunk_hashes: list[bytes]
    chunk_boundary_offsets: list[int]
    unpacked_chunk_offsets: list[int]
    footer_buffer: bytes


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
    footer_buffer: bytes = bytes(FOOTER_BUFFER_SIZE)

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


def _build_footer(
    xorb: Xorb,
    chunk_boundary_offsets: list[int],
    unpacked_chunk_offsets: list[int],
    uniqueness_nonce: bytes,
) -> bytes:
    """Build the CasObjectInfo footer."""
    if len(uniqueness_nonce) != UNIQUENESS_NONCE_SIZE:
        raise ValueError("Uniqueness nonce must be exactly 4 bytes")

    num_chunks = len(xorb.chunks)
    main_header = MAIN_FOOTER_IDENT + bytes([1]) + xorb.xorb_hash

    hash_section_start = len(main_header)
    hash_section = bytearray()
    hash_section.extend(HASH_FOOTER_IDENT)
    hash_section.append(0)
    hash_section.extend(struct.pack("<I", num_chunks))
    for chunk in xorb.chunks:
        hash_section.extend(chunk.chunk_hash)

    boundary_section_start = hash_section_start + len(hash_section)
    boundary_section = bytearray()
    boundary_section.extend(BOUNDARY_FOOTER_IDENT)
    boundary_section.append(1)
    boundary_section.extend(struct.pack("<I", num_chunks))
    for offset in chunk_boundary_offsets:
        boundary_section.extend(struct.pack("<I", offset))
    for offset in unpacked_chunk_offsets:
        boundary_section.extend(struct.pack("<I", offset))

    body = main_header + bytes(hash_section) + bytes(boundary_section)
    footer_length = len(body) + FOOTER_TRAILER_SIZE
    hashes_offset_from_end = footer_length - hash_section_start
    boundaries_offset_from_end = footer_length - boundary_section_start
    footer_buffer = uniqueness_nonce + bytes(FOOTER_BUFFER_SIZE - UNIQUENESS_NONCE_SIZE)
    trailer = struct.pack(
        "<III", num_chunks, hashes_offset_from_end, boundaries_offset_from_end
    ) + footer_buffer

    return body + trailer


def _parse_footer(footer: bytes) -> _FooterInfo:
    """Parse the CasObjectInfo footer."""
    if len(footer) < len(MAIN_FOOTER_IDENT) + 1 + 32 + FOOTER_TRAILER_SIZE:
        raise ValueError("CasObjectInfo footer is too short")
    if footer[:7] != MAIN_FOOTER_IDENT:
        raise ValueError("Missing CasObjectInfo footer ident")
    if footer[7] != 1:
        raise ValueError(f"Unknown CasObjectInfo version {footer[7]}")

    xorb_hash = footer[8:40]
    trailer = footer[-FOOTER_TRAILER_SIZE:]
    num_chunks, hashes_offset, boundaries_offset = struct.unpack("<III", trailer[:12])
    footer_buffer = trailer[12:]
    if footer_buffer[UNIQUENESS_NONCE_SIZE:] != bytes(
        FOOTER_BUFFER_SIZE - UNIQUENESS_NONCE_SIZE
    ):
        raise ValueError("Reserved footer buffer bytes must be zero")

    hash_section_start = len(footer) - hashes_offset
    boundary_section_start = len(footer) - boundaries_offset
    if not (40 <= hash_section_start < boundary_section_start < len(footer)):
        raise ValueError("Invalid CasObjectInfo section offsets")

    if footer[hash_section_start : hash_section_start + 7] != HASH_FOOTER_IDENT:
        raise ValueError("Missing hash section ident")
    hashes_version = footer[hash_section_start + 7]
    if hashes_version != 0:
        raise ValueError(f"Unknown hashes section version {hashes_version}")
    hash_count = struct.unpack(
        "<I", footer[hash_section_start + 8 : hash_section_start + 12]
    )[0]
    if hash_count != num_chunks:
        raise ValueError("Hash section chunk count mismatch")
    hashes_start = hash_section_start + 12
    hashes_end = hashes_start + 32 * num_chunks
    if hashes_end > len(footer):
        raise ValueError("Hash section is truncated")
    chunk_hashes = [
        footer[hashes_start + 32 * i : hashes_start + 32 * (i + 1)]
        for i in range(num_chunks)
    ]

    if footer[boundary_section_start : boundary_section_start + 7] != BOUNDARY_FOOTER_IDENT:
        raise ValueError("Missing boundary section ident")
    boundaries_version = footer[boundary_section_start + 7]
    if boundaries_version != 1:
        raise ValueError(f"Unknown boundary section version {boundaries_version}")
    boundary_count = struct.unpack(
        "<I", footer[boundary_section_start + 8 : boundary_section_start + 12]
    )[0]
    if boundary_count != num_chunks:
        raise ValueError("Boundary section chunk count mismatch")
    offsets_start = boundary_section_start + 12
    offsets_end = offsets_start + 8 * num_chunks
    if offsets_end > len(footer) - FOOTER_TRAILER_SIZE:
        raise ValueError("Boundary section is truncated")
    chunk_boundary_offsets = [
        struct.unpack("<I", footer[offsets_start + 4 * i : offsets_start + 4 * (i + 1)])[0]
        for i in range(num_chunks)
    ]
    unpacked_start = offsets_start + 4 * num_chunks
    unpacked_chunk_offsets = [
        struct.unpack("<I", footer[unpacked_start + 4 * i : unpacked_start + 4 * (i + 1)])[0]
        for i in range(num_chunks)
    ]

    return _FooterInfo(
        xorb_hash=xorb_hash,
        num_chunks=num_chunks,
        chunk_hashes=chunk_hashes,
        chunk_boundary_offsets=chunk_boundary_offsets,
        unpacked_chunk_offsets=unpacked_chunk_offsets,
        footer_buffer=footer_buffer,
    )


def _split_xorb_regions(data: bytes) -> tuple[bytes, Optional[_FooterInfo]]:
    """Split serialized bytes into chunk data and optional footer metadata."""
    if len(data) < INFO_LENGTH_SIZE:
        return data, None

    info_length = struct.unpack("<I", data[-INFO_LENGTH_SIZE:])[0]
    if info_length == 0 or info_length > len(data) - INFO_LENGTH_SIZE:
        return data, None

    footer_start = len(data) - INFO_LENGTH_SIZE - info_length
    footer = data[footer_start : len(data) - INFO_LENGTH_SIZE]
    if not footer.startswith(MAIN_FOOTER_IDENT):
        return data, None

    return data[:footer_start], _parse_footer(footer)


def serialize_xorb(
    xorb: Xorb,
    compression_type: int = COMPRESSION_LZ4,
    uniqueness_nonce: bytes = bytes(UNIQUENESS_NONCE_SIZE),
) -> bytes:
    """Serialize a xorb to binary format.

    Xorb format:
    - Sequence of chunk entries, each with:
      - 8-byte header
      - Variable-length compressed data
    - CasObjectInfo footer
    - 4-byte footer length

    Chunk header (8 bytes):
    - Byte 0: Version (must be 0)
    - Bytes 1-3: Compressed size (little-endian)
    - Byte 4: Compression type
    - Bytes 5-7: Uncompressed size (little-endian)

    Args:
        xorb: The xorb to serialize.
        compression_type: Default compression type to use.
        uniqueness_nonce: Optional 4-byte value for the footer buffer.

    Returns:
        Serialized xorb bytes.

    Raises:
        ValueError: If xorb exceeds raw size/count limits.
    """
    if len(xorb.chunks) > MAX_XORB_CHUNKS:
        raise ValueError(
            f"Xorb has {len(xorb.chunks)} chunks, max is {MAX_XORB_CHUNKS}"
        )

    raw_payload_size = sum(len(chunk.data) for chunk in xorb.chunks)
    if raw_payload_size > MAX_XORB_SIZE:
        raise ValueError(
            f"Xorb raw payload is {raw_payload_size} bytes, max is {MAX_XORB_SIZE}"
        )

    result = bytearray()
    chunk_boundary_offsets = []
    unpacked_chunk_offsets = []
    unpacked_offset = 0

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
        chunk_boundary_offsets.append(len(result))
        unpacked_offset += uncompressed_size
        unpacked_chunk_offsets.append(unpacked_offset)

    footer = _build_footer(
        xorb, chunk_boundary_offsets, unpacked_chunk_offsets, uniqueness_nonce
    )
    result.extend(footer)
    result.extend(struct.pack("<I", len(footer)))

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
    chunk_data_region, footer_info = _split_xorb_regions(data)
    chunks = []
    offset = 0

    while offset < len(chunk_data_region):
        if offset + 8 > len(chunk_data_region):
            raise ValueError(f"Truncated header at offset {offset}")

        header = chunk_data_region[offset : offset + 8]
        version = header[0]
        if version != 0:
            raise ValueError(f"Unknown chunk version {version} at offset {offset}")

        compressed_size = _decode_u24_le(header[1:4])
        compression_type = header[4]
        uncompressed_size = _decode_u24_le(header[5:8])

        offset += 8

        if offset + compressed_size > len(chunk_data_region):
            raise ValueError(f"Truncated chunk data at offset {offset}")

        compressed_data = chunk_data_region[offset : offset + compressed_size]
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
    if footer_info is not None:
        if footer_info.num_chunks != len(chunks):
            raise ValueError("Footer chunk count does not match chunk data region")
        if footer_info.chunk_hashes != [chunk.chunk_hash for chunk in chunks]:
            raise ValueError("Footer chunk hashes do not match chunk data")
        if footer_info.chunk_boundary_offsets and footer_info.chunk_boundary_offsets[-1] != len(
            chunk_data_region
        ):
            raise ValueError("Footer chunk boundary offsets do not match chunk data")
        if footer_info.xorb_hash != xorb.xorb_hash:
            raise ValueError("Footer xorb hash does not match chunk data")
        xorb.footer_buffer = footer_info.footer_buffer
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
    chunk_data_region, _ = _split_xorb_regions(data)
    chunks = []
    offset = 0
    chunk_index = 0

    while offset < len(chunk_data_region) and chunk_index < end_index:
        if offset + 8 > len(chunk_data_region):
            raise ValueError(f"Truncated header at offset {offset}")

        header = chunk_data_region[offset : offset + 8]
        version = header[0]
        if version != 0:
            raise ValueError(f"Unknown chunk version {version}")

        compressed_size = _decode_u24_le(header[1:4])
        compression_type = header[4]
        uncompressed_size = _decode_u24_le(header[5:8])

        offset += 8
        if offset + compressed_size > len(chunk_data_region):
            raise ValueError(f"Truncated chunk data at offset {offset}")
        compressed_data = chunk_data_region[offset : offset + compressed_size]
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
        """Check if a chunk can be added without exceeding raw limits."""
        if len(self.chunks) >= self.max_chunks:
            return False

        return self.current_size + len(chunk_data) <= self.max_size

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
        self.current_size += len(chunk_data)
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
