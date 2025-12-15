"""XET Shard Format

Implements shard serialization and deserialization for XET.
"""

import struct
from dataclasses import dataclass, field
from typing import Optional

from constants import (
    SHARD_HEADER_TAG,
    FILE_FLAG_WITH_VERIFICATION,
    FILE_FLAG_WITH_METADATA_EXT,
    CHUNK_FLAG_GLOBAL_DEDUP_ELIGIBLE,
)


@dataclass
class FileDataSequenceEntry:
    """A term in a file reconstruction."""

    cas_hash: bytes  # 32-byte xorb hash
    cas_flags: int  # Reserved, set to 0
    unpacked_segment_bytes: int
    chunk_index_start: int
    chunk_index_end: int  # Exclusive


@dataclass
class FileVerificationEntry:
    """Verification hash for a term."""

    range_hash: bytes  # 32-byte verification hash


@dataclass
class FileMetadataExt:
    """Extended file metadata."""

    sha256_hash: bytes  # 32-byte SHA-256 of file contents


@dataclass
class FileBlock:
    """A file reconstruction block in a shard."""

    file_hash: bytes  # 32-byte file hash
    entries: list[FileDataSequenceEntry]
    verification_entries: list[FileVerificationEntry] = field(default_factory=list)
    metadata_ext: Optional[FileMetadataExt] = None


@dataclass
class CASChunkSequenceEntry:
    """Information about a chunk within a xorb."""

    chunk_hash: bytes  # 32-byte chunk hash
    chunk_byte_range_start: int
    unpacked_segment_bytes: int
    flags: int  # Bit 31: GLOBAL_DEDUP_ELIGIBLE


@dataclass
class CASBlock:
    """A xorb information block in a shard."""

    cas_hash: bytes  # 32-byte xorb hash
    cas_flags: int  # Reserved, set to 0
    entries: list[CASChunkSequenceEntry]
    num_bytes_in_cas: int  # Total uncompressed bytes
    num_bytes_on_disk: int  # Serialized xorb size


@dataclass
class ShardFooter:
    """Shard footer for stored shards."""

    version: int = 1
    file_info_offset: int = 0
    cas_info_offset: int = 0
    file_lookup_offset: int = 0
    file_lookup_num_entries: int = 0
    cas_lookup_offset: int = 0
    cas_lookup_num_entries: int = 0
    chunk_lookup_offset: int = 0
    chunk_lookup_num_entries: int = 0
    chunk_hash_key: bytes = field(default_factory=lambda: bytes(32))
    shard_creation_timestamp: int = 0
    shard_key_expiry: int = 0
    stored_bytes_on_disk: int = 0
    materialized_bytes: int = 0
    stored_bytes: int = 0
    footer_offset: int = 0


@dataclass
class Shard:
    """A complete shard structure."""

    file_blocks: list[FileBlock] = field(default_factory=list)
    cas_blocks: list[CASBlock] = field(default_factory=list)
    footer: Optional[ShardFooter] = None


# Constants for bookend entries
BOOKEND_HASH = bytes([0xFF] * 32)
BOOKEND_PADDING = bytes(16)


def _write_u32(value: int) -> bytes:
    """Write u32 as little-endian bytes."""
    return struct.pack("<I", value)


def _write_u64(value: int) -> bytes:
    """Write u64 as little-endian bytes."""
    return struct.pack("<Q", value)


def _read_u32(data: bytes, offset: int) -> int:
    """Read u32 from little-endian bytes."""
    return struct.unpack("<I", data[offset : offset + 4])[0]


def _read_u64(data: bytes, offset: int) -> int:
    """Read u64 from little-endian bytes."""
    return struct.unpack("<Q", data[offset : offset + 8])[0]


def serialize_shard_header(version: int = 2, footer_size: int = 0) -> bytes:
    """Serialize shard header (48 bytes).

    Header format:
    - Bytes 0-31: Magic tag
    - Bytes 32-39: Version (u64)
    - Bytes 40-47: Footer size (u64)
    """
    result = bytearray(48)
    result[0:32] = SHARD_HEADER_TAG
    result[32:40] = _write_u64(version)
    result[40:48] = _write_u64(footer_size)
    return bytes(result)


def serialize_file_data_sequence_header(
    file_hash: bytes, num_entries: int, with_verification: bool, with_metadata_ext: bool
) -> bytes:
    """Serialize FileDataSequenceHeader (48 bytes).

    Format:
    - Bytes 0-31: File hash
    - Bytes 32-35: Flags (u32)
    - Bytes 36-39: Number of entries (u32)
    - Bytes 40-47: Reserved (zeros)
    """
    flags = 0
    if with_verification:
        flags |= FILE_FLAG_WITH_VERIFICATION
    if with_metadata_ext:
        flags |= FILE_FLAG_WITH_METADATA_EXT

    result = bytearray(48)
    result[0:32] = file_hash
    result[32:36] = _write_u32(flags)
    result[36:40] = _write_u32(num_entries)
    # Bytes 40-47 are zeros (reserved)
    return bytes(result)


def serialize_file_data_sequence_entry(entry: FileDataSequenceEntry) -> bytes:
    """Serialize FileDataSequenceEntry (48 bytes).

    Format:
    - Bytes 0-31: CAS hash (xorb hash)
    - Bytes 32-35: CAS flags (u32, reserved)
    - Bytes 36-39: Unpacked segment bytes (u32)
    - Bytes 40-43: Chunk index start (u32)
    - Bytes 44-47: Chunk index end (u32, exclusive)
    """
    result = bytearray(48)
    result[0:32] = entry.cas_hash
    result[32:36] = _write_u32(entry.cas_flags)
    result[36:40] = _write_u32(entry.unpacked_segment_bytes)
    result[40:44] = _write_u32(entry.chunk_index_start)
    result[44:48] = _write_u32(entry.chunk_index_end)
    return bytes(result)


def serialize_file_verification_entry(entry: FileVerificationEntry) -> bytes:
    """Serialize FileVerificationEntry (48 bytes).

    Format:
    - Bytes 0-31: Range hash (verification hash)
    - Bytes 32-47: Reserved (zeros)
    """
    result = bytearray(48)
    result[0:32] = entry.range_hash
    # Bytes 32-47 are zeros (reserved)
    return bytes(result)


def serialize_file_metadata_ext(metadata: FileMetadataExt) -> bytes:
    """Serialize FileMetadataExt (48 bytes).

    Format:
    - Bytes 0-31: SHA-256 hash of file contents
    - Bytes 32-47: Reserved (zeros)
    """
    result = bytearray(48)
    result[0:32] = metadata.sha256_hash
    # Bytes 32-47 are zeros (reserved)
    return bytes(result)


def serialize_bookend() -> bytes:
    """Serialize bookend entry (48 bytes).

    Format:
    - Bytes 0-31: All 0xFF
    - Bytes 32-47: All 0x00
    """
    result = bytearray(48)
    result[0:32] = BOOKEND_HASH
    # Bytes 32-47 are already zeros
    return bytes(result)


def serialize_cas_chunk_sequence_header(
    cas_hash: bytes, num_entries: int, num_bytes_in_cas: int, num_bytes_on_disk: int
) -> bytes:
    """Serialize CASChunkSequenceHeader (48 bytes).

    Format:
    - Bytes 0-31: CAS hash (xorb hash)
    - Bytes 32-35: CAS flags (u32, reserved, set to 0)
    - Bytes 36-39: Number of entries (u32)
    - Bytes 40-43: Num bytes in CAS (u32, total uncompressed)
    - Bytes 44-47: Num bytes on disk (u32, serialized xorb size)
    """
    result = bytearray(48)
    result[0:32] = cas_hash
    result[32:36] = _write_u32(0)  # Reserved flags
    result[36:40] = _write_u32(num_entries)
    result[40:44] = _write_u32(num_bytes_in_cas)
    result[44:48] = _write_u32(num_bytes_on_disk)
    return bytes(result)


def serialize_cas_chunk_sequence_entry(entry: CASChunkSequenceEntry) -> bytes:
    """Serialize CASChunkSequenceEntry (48 bytes).

    Format:
    - Bytes 0-31: Chunk hash
    - Bytes 32-35: Chunk byte range start (u32)
    - Bytes 36-39: Unpacked segment bytes (u32)
    - Bytes 40-43: Flags (u32)
    - Bytes 44-47: Reserved (u32, zeros)
    """
    result = bytearray(48)
    result[0:32] = entry.chunk_hash
    result[32:36] = _write_u32(entry.chunk_byte_range_start)
    result[36:40] = _write_u32(entry.unpacked_segment_bytes)
    result[40:44] = _write_u32(entry.flags)
    # Bytes 44-47 are zeros (reserved)
    return bytes(result)


def serialize_shard_for_upload(shard: Shard) -> bytes:
    """Serialize shard for upload (without footer).

    Structure:
    - Header (48 bytes)
    - File info section (variable, ends with bookend)
    - CAS info section (variable, ends with bookend)

    Args:
        shard: The shard to serialize.

    Returns:
        Serialized shard bytes (without footer).
    """
    result = bytearray()

    # Header with footer_size = 0 (no footer for uploads)
    result.extend(serialize_shard_header(version=2, footer_size=0))

    # File info section
    for fb in shard.file_blocks:
        with_verification = len(fb.verification_entries) > 0
        with_metadata_ext = fb.metadata_ext is not None

        result.extend(
            serialize_file_data_sequence_header(
                fb.file_hash, len(fb.entries), with_verification, with_metadata_ext
            )
        )

        for entry in fb.entries:
            result.extend(serialize_file_data_sequence_entry(entry))

        if with_verification:
            for ve in fb.verification_entries:
                result.extend(serialize_file_verification_entry(ve))

        if with_metadata_ext and fb.metadata_ext:
            result.extend(serialize_file_metadata_ext(fb.metadata_ext))

    result.extend(serialize_bookend())

    # CAS info section
    for cb in shard.cas_blocks:
        num_bytes_in_cas = sum(e.unpacked_segment_bytes for e in cb.entries)
        result.extend(
            serialize_cas_chunk_sequence_header(
                cb.cas_hash, len(cb.entries), num_bytes_in_cas, cb.num_bytes_on_disk
            )
        )

        for entry in cb.entries:
            result.extend(serialize_cas_chunk_sequence_entry(entry))

    result.extend(serialize_bookend())

    return bytes(result)


def deserialize_shard(data: bytes) -> Shard:
    """Deserialize binary shard data.

    Args:
        data: Serialized shard bytes.

    Returns:
        Shard object.

    Raises:
        ValueError: If format is invalid.
    """
    if len(data) < 48:
        raise ValueError("Shard data too short for header")

    # Verify header tag
    if data[0:32] != SHARD_HEADER_TAG:
        raise ValueError("Invalid shard magic tag")

    version = _read_u64(data, 32)
    if version != 2:
        raise ValueError(f"Unsupported shard version: {version}")

    footer_size = _read_u64(data, 40)

    offset = 48
    shard = Shard()

    # Parse file info section
    while offset < len(data):
        if offset + 48 > len(data):
            raise ValueError(f"Truncated file block at offset {offset}")

        # Check for bookend
        if data[offset : offset + 32] == BOOKEND_HASH:
            offset += 48
            break

        # Parse FileDataSequenceHeader
        file_hash = data[offset : offset + 32]
        flags = _read_u32(data, offset + 32)
        num_entries = _read_u32(data, offset + 36)
        offset += 48

        with_verification = bool(flags & FILE_FLAG_WITH_VERIFICATION)
        with_metadata_ext = bool(flags & FILE_FLAG_WITH_METADATA_EXT)

        entries = []
        for _ in range(num_entries):
            if offset + 48 > len(data):
                raise ValueError(f"Truncated file entry at offset {offset}")

            entry = FileDataSequenceEntry(
                cas_hash=data[offset : offset + 32],
                cas_flags=_read_u32(data, offset + 32),
                unpacked_segment_bytes=_read_u32(data, offset + 36),
                chunk_index_start=_read_u32(data, offset + 40),
                chunk_index_end=_read_u32(data, offset + 44),
            )
            entries.append(entry)
            offset += 48

        verification_entries = []
        if with_verification:
            for _ in range(num_entries):
                if offset + 48 > len(data):
                    raise ValueError(f"Truncated verification entry at offset {offset}")
                ve = FileVerificationEntry(range_hash=data[offset : offset + 32])
                verification_entries.append(ve)
                offset += 48

        metadata_ext = None
        if with_metadata_ext:
            if offset + 48 > len(data):
                raise ValueError(f"Truncated metadata ext at offset {offset}")
            metadata_ext = FileMetadataExt(sha256_hash=data[offset : offset + 32])
            offset += 48

        shard.file_blocks.append(
            FileBlock(
                file_hash=file_hash,
                entries=entries,
                verification_entries=verification_entries,
                metadata_ext=metadata_ext,
            )
        )

    # Parse CAS info section
    while offset < len(data):
        if footer_size > 0 and offset >= len(data) - footer_size:
            # We've reached the footer
            break

        if offset + 48 > len(data):
            raise ValueError(f"Truncated CAS block at offset {offset}")

        # Check for bookend
        if data[offset : offset + 32] == BOOKEND_HASH:
            offset += 48
            break

        # Parse CASChunkSequenceHeader
        cas_hash = data[offset : offset + 32]
        cas_flags = _read_u32(data, offset + 32)
        num_entries = _read_u32(data, offset + 36)
        num_bytes_in_cas = _read_u32(data, offset + 40)
        num_bytes_on_disk = _read_u32(data, offset + 44)
        offset += 48

        entries = []
        for _ in range(num_entries):
            if offset + 48 > len(data):
                raise ValueError(f"Truncated CAS entry at offset {offset}")

            entry = CASChunkSequenceEntry(
                chunk_hash=data[offset : offset + 32],
                chunk_byte_range_start=_read_u32(data, offset + 32),
                unpacked_segment_bytes=_read_u32(data, offset + 36),
                flags=_read_u32(data, offset + 40),
            )
            entries.append(entry)
            offset += 48

        shard.cas_blocks.append(
            CASBlock(
                cas_hash=cas_hash,
                cas_flags=cas_flags,
                entries=entries,
                num_bytes_in_cas=num_bytes_in_cas,
                num_bytes_on_disk=num_bytes_on_disk,
            )
        )

    # Parse footer if present
    if footer_size > 0:
        footer_offset = len(data) - footer_size
        if footer_size >= 200:
            shard.footer = ShardFooter(
                version=_read_u64(data, footer_offset),
                file_info_offset=_read_u64(data, footer_offset + 8),
                cas_info_offset=_read_u64(data, footer_offset + 16),
                file_lookup_offset=_read_u64(data, footer_offset + 24),
                file_lookup_num_entries=_read_u64(data, footer_offset + 32),
                cas_lookup_offset=_read_u64(data, footer_offset + 40),
                cas_lookup_num_entries=_read_u64(data, footer_offset + 48),
                chunk_lookup_offset=_read_u64(data, footer_offset + 56),
                chunk_lookup_num_entries=_read_u64(data, footer_offset + 64),
                chunk_hash_key=data[footer_offset + 72 : footer_offset + 104],
                shard_creation_timestamp=_read_u64(data, footer_offset + 104),
                shard_key_expiry=_read_u64(data, footer_offset + 112),
                stored_bytes_on_disk=_read_u64(data, footer_offset + 168),
                materialized_bytes=_read_u64(data, footer_offset + 176),
                stored_bytes=_read_u64(data, footer_offset + 184),
                footer_offset=_read_u64(data, footer_offset + 192),
            )

    return shard


class ShardBuilder:
    """Builder for creating shards from file and xorb information."""

    def __init__(self):
        self.file_blocks: list[FileBlock] = []
        self.cas_blocks: list[CASBlock] = []
        self._cas_hashes: set[bytes] = set()

    def add_file(
        self,
        file_hash: bytes,
        terms: list[FileDataSequenceEntry],
        verification_hashes: list[bytes],
        sha256_hash: Optional[bytes] = None,
    ):
        """Add a file reconstruction to the shard.

        Args:
            file_hash: 32-byte file hash.
            terms: List of reconstruction terms.
            verification_hashes: List of verification hashes for each term.
            sha256_hash: Optional SHA-256 hash of file contents.
        """
        verification_entries = [
            FileVerificationEntry(range_hash=h) for h in verification_hashes
        ]
        metadata_ext = FileMetadataExt(sha256_hash=sha256_hash) if sha256_hash else None

        self.file_blocks.append(
            FileBlock(
                file_hash=file_hash,
                entries=terms,
                verification_entries=verification_entries,
                metadata_ext=metadata_ext,
            )
        )

    def add_cas_block(
        self,
        cas_hash: bytes,
        chunk_hashes: list[bytes],
        chunk_sizes: list[int],
        serialized_size: int,
        dedup_eligible: Optional[list[bool]] = None,
    ):
        """Add a xorb (CAS block) to the shard.

        Args:
            cas_hash: 32-byte xorb hash.
            chunk_hashes: List of chunk hashes.
            chunk_sizes: List of chunk sizes (uncompressed).
            serialized_size: Size of serialized xorb.
            dedup_eligible: Optional list of dedup eligibility per chunk.
        """
        if cas_hash in self._cas_hashes:
            return  # Already added

        self._cas_hashes.add(cas_hash)

        entries = []
        byte_offset = 0

        for i, (chunk_hash, chunk_size) in enumerate(zip(chunk_hashes, chunk_sizes)):
            flags = 0
            if dedup_eligible and dedup_eligible[i]:
                flags |= CHUNK_FLAG_GLOBAL_DEDUP_ELIGIBLE

            entries.append(
                CASChunkSequenceEntry(
                    chunk_hash=chunk_hash,
                    chunk_byte_range_start=byte_offset,
                    unpacked_segment_bytes=chunk_size,
                    flags=flags,
                )
            )
            byte_offset += chunk_size

        self.cas_blocks.append(
            CASBlock(
                cas_hash=cas_hash,
                cas_flags=0,
                entries=entries,
                num_bytes_in_cas=byte_offset,
                num_bytes_on_disk=serialized_size,
            )
        )

    def build(self) -> Shard:
        """Build the shard."""
        return Shard(file_blocks=self.file_blocks, cas_blocks=self.cas_blocks)
