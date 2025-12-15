"""XET Hashing Methods

Implements BLAKE3 keyed hashing and Merkle tree computations for XET.
"""

import struct

import blake3

from constants import (
    DATA_KEY,
    INTERNAL_NODE_KEY,
    ZERO_KEY,
    VERIFICATION_KEY,
    MEAN_BRANCHING_FACTOR,
    MIN_CHILDREN,
    MAX_CHILDREN,
)


def blake3_keyed_hash(key: bytes, data: bytes) -> bytes:
    """Compute BLAKE3 keyed hash.

    Args:
        key: 32-byte key.
        data: Data to hash.

    Returns:
        32-byte hash.
    """
    hasher = blake3.blake3(key=key)
    hasher.update(data)
    return hasher.digest()


def compute_chunk_hash(chunk_data: bytes) -> bytes:
    """Compute hash for a chunk.

    Uses BLAKE3 keyed hash with DATA_KEY.

    Args:
        chunk_data: Raw chunk bytes.

    Returns:
        32-byte chunk hash.
    """
    return blake3_keyed_hash(DATA_KEY, chunk_data)


def hash_to_string(hash_bytes: bytes) -> str:
    """Convert 32-byte hash to XET string representation.

    The XET format interprets the hash as four little-endian u64 values
    and prints each as 16 hexadecimal digits.

    Args:
        hash_bytes: 32-byte hash.

    Returns:
        64-character lowercase hex string.
    """
    result = ""
    for i in range(4):
        start = i * 8
        end = start + 8
        segment = hash_bytes[start:end]
        # Interpret as little-endian u64
        u64_val = struct.unpack("<Q", segment)[0]
        result += f"{u64_val:016x}"
    return result


def string_to_hash(hex_string: str) -> bytes:
    """Convert XET string representation to 32-byte hash.

    Args:
        hex_string: 64-character hex string in XET format.

    Returns:
        32-byte hash.
    """
    result = bytearray()
    for i in range(4):
        start = i * 16
        end = start + 16
        u64_val = int(hex_string[start:end], 16)
        result.extend(struct.pack("<Q", u64_val))
    return bytes(result)


def _next_merge_cut(entries: list[tuple[bytes, int]]) -> int:
    """Determine the next cut point for Merkle tree construction.

    A cut point occurs when:
    1. Minimum children (2) accumulated AND hash % MEAN_BRANCHING_FACTOR == 0
    2. Maximum children (9) reached
    3. End of list reached

    Args:
        entries: List of (hash, size) pairs.

    Returns:
        Number of entries to include in this merge group.
    """
    if len(entries) <= 2:
        return len(entries)

    end = min(MAX_CHILDREN, len(entries))

    for i in range(MIN_CHILDREN - 1, end):
        h = entries[i][0]
        # Interpret last 8 bytes of hash as little-endian u64
        hash_value = struct.unpack("<Q", h[24:32])[0]
        if hash_value % MEAN_BRANCHING_FACTOR == 0:
            return i + 1

    return end


def _merge_hash_sequence(entries: list[tuple[bytes, int]]) -> tuple[bytes, int]:
    """Merge a sequence of (hash, size) pairs into a single entry.

    Args:
        entries: List of (hash, size) pairs.

    Returns:
        Tuple of (merged_hash, total_size).
    """
    buffer = ""
    total_size = 0

    for h, size in entries:
        buffer += f"{hash_to_string(h)} : {size}\n"
        total_size += size

    new_hash = blake3_keyed_hash(INTERNAL_NODE_KEY, buffer.encode("utf-8"))
    return (new_hash, total_size)


def compute_merkle_root(entries: list[tuple[bytes, int]]) -> bytes:
    """Compute Merkle tree root from (hash, size) pairs.

    Uses aggregated hash tree construction with variable fan-out.

    Args:
        entries: List of (hash, size) pairs.

    Returns:
        32-byte root hash, or 32 zero bytes if empty.
    """
    if len(entries) == 0:
        return bytes(32)

    # Copy entries to avoid modifying original
    hv = list(entries)

    while len(hv) > 1:
        new_hv = []
        read_idx = 0

        while read_idx < len(hv):
            # Find the next cut point
            remaining = hv[read_idx:]
            cut_len = _next_merge_cut(remaining)

            # Merge this slice into one parent node
            merged = _merge_hash_sequence(hv[read_idx : read_idx + cut_len])
            new_hv.append(merged)

            read_idx += cut_len

        hv = new_hv

    return hv[0][0]


def compute_xorb_hash(chunk_hashes: list[bytes], chunk_sizes: list[int]) -> bytes:
    """Compute xorb hash from its chunks.

    The xorb hash is the Merkle tree root built from chunk hashes.

    Args:
        chunk_hashes: List of 32-byte chunk hashes.
        chunk_sizes: List of chunk sizes in bytes.

    Returns:
        32-byte xorb hash.
    """
    entries = list(zip(chunk_hashes, chunk_sizes))
    return compute_merkle_root(entries)


def compute_file_hash(chunk_hashes: list[bytes], chunk_sizes: list[int]) -> bytes:
    """Compute file hash from its chunks.

    The file hash is the Merkle root with an additional keyed hash using ZERO_KEY.

    Args:
        chunk_hashes: List of 32-byte chunk hashes.
        chunk_sizes: List of chunk sizes in bytes.

    Returns:
        32-byte file hash.
    """
    entries = list(zip(chunk_hashes, chunk_sizes))
    merkle_root = compute_merkle_root(entries)
    return blake3_keyed_hash(ZERO_KEY, merkle_root)


def compute_verification_hash(
    chunk_hashes: list[bytes], start_index: int, end_index: int
) -> bytes:
    """Compute verification hash for a chunk range.

    Used in shards to prove possession of actual file data.

    Args:
        chunk_hashes: List of all chunk hashes.
        start_index: Start index (inclusive).
        end_index: End index (exclusive).

    Returns:
        32-byte verification hash.
    """
    buffer = b""
    for i in range(start_index, end_index):
        buffer += chunk_hashes[i]  # 32 bytes each
    return blake3_keyed_hash(VERIFICATION_KEY, buffer)


def is_global_dedup_eligible(chunk_hash: bytes, is_first_chunk: bool) -> bool:
    """Check if a chunk is eligible for global deduplication queries.

    A chunk is eligible if:
    1. It is the first chunk of a file, OR
    2. The last 8 bytes of its hash, as little-endian u64, mod 1024 == 0

    Args:
        chunk_hash: 32-byte chunk hash.
        is_first_chunk: Whether this is the first chunk of a file.

    Returns:
        True if eligible for global dedup.
    """
    if is_first_chunk:
        return True
    hash_value = struct.unpack("<Q", chunk_hash[24:32])[0]
    return hash_value % 1024 == 0


def compute_keyed_chunk_hash(chunk_hash: bytes, key: bytes) -> bytes:
    """Compute keyed hash of chunk hash for deduplication matching.

    Args:
        chunk_hash: 32-byte chunk hash.
        key: 32-byte key from shard footer.

    Returns:
        32-byte keyed hash result.
    """
    return blake3_keyed_hash(key, chunk_hash)
