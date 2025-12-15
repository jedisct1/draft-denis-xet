"""Test XET implementation against specification test vectors."""

import sys

sys.path.insert(0, ".")

from hashing import (
    compute_chunk_hash,
    hash_to_string,
    string_to_hash,
    blake3_keyed_hash,
    compute_verification_hash,
    _merge_hash_sequence,
)
from constants import INTERNAL_NODE_KEY


def test_chunk_hash():
    """Test chunk hash computation."""
    print("Testing chunk hash...")

    input_data = b"Hello World!"
    expected_raw_hex = (
        "a29cfb08e608d4d8726dd8659a90b9134b3240d5d8e42d5fcb28e2a6e763a3e8"
    )
    expected_xet_string = (
        "d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb"
    )

    chunk_hash = compute_chunk_hash(input_data)

    # Check raw hex
    raw_hex = chunk_hash.hex()
    print(f"  Input: {input_data}")
    print(f"  Raw hex:     {raw_hex}")
    print(f"  Expected:    {expected_raw_hex}")
    assert raw_hex == expected_raw_hex, "Raw hex mismatch!"

    # Check XET string format
    xet_string = hash_to_string(chunk_hash)
    print(f"  XET string:  {xet_string}")
    print(f"  Expected:    {expected_xet_string}")
    assert xet_string == expected_xet_string, "XET string mismatch!"

    # Verify round-trip
    recovered = string_to_hash(xet_string)
    assert recovered == chunk_hash, "Round-trip conversion failed!"

    print("  PASSED\n")


def test_hash_string_conversion():
    """Test hash string conversion."""
    print("Testing hash string conversion...")

    # Test vector: bytes 0x00 through 0x1f
    input_bytes = bytes(range(32))
    expected_string = "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"

    result = hash_to_string(input_bytes)
    print(f"  Input bytes: {input_bytes.hex()}")
    print(f"  Result:      {result}")
    print(f"  Expected:    {expected_string}")
    assert result == expected_string, "Hash string conversion mismatch!"

    # Verify reverse conversion
    recovered = string_to_hash(expected_string)
    assert recovered == input_bytes, "Reverse conversion failed!"

    print("  PASSED\n")


def test_internal_node_hash():
    """Test internal node hash computation."""
    print("Testing internal node hash...")

    # Child 1
    child1_xet = "c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69"
    child1_size = 100

    # Child 2
    child2_xet = "6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22"
    child2_size = 200

    # Expected result
    expected_xet = "be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14"

    # Build the buffer exactly as specified
    # Format: {hash_hex} : {size}\n
    buffer = f"{child1_xet} : {child1_size}\n{child2_xet} : {child2_size}\n"
    print("  Buffer:")
    for line in buffer.split("\n"):
        if line:
            print(f"    {line}")

    result = blake3_keyed_hash(INTERNAL_NODE_KEY, buffer.encode("utf-8"))
    result_xet = hash_to_string(result)

    print(f"  Result:   {result_xet}")
    print(f"  Expected: {expected_xet}")
    assert result_xet == expected_xet, "Internal node hash mismatch!"

    # Also test the actual _merge_hash_sequence function with raw bytes
    child1_bytes = string_to_hash(child1_xet)
    child2_bytes = string_to_hash(child2_xet)
    entries = [(child1_bytes, child1_size), (child2_bytes, child2_size)]
    merged_hash, merged_size = _merge_hash_sequence(entries)
    merged_xet = hash_to_string(merged_hash)

    print(f"  _merge_hash_sequence result: {merged_xet}")
    assert merged_xet == expected_xet, "_merge_hash_sequence mismatch!"
    assert merged_size == child1_size + child2_size, "Size mismatch!"

    print("  PASSED\n")


def test_verification_range_hash():
    """Test verification range hash computation."""
    print("Testing verification range hash...")

    # Test vector from spec: two chunk hashes (raw hex)
    chunk1_raw_hex = "aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad"
    chunk2_raw_hex = "2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2"

    chunk1 = bytes.fromhex(chunk1_raw_hex)
    chunk2 = bytes.fromhex(chunk2_raw_hex)

    expected_xet = "eb06a8ad81d588ac05d1d9a079232d9c1e7d0b07232fa58091caa7bf333a2768"

    # Compute verification hash
    result = compute_verification_hash([chunk1, chunk2], 0, 2)
    result_xet = hash_to_string(result)

    print(f"  Chunk 1 (raw hex): {chunk1_raw_hex}")
    print(f"  Chunk 2 (raw hex): {chunk2_raw_hex}")
    print(f"  Result (XET): {result_xet}")
    print(f"  Expected:     {expected_xet}")
    assert result_xet == expected_xet, "Verification hash mismatch!"

    print("  PASSED\n")


def test_chunking():
    """Test content-defined chunking."""
    print("Testing chunking...")

    from chunking import chunk_data
    from constants import MIN_CHUNK_SIZE, MAX_CHUNK_SIZE

    # Test 1: Small file (single chunk)
    small_data = b"Hello World!"
    chunks = chunk_data(small_data)
    print(f"  Small file ({len(small_data)} bytes): {len(chunks)} chunk(s)")
    assert len(chunks) == 1, "Small file should produce single chunk"
    assert chunks[0].data == small_data, "Chunk data should match input"

    # Test 2: Empty file (no chunks)
    empty_chunks = chunk_data(b"")
    print(f"  Empty file: {len(empty_chunks)} chunk(s)")
    assert len(empty_chunks) == 0, "Empty file should produce no chunks"

    # Test 3: Large file (multiple chunks)
    import os

    large_data = os.urandom(500_000)  # 500 KB
    large_chunks = chunk_data(large_data)
    print(f"  Large file ({len(large_data)} bytes): {len(large_chunks)} chunk(s)")

    # Verify chunk sizes are within bounds (except possibly the last)
    for i, chunk in enumerate(large_chunks):
        is_last = i == len(large_chunks) - 1
        if not is_last:
            assert chunk.size >= MIN_CHUNK_SIZE, f"Chunk {i} too small: {chunk.size}"
        assert chunk.size <= MAX_CHUNK_SIZE, f"Chunk {i} too large: {chunk.size}"

    # Verify reconstruction
    reconstructed = b"".join(c.data for c in large_chunks)
    assert reconstructed == large_data, "Reconstructed data should match original"

    # Test 4: Determinism
    large_chunks2 = chunk_data(large_data)
    assert len(large_chunks) == len(large_chunks2), "Chunking should be deterministic"
    for c1, c2 in zip(large_chunks, large_chunks2):
        assert c1.data == c2.data, "Chunk data should be identical"

    print("  PASSED\n")


def test_xorb_serialization():
    """Test xorb serialization and deserialization."""
    print("Testing xorb serialization...")

    from xorb import Xorb, ChunkEntry, serialize_xorb, deserialize_xorb
    from hashing import compute_chunk_hash

    # Create test chunks
    chunk1_data = b"This is chunk 1 data"
    chunk2_data = b"This is chunk 2 data with more content"

    chunk1_hash = compute_chunk_hash(chunk1_data)
    chunk2_hash = compute_chunk_hash(chunk2_data)

    xorb = Xorb(
        chunks=[
            ChunkEntry(data=chunk1_data, chunk_hash=chunk1_hash),
            ChunkEntry(data=chunk2_data, chunk_hash=chunk2_hash),
        ]
    )

    # Serialize
    serialized = serialize_xorb(xorb)
    print(f"  Created xorb with {len(xorb.chunks)} chunks")
    print(f"  Serialized size: {len(serialized)} bytes")

    # Deserialize
    recovered = deserialize_xorb(serialized)
    print(f"  Recovered {len(recovered.chunks)} chunks")

    # Verify
    assert len(recovered.chunks) == 2, "Should recover 2 chunks"
    assert recovered.chunks[0].data == chunk1_data, "Chunk 1 data mismatch"
    assert recovered.chunks[1].data == chunk2_data, "Chunk 2 data mismatch"
    assert recovered.chunks[0].chunk_hash == chunk1_hash, "Chunk 1 hash mismatch"
    assert recovered.chunks[1].chunk_hash == chunk2_hash, "Chunk 2 hash mismatch"

    print("  PASSED\n")


def test_shard_serialization():
    """Test shard serialization and deserialization."""
    print("Testing shard serialization...")

    from shard import (
        Shard,
        FileBlock,
        FileDataSequenceEntry,
        FileVerificationEntry,
        FileMetadataExt,
        CASBlock,
        CASChunkSequenceEntry,
        serialize_shard_for_upload,
        deserialize_shard,
    )

    # Create test data
    file_hash = bytes(range(32))
    cas_hash = bytes(range(32, 64))
    chunk_hash = bytes(range(64, 96))
    verification_hash = bytes(range(96, 128))
    sha256_hash = bytes(range(128, 160))

    shard = Shard(
        file_blocks=[
            FileBlock(
                file_hash=file_hash,
                entries=[
                    FileDataSequenceEntry(
                        cas_hash=cas_hash,
                        cas_flags=0,
                        unpacked_segment_bytes=1000,
                        chunk_index_start=0,
                        chunk_index_end=5,
                    )
                ],
                verification_entries=[
                    FileVerificationEntry(range_hash=verification_hash)
                ],
                metadata_ext=FileMetadataExt(sha256_hash=sha256_hash),
            )
        ],
        cas_blocks=[
            CASBlock(
                cas_hash=cas_hash,
                cas_flags=0,
                entries=[
                    CASChunkSequenceEntry(
                        chunk_hash=chunk_hash,
                        chunk_byte_range_start=0,
                        unpacked_segment_bytes=200,
                        flags=0,
                    )
                ],
                num_bytes_in_cas=200,
                num_bytes_on_disk=150,
            )
        ],
    )

    # Serialize
    serialized = serialize_shard_for_upload(shard)
    print(f"  Serialized shard: {len(serialized)} bytes")

    # Deserialize
    recovered = deserialize_shard(serialized)
    print(f"  Recovered {len(recovered.file_blocks)} file block(s)")
    print(f"  Recovered {len(recovered.cas_blocks)} CAS block(s)")

    # Verify file block
    assert len(recovered.file_blocks) == 1
    fb = recovered.file_blocks[0]
    assert fb.file_hash == file_hash
    assert len(fb.entries) == 1
    assert fb.entries[0].cas_hash == cas_hash
    assert fb.entries[0].unpacked_segment_bytes == 1000
    assert len(fb.verification_entries) == 1
    assert fb.verification_entries[0].range_hash == verification_hash
    assert fb.metadata_ext is not None
    assert fb.metadata_ext.sha256_hash == sha256_hash

    # Verify CAS block
    assert len(recovered.cas_blocks) == 1
    cb = recovered.cas_blocks[0]
    assert cb.cas_hash == cas_hash
    assert len(cb.entries) == 1
    assert cb.entries[0].chunk_hash == chunk_hash
    assert cb.entries[0].unpacked_segment_bytes == 200

    print("  PASSED\n")


def test_byte_grouping():
    """Test byte grouping transformation."""
    print("Testing byte grouping...")

    from xorb import byte_group_4, byte_ungroup_4

    # Test with known pattern
    # Original:  [A0 A1 A2 A3 | B0 B1 B2 B3 | C0 C1 C2 C3]
    # Grouped:   [A0 B0 C0 | A1 B1 C1 | A2 B2 C2 | A3 B3 C3]
    original = bytes(
        [
            0x10,
            0x11,
            0x12,
            0x13,  # A
            0x20,
            0x21,
            0x22,
            0x23,  # B
            0x30,
            0x31,
            0x32,
            0x33,  # C
        ]
    )
    expected_grouped = bytes(
        [
            0x10,
            0x20,
            0x30,  # group 0 (A0, B0, C0)
            0x11,
            0x21,
            0x31,  # group 1 (A1, B1, C1)
            0x12,
            0x22,
            0x32,  # group 2 (A2, B2, C2)
            0x13,
            0x23,
            0x33,  # group 3 (A3, B3, C3)
        ]
    )

    grouped = byte_group_4(original)
    print(f"  Original: {original.hex()}")
    print(f"  Grouped:  {grouped.hex()}")
    print(f"  Expected: {expected_grouped.hex()}")
    assert grouped == expected_grouped, "Byte grouping mismatch!"

    # Test round-trip
    ungrouped = byte_ungroup_4(grouped, len(original))
    assert ungrouped == original, "Round-trip failed!"

    # Test with non-multiple of 4
    odd_data = bytes(range(10))
    odd_grouped = byte_group_4(odd_data)
    odd_recovered = byte_ungroup_4(odd_grouped, len(odd_data))
    assert odd_recovered == odd_data, "Odd-length round-trip failed!"

    print("  PASSED\n")


def test_merkle_tree():
    """Test Merkle tree construction."""
    print("Testing Merkle tree...")

    from hashing import compute_merkle_root, compute_chunk_hash

    # Test with single entry
    chunk1 = b"test chunk 1"
    hash1 = compute_chunk_hash(chunk1)
    root1 = compute_merkle_root([(hash1, len(chunk1))])
    print(f"  Single entry root: {hash_to_string(root1)}")
    assert len(root1) == 32

    # Test with two entries
    chunk2 = b"test chunk 2 with more data"
    hash2 = compute_chunk_hash(chunk2)
    root2 = compute_merkle_root([(hash1, len(chunk1)), (hash2, len(chunk2))])
    print(f"  Two entries root:  {hash_to_string(root2)}")
    assert len(root2) == 32
    assert root2 != root1  # Different tree should give different root

    # Test empty
    root_empty = compute_merkle_root([])
    assert root_empty == bytes(32), "Empty tree should return zero hash"

    print("  PASSED\n")


def test_file_hash():
    """Test file hash computation."""
    print("Testing file hash...")

    from chunking import chunk_data
    from hashing import compute_chunk_hash, compute_file_hash

    # Create test file
    test_data = b"Hello, XET!" * 1000

    # Chunk it
    chunks = chunk_data(test_data)
    print(f"  Test data: {len(test_data)} bytes, {len(chunks)} chunk(s)")

    # Compute file hash
    chunk_hashes = [compute_chunk_hash(c.data) for c in chunks]
    chunk_sizes = [c.size for c in chunks]
    file_hash = compute_file_hash(chunk_hashes, chunk_sizes)

    print(f"  File hash: {hash_to_string(file_hash)}")
    assert len(file_hash) == 32

    # Verify determinism
    file_hash2 = compute_file_hash(chunk_hashes, chunk_sizes)
    assert file_hash == file_hash2, "File hash should be deterministic"

    print("  PASSED\n")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("XET Implementation Tests")
    print("=" * 60 + "\n")

    test_chunk_hash()
    test_hash_string_conversion()
    test_internal_node_hash()
    test_verification_range_hash()
    test_chunking()
    test_xorb_serialization()
    test_shard_serialization()
    test_byte_grouping()
    test_merkle_tree()
    test_file_hash()

    print("=" * 60)
    print("All tests PASSED!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()
