"""Content-Defined Chunking using Gearhash

Implements the XET content-defined chunking algorithm.
"""

from dataclasses import dataclass
from typing import Iterator

from constants import (
    GEARHASH_TABLE,
    MIN_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    MASK,
)


@dataclass
class Chunk:
    """A chunk of data with its offset and size."""

    data: bytes
    offset: int
    size: int


def chunk_data(data: bytes) -> list[Chunk]:
    """Split data into chunks using content-defined chunking.

    Uses Gearhash algorithm with the following parameters:
    - MIN_CHUNK_SIZE: 8 KiB (minimum chunk size)
    - MAX_CHUNK_SIZE: 128 KiB (maximum chunk size)
    - MASK: 0xFFFF000000000000 (16 one-bits for boundary detection)

    Args:
        data: The input data to chunk.

    Returns:
        List of Chunk objects.
    """
    if len(data) == 0:
        return []

    chunks = []
    h = 0  # 64-bit rolling hash
    start_offset = 0

    for i in range(len(data)):
        b = data[i]
        h = ((h << 1) + GEARHASH_TABLE[b]) & 0xFFFFFFFFFFFFFFFF  # 64-bit wrap

        chunk_size = i - start_offset + 1

        # Skip boundary checks until minimum size reached
        if chunk_size < MIN_CHUNK_SIZE:
            continue

        # Force boundary at maximum size
        if chunk_size >= MAX_CHUNK_SIZE:
            chunks.append(
                Chunk(
                    data=data[start_offset : i + 1],
                    offset=start_offset,
                    size=chunk_size,
                )
            )
            start_offset = i + 1
            h = 0
            continue

        # Check for natural boundary
        if (h & MASK) == 0:
            chunks.append(
                Chunk(
                    data=data[start_offset : i + 1],
                    offset=start_offset,
                    size=chunk_size,
                )
            )
            start_offset = i + 1
            h = 0

    # Emit final chunk if any data remains
    if start_offset < len(data):
        chunks.append(
            Chunk(
                data=data[start_offset:],
                offset=start_offset,
                size=len(data) - start_offset,
            )
        )

    return chunks


def chunk_file(filepath: str) -> list[Chunk]:
    """Chunk a file using content-defined chunking.

    Args:
        filepath: Path to the file to chunk.

    Returns:
        List of Chunk objects.
    """
    with open(filepath, "rb") as f:
        data = f.read()
    return chunk_data(data)


def chunk_stream(stream, buffer_size: int = 1024 * 1024) -> Iterator[Chunk]:
    """Chunk a stream using content-defined chunking.

    This is a streaming implementation that doesn't require loading
    the entire file into memory.

    Args:
        stream: A file-like object supporting read().
        buffer_size: Size of internal read buffer.

    Yields:
        Chunk objects.
    """
    h = 0  # 64-bit rolling hash
    current_chunk = bytearray()
    global_offset = 0
    chunk_start_offset = 0

    while True:
        buffer = stream.read(buffer_size)
        if not buffer:
            break

        for b in buffer:
            h = ((h << 1) + GEARHASH_TABLE[b]) & 0xFFFFFFFFFFFFFFFF
            current_chunk.append(b)
            chunk_size = len(current_chunk)

            emit_chunk = False

            if chunk_size < MIN_CHUNK_SIZE:
                pass
            elif chunk_size >= MAX_CHUNK_SIZE:
                emit_chunk = True
            elif (h & MASK) == 0:
                emit_chunk = True

            if emit_chunk:
                yield Chunk(
                    data=bytes(current_chunk),
                    offset=chunk_start_offset,
                    size=chunk_size,
                )
                chunk_start_offset = global_offset + 1
                current_chunk = bytearray()
                h = 0

            global_offset += 1

    # Emit final chunk if any data remains
    if current_chunk:
        yield Chunk(
            data=bytes(current_chunk),
            offset=chunk_start_offset,
            size=len(current_chunk),
        )
