"""XET CAS API Client

Implements the XET CAS HTTP API client.
"""

from dataclasses import dataclass
from typing import Optional

import requests

from hashing import hash_to_string, string_to_hash
from shard import deserialize_shard, Shard


@dataclass
class ChunkRange:
    """A chunk index range."""

    start: int
    end: int  # Exclusive


@dataclass
class ByteRange:
    """A byte range."""

    start: int
    end: int  # Inclusive (for HTTP Range header)


@dataclass
class FetchInfo:
    """Information for fetching xorb data."""

    chunk_range: ChunkRange
    url: str
    url_range: ByteRange


@dataclass
class ReconstructionTerm:
    """A term in file reconstruction."""

    xorb_hash: bytes
    unpacked_length: int
    chunk_range: ChunkRange


@dataclass
class ReconstructionResponse:
    """Response from reconstruction API."""

    offset_into_first_range: int
    terms: list[ReconstructionTerm]
    fetch_info: dict[bytes, list[FetchInfo]]  # xorb_hash -> fetch info list


class CASClient:
    """Client for the XET CAS API."""

    def __init__(self, base_url: str, token: str, timeout: float = 30.0):
        """Initialize CAS client.

        Args:
            base_url: Base URL for the CAS API (e.g., "https://cas.example.com").
            token: Bearer token for authentication.
            timeout: Request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
            }
        )

    def get_reconstruction(
        self, file_hash: bytes, byte_range: Optional[tuple[int, int]] = None
    ) -> ReconstructionResponse:
        """Get file reconstruction information.

        GET /v1/reconstructions/{file_id}

        Args:
            file_hash: 32-byte file hash.
            byte_range: Optional (start, end) byte range (end inclusive).

        Returns:
            ReconstructionResponse with terms and fetch info.

        Raises:
            requests.HTTPError: On API errors.
        """
        file_id = hash_to_string(file_hash)
        url = f"{self.base_url}/v1/reconstructions/{file_id}"

        headers = {}
        if byte_range:
            headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"

        response = self.session.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()

        # Parse terms
        terms = []
        for t in data["terms"]:
            terms.append(
                ReconstructionTerm(
                    xorb_hash=string_to_hash(t["hash"]),
                    unpacked_length=t["unpacked_length"],
                    chunk_range=ChunkRange(
                        start=t["range"]["start"], end=t["range"]["end"]
                    ),
                )
            )

        # Parse fetch info
        fetch_info = {}
        for hash_str, infos in data["fetch_info"].items():
            xorb_hash = string_to_hash(hash_str)
            fetch_info[xorb_hash] = [
                FetchInfo(
                    chunk_range=ChunkRange(
                        start=fi["range"]["start"], end=fi["range"]["end"]
                    ),
                    url=fi["url"],
                    url_range=ByteRange(
                        start=fi["url_range"]["start"], end=fi["url_range"]["end"]
                    ),
                )
                for fi in infos
            ]

        return ReconstructionResponse(
            offset_into_first_range=data["offset_into_first_range"],
            terms=terms,
            fetch_info=fetch_info,
        )

    def query_chunk_dedup(self, chunk_hash: bytes) -> Optional[Shard]:
        """Query global deduplication for a chunk.

        GET /v1/chunks/default-merkledb/{chunk_hash}

        Args:
            chunk_hash: 32-byte chunk hash.

        Returns:
            Shard if chunk exists, None if not found.

        Raises:
            requests.HTTPError: On API errors other than 404.
        """
        chunk_id = hash_to_string(chunk_hash)
        url = f"{self.base_url}/v1/chunks/default-merkledb/{chunk_id}"

        response = self.session.get(url, timeout=self.timeout)

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return deserialize_shard(response.content)

    def upload_xorb(self, xorb_hash: bytes, xorb_data: bytes) -> bool:
        """Upload a xorb.

        POST /v1/xorbs/default/{xorb_hash}

        Args:
            xorb_hash: 32-byte xorb hash.
            xorb_data: Serialized xorb bytes.

        Returns:
            True if xorb was inserted, False if it already existed.

        Raises:
            requests.HTTPError: On API errors.
        """
        xorb_id = hash_to_string(xorb_hash)
        url = f"{self.base_url}/v1/xorbs/default/{xorb_id}"

        response = self.session.post(
            url,
            data=xorb_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=self.timeout,
        )
        response.raise_for_status()

        data = response.json()
        return data.get("was_inserted", True)

    def upload_shard(self, shard_data: bytes) -> int:
        """Upload a shard.

        POST /v1/shards

        Args:
            shard_data: Serialized shard bytes (without footer).

        Returns:
            Result code: 0 = shard already exists, 1 = shard was registered.

        Raises:
            requests.HTTPError: On API errors.
        """
        url = f"{self.base_url}/v1/shards"

        response = self.session.post(
            url,
            data=shard_data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=self.timeout,
        )
        response.raise_for_status()

        data = response.json()
        return data.get("result", 1)

    def download_xorb_range(
        self, url: str, byte_range: Optional[ByteRange] = None
    ) -> bytes:
        """Download xorb data from a pre-signed URL.

        Args:
            url: Pre-signed URL from fetch_info.
            byte_range: Optional byte range to download.

        Returns:
            Xorb data bytes.

        Raises:
            requests.HTTPError: On download errors.
        """
        headers = {}
        if byte_range:
            headers["Range"] = f"bytes={byte_range.start}-{byte_range.end}"

        # Use a fresh session for pre-signed URLs (no auth needed)
        response = requests.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.content


@dataclass
class DeduplicationResult:
    """Result from deduplication query."""

    chunk_hash: bytes
    found: bool
    xorb_hash: Optional[bytes] = None
    chunk_index: Optional[int] = None


class DeduplicationCache:
    """Cache for deduplication results."""

    def __init__(self):
        # chunk_hash -> (xorb_hash, chunk_index)
        self._cache: dict[bytes, tuple[bytes, int]] = {}

    def add(self, chunk_hash: bytes, xorb_hash: bytes, chunk_index: int):
        """Add a deduplication result to the cache."""
        self._cache[chunk_hash] = (xorb_hash, chunk_index)

    def get(self, chunk_hash: bytes) -> Optional[tuple[bytes, int]]:
        """Get cached deduplication result."""
        return self._cache.get(chunk_hash)

    def add_from_shard(self, shard: Shard, chunk_key: Optional[bytes] = None):
        """Populate cache from a shard's CAS info.

        Args:
            shard: Shard containing CAS blocks.
            chunk_key: If provided, chunk hashes in shard are keyed.
        """

        for cas_block in shard.cas_blocks:
            for i, entry in enumerate(cas_block.entries):
                if chunk_key and chunk_key != bytes(32):
                    # Can't directly cache keyed hashes - they need to be
                    # matched by computing keyed hash of local chunk hashes
                    pass
                else:
                    self._cache[entry.chunk_hash] = (cas_block.cas_hash, i)
