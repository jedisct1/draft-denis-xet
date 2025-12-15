# XET Python Implementation

This is the reference implementation for the [XET Internet-Draft specification](../draft-denis-xet.md). It is a simple Python implementation designed to closely match the specification text and serve as an illustration of how the protocol works.

This code prioritizes clarity and correctness over performance. It is intended for:

- Understanding the specification
- Validating test vectors
- Prototyping and experimentation

## For Production Use

For production deployments, use the official implementations from Hugging Face:

- [xet-core](https://github.com/huggingface/xet-core) - The official Rust implementation
- [huggingface_hub](https://github.com/huggingface/huggingface_hub) - Python library with XET support

## Running Tests

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python test_spec.py
```
