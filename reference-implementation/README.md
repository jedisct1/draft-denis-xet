# XET Reference Implementation

This is a simple Python reference implementation of the XET protocol, designed to closely match the specification and serve as an illustration of how the protocol works.

This code prioritizes clarity and correctness over performance. It is intended for:

- Understanding the specification
- Validating test vectors
- Prototyping and experimentation

## For Production Use

For production deployments, use the official implementation from Hugging Face:

- [xet-core](https://github.com/huggingface/xet-core) - The official Rust implementation

## Running Tests

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python test_spec.py
```
