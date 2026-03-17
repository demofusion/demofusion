# demofusion-py

Python bindings for demofusion - SQL queries over Valve Source 2 demo files via Apache DataFusion.

## Installation

```bash
pip install demofusion
```

## Quick Start

See the main [README.md](../README.md) in the parent directory for full documentation.

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Build bindings
maturin develop
```

## Wheels and ABI compatibility

Python wheels are built with PyO3 `abi3` targeting Python 3.11+
(`abi3-py311`). This means one wheel per platform/architecture can be used
across CPython 3.11, 3.12, and 3.13.

Notes:

- `pip install git+https://...` still builds from source locally.
- Prebuilt wheels are only used when installed from an index/release that hosts
  those wheel files (for example, PyPI or GitHub Release assets).
