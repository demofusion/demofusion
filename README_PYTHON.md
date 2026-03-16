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
