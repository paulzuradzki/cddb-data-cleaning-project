#!/bin/bash

# Usage: bash ./run_tests.sh

# Testing
echo "Testing..."
python -m pytest

# Linting
echo "Linting..."
ruff check .

# Type-checking
echo "Type-checking..."
MYPYPATH=src mypy . --explicit-package-bases