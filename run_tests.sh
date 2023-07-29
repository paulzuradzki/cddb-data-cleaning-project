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
mypy src --explicit-package-bases --check-untyped-defs --strict
mypy tests --explicit-package-bases --check-untyped-defs --strict --ignore-missing-imports

# Package linting
echo "Package linting..."
pyroma .
