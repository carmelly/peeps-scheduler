#!/bin/bash
# Setup script for peeps-scheduler
# Initializes Python environment for production or development
# Usage: ./scripts/setup.sh [--dev]
#
# Modes:
#   ./scripts/setup.sh          Production: Python production dependencies only
#   ./scripts/setup.sh --dev    Development: Python dev deps + Node deps + hydration

set -e

# Source shared library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.peeps-lib.sh"

# Parse arguments
MODE="prod"
if [ "$1" = "--dev" ]; then
    MODE="dev"
elif [ -n "$1" ]; then
    echo "Usage: $0 [--dev]"
    echo ""
    echo "Modes:"
    echo "  $0              Production: Python production dependencies only"
    echo "  $0 --dev        Development: Python dev deps + Node deps + hydration"
    exit 1
fi

print_status "info" "Setting up project ($MODE mode)..."
echo ""

# Development-only: Initialize git hooks and hydrate
if [ "$MODE" = "dev" ]; then
    print_status "info" "Initializing git hooks..."
    git config core.hooksPath .githooks
    print_status "success" "Git hooks configured"

    echo ""

    if [ -d "peeps-config" ]; then
        print_status "info" "Configuring submodules..."
        ./peeps-config/scripts/hydrate.sh
    else
        print_status "info" "peeps-config not found, skipping submodule setup"
    fi

    echo ""
fi

# Check for virtual environment and install dependencies
if [ -d ".venv" ]; then
    print_status "info" "Installing Python dependencies..."
    source .venv/bin/activate
    if [ -f "pyproject.toml" ]; then
        if [ "$MODE" = "dev" ]; then
            python -m pip install -e ".[dev]"
            print_status "success" "Development dependencies installed"
        else
            python -m pip install -e .
            print_status "success" "Production dependencies installed"
        fi
    else
        print_status "info" "No pyproject.toml found"
    fi
else
    print_status "info" "No virtual environment found, skipping dependency installation"
    print_status "info" "Run: python3 -m venv .venv && source .venv/bin/activate"
fi

# Development-only: Install Node dependencies
if [ "$MODE" = "dev" ]; then
    echo ""
    print_status "info" "Installing Node dependencies..."
    if [ -f "package.json" ]; then
        npm install
        print_status "success" "Node dependencies installed"
    else
        print_status "info" "No package.json found, skipping Node dependencies"
    fi
fi

echo ""
print_status "success" "Setup complete ($MODE mode)!"