#!/bin/bash

# Script to generate TypeScript types from FastAPI OpenAPI schema
# This script fetches the OpenAPI schema from the running backend and generates types

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TYPES_DIR="$FRONTEND_DIR/src/api/types"
API_URL="${VITE_API_URL:-http://localhost:8000}"
SCHEMA_URL="$API_URL/openapi.json"
OUTPUT_FILE="$TYPES_DIR/generated.ts"

echo "Generating TypeScript types from OpenAPI schema..."
echo "API URL: $API_URL"
echo "Schema URL: $SCHEMA_URL"
echo "Output: $OUTPUT_FILE"

# Check if backend is running
if ! curl -s -f "$API_URL/health" > /dev/null 2>&1; then
    echo "Warning: Backend is not running at $API_URL"
    echo "Please start the backend server first:"
    echo "  cd backend && uvicorn app.main:app --reload"
    echo ""
    echo "Or set VITE_API_URL to point to a running backend:"
    echo "  export VITE_API_URL=http://your-backend-url:8000"
    exit 1
fi

# Create types directory if it doesn't exist
mkdir -p "$TYPES_DIR"

# Check if openapi-typescript is installed
if ! command -v npx &> /dev/null; then
    echo "Error: npx is not available. Please install Node.js."
    exit 1
fi

# Fetch OpenAPI schema and generate types
echo "Fetching OpenAPI schema..."
if curl -s -f "$SCHEMA_URL" > /dev/null 2>&1; then
    echo "Generating TypeScript types..."
    npx --yes openapi-typescript "$SCHEMA_URL" -o "$OUTPUT_FILE"
    
    if [ $? -eq 0 ]; then
        echo "✓ Types generated successfully at $OUTPUT_FILE"
        echo ""
        echo "You can now import types from:"
        echo "  import type { paths, components } from '@/api/types/generated'"
    else
        echo "Error: Failed to generate types"
        exit 1
    fi
else
    echo "Error: Could not fetch OpenAPI schema from $SCHEMA_URL"
    echo "Please ensure the backend is running and accessible."
    exit 1
fi

