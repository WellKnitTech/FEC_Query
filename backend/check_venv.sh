#!/bin/bash
# Quick script to verify venv setup and dependencies

cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Checking Python version..."
python --version

echo ""
echo "Checking if slowapi is installed..."
if python -c "import slowapi" 2>/dev/null; then
    echo "✅ slowapi is installed"
else
    echo "❌ slowapi is not installed"
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

echo ""
echo "Verifying all security dependencies..."
python -c "
import sys
deps = ['slowapi', 'fastapi', 'uvicorn', 'pydantic']
missing = []
for dep in deps:
    try:
        __import__(dep)
        print(f'✅ {dep}')
    except ImportError:
        print(f'❌ {dep} - MISSING')
        missing.append(dep)

if missing:
    print(f'\n⚠️  Missing dependencies: {missing}')
    sys.exit(1)
else:
    print('\n✅ All dependencies installed!')
"

echo ""
echo "Virtual environment is ready!"
echo "To activate manually, run: source venv/bin/activate"

