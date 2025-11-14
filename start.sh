#!/bin/bash

# FEC Query Application Startup Script

echo "Starting FEC Campaign Finance Analysis Tool..."
echo ""

# Kill any existing processes on ports 8000 and 3000
echo "Checking for existing processes..."

# Kill backend processes
echo "  Stopping backend processes..."
pkill -f "uvicorn app.main:app" 2>/dev/null
pkill -f "uvicorn.*8000" 2>/dev/null
# Kill processes using port 8000
if command -v lsof >/dev/null 2>&1; then
    lsof -ti:8000 | xargs kill -9 2>/dev/null
fi
if command -v fuser >/dev/null 2>&1; then
    fuser -k 8000/tcp 2>/dev/null
fi

# Kill frontend processes
echo "  Stopping frontend processes..."
pkill -f "vite" 2>/dev/null
pkill -f "node.*vite" 2>/dev/null
# Kill processes using port 3000
if command -v lsof >/dev/null 2>&1; then
    lsof -ti:3000 | xargs kill -9 2>/dev/null
    lsof -ti:5173 | xargs kill -9 2>/dev/null
fi
if command -v fuser >/dev/null 2>&1; then
    fuser -k 3000/tcp 2>/dev/null
    fuser -k 5173/tcp 2>/dev/null
fi

# Wait for processes to fully terminate
sleep 2

# Verify processes are killed
if pgrep -f "uvicorn app.main:app" >/dev/null 2>&1 || pgrep -f "vite" >/dev/null 2>&1; then
    echo "  ⚠️  Warning: Some processes may still be running"
    echo "  Attempting force kill..."
    pkill -9 -f "uvicorn app.main:app" 2>/dev/null
    pkill -9 -f "vite" 2>/dev/null
    sleep 1
fi

echo "  ✓ Processes cleaned up"

# Check if .env exists in backend
if [ ! -f "backend/.env" ]; then
    echo "⚠️  Warning: backend/.env not found!"
    echo "Please create backend/.env from backend/env.example and add your FEC_API_KEY"
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Start backend
echo "Starting backend server..."
cd backend
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install -q -r requirements.txt

echo "Backend starting on http://localhost:8000"
uvicorn app.main:app --reload --port 8000 &
BACKEND_PID=$!
cd ..

# Wait a moment for backend to start
sleep 3

# Start frontend
echo "Starting frontend server..."
cd frontend
if [ ! -d "node_modules" ]; then
    echo "Installing frontend dependencies..."
    npm install
fi

echo "Frontend starting on http://localhost:3000"
npm run dev &
FRONTEND_PID=$!
cd ..

echo ""
echo "✅ Application started!"
echo "   Backend:  http://localhost:8000"
echo "   Frontend: http://localhost:3000"
echo "   API Docs: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop both servers"

# Function to cleanup processes
cleanup() {
    echo ""
    echo "Shutting down servers..."
    
    # Kill backend process and all its children (uvicorn spawns workers)
    if [ ! -z "$BACKEND_PID" ]; then
        pkill -P $BACKEND_PID 2>/dev/null
        kill $BACKEND_PID 2>/dev/null
    fi
    
    # Kill frontend process and all its children
    if [ ! -z "$FRONTEND_PID" ]; then
        pkill -P $FRONTEND_PID 2>/dev/null
        kill $FRONTEND_PID 2>/dev/null
    fi
    
    # Also kill any remaining uvicorn or vite processes
    pkill -f "uvicorn app.main:app" 2>/dev/null
    pkill -f "vite" 2>/dev/null
    
    echo "Servers stopped."
    exit 0
}

# Set up signal handlers
trap cleanup INT TERM

# Wait for user interrupt
wait

