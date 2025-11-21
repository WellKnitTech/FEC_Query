#!/bin/bash

# FEC Query Application Startup Script

# Parse command line arguments
SHOW_LOGS=false
if [[ "$1" == "--logs" ]] || [[ "$1" == "-l" ]]; then
    SHOW_LOGS=true
fi

echo "Starting FEC Campaign Finance Analysis Tool..."
if [ "$SHOW_LOGS" = true ]; then
    echo "📋 Logging mode: Logs will be displayed in this terminal"
fi
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

# Create logs directory if showing logs
if [ "$SHOW_LOGS" = true ]; then
    mkdir -p logs
    BACKEND_LOG="logs/backend.log"
    FRONTEND_LOG="logs/frontend.log"
    # Clear old logs
    > "$BACKEND_LOG"
    > "$FRONTEND_LOG"
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

# Get worker count from environment or default to 1 for development
UVICORN_WORKERS=${UVICORN_WORKERS:-1}

echo "Backend starting on http://localhost:8000 with ${UVICORN_WORKERS} worker(s)"
if [ "$SHOW_LOGS" = true ]; then
    # Run with access logs enabled, output to log file
    if [ "$UVICORN_WORKERS" -gt 1 ]; then
        echo "  Running with ${UVICORN_WORKERS} worker(s) - logs: ../$BACKEND_LOG"
        uvicorn app.main:app --reload --port 8000 --workers $UVICORN_WORKERS --access-log --log-level debug > "../$BACKEND_LOG" 2>&1 &
    else
        echo "  Logs: ../$BACKEND_LOG"
        uvicorn app.main:app --reload --port 8000 --access-log --log-level debug > "../$BACKEND_LOG" 2>&1 &
    fi
    BACKEND_PID=$!
else
    # Run in background without logs
    if [ "$UVICORN_WORKERS" -gt 1 ]; then
        uvicorn app.main:app --reload --port 8000 --workers $UVICORN_WORKERS > /dev/null 2>&1 &
    else
        uvicorn app.main:app --reload --port 8000 > /dev/null 2>&1 &
    fi
    BACKEND_PID=$!
fi
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
if [ "$SHOW_LOGS" = true ]; then
    echo "  Logs: ../$FRONTEND_LOG"
    npm run dev > "../$FRONTEND_LOG" 2>&1 &
else
    npm run dev > /dev/null 2>&1 &
fi
FRONTEND_PID=$!
cd ..

# Function to cleanup processes
cleanup() {
    echo ""
    echo "Shutting down servers..."
    
    # Kill tail processes if they exist
    pkill -f "tail -f.*backend.log" 2>/dev/null
    pkill -f "tail -f.*frontend.log" 2>/dev/null
    
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

echo ""
echo "✅ Application started!"
echo "   Backend:  http://localhost:8000"
echo "   Frontend: http://localhost:3000"
echo "   API Docs: http://localhost:8000/docs"
echo ""

if [ "$SHOW_LOGS" = true ]; then
    echo "📋 Showing logs below (backend and frontend output will appear here)"
    echo "   Press Ctrl+C to stop both servers"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    # Wait a moment for log files to be created
    sleep 1
    
    # Tail both log files with labels
    # Use a function to add prefixes to each line
    tail -f "$BACKEND_LOG" 2>/dev/null | sed 's/^/[BACKEND] /' &
    TAIL_BACKEND_PID=$!
    tail -f "$FRONTEND_LOG" 2>/dev/null | sed 's/^/[FRONTEND] /' &
    TAIL_FRONTEND_PID=$!
    
    # Wait for either tail process to exit (or user interrupt)
    wait $TAIL_BACKEND_PID $TAIL_FRONTEND_PID 2>/dev/null || true
else
    echo "💡 Tip: Run './start.sh --logs' or './start.sh -l' to see logs in real-time"
    echo ""
    echo "Press Ctrl+C to stop both servers"
    
    # Wait for user interrupt
    wait
fi

