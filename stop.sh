#!/bin/bash

# FEC Query Application Shutdown Script

echo "Stopping FEC Campaign Finance Analysis Tool..."
echo ""

# Function to gracefully kill a process
graceful_kill() {
    local pid=$1
    local name=$2
    
    if [ ! -z "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "  Stopping $name (PID: $pid)..."
        kill -TERM $pid 2>/dev/null
        sleep 2
        
        # Check if still running, force kill if needed
        if kill -0 $pid 2>/dev/null; then
            echo "  Force killing $name..."
            kill -9 $pid 2>/dev/null
        fi
    fi
}

# Stop backend processes
echo "Stopping backend processes..."

# Try to find and gracefully stop uvicorn processes
BACKEND_PIDS=$(pgrep -f "uvicorn app.main:app" 2>/dev/null)
if [ ! -z "$BACKEND_PIDS" ]; then
    for pid in $BACKEND_PIDS; do
        echo "  Stopping backend (PID: $pid)..."
        kill -TERM $pid 2>/dev/null
    done
    sleep 2
    
    # Force kill if still running
    BACKEND_PIDS=$(pgrep -f "uvicorn app.main:app" 2>/dev/null)
    if [ ! -z "$BACKEND_PIDS" ]; then
        for pid in $BACKEND_PIDS; do
            echo "  Force killing backend (PID: $pid)..."
            kill -9 $pid 2>/dev/null
        done
    fi
fi

# Kill processes using port 8000
if command -v lsof >/dev/null 2>&1; then
    PORT_8000_PIDS=$(lsof -ti:8000 2>/dev/null)
    if [ ! -z "$PORT_8000_PIDS" ]; then
        for pid in $PORT_8000_PIDS; do
            echo "  Stopping process on port 8000 (PID: $pid)..."
            kill -TERM $pid 2>/dev/null
        done
        sleep 1
        PORT_8000_PIDS=$(lsof -ti:8000 2>/dev/null)
        if [ ! -z "$PORT_8000_PIDS" ]; then
            for pid in $PORT_8000_PIDS; do
                kill -9 $pid 2>/dev/null
            done
        fi
    fi
fi

if command -v fuser >/dev/null 2>&1; then
    fuser -k 8000/tcp 2>/dev/null
fi

# Stop frontend processes
echo "Stopping frontend processes..."

# Try to find and gracefully stop vite processes
FRONTEND_PIDS=$(pgrep -f "vite" 2>/dev/null)
if [ ! -z "$FRONTEND_PIDS" ]; then
    for pid in $FRONTEND_PIDS; do
        echo "  Stopping frontend (PID: $pid)..."
        kill -TERM $pid 2>/dev/null
    done
    sleep 2
    
    # Force kill if still running
    FRONTEND_PIDS=$(pgrep -f "vite" 2>/dev/null)
    if [ ! -z "$FRONTEND_PIDS" ]; then
        for pid in $FRONTEND_PIDS; do
            echo "  Force killing frontend (PID: $pid)..."
            kill -9 $pid 2>/dev/null
        done
    fi
fi

# Kill processes using ports 3000 and 5173 (vite default)
if command -v lsof >/dev/null 2>&1; then
    PORT_3000_PIDS=$(lsof -ti:3000 2>/dev/null)
    if [ ! -z "$PORT_3000_PIDS" ]; then
        for pid in $PORT_3000_PIDS; do
            echo "  Stopping process on port 3000 (PID: $pid)..."
            kill -TERM $pid 2>/dev/null
        done
        sleep 1
        PORT_3000_PIDS=$(lsof -ti:3000 2>/dev/null)
        if [ ! -z "$PORT_3000_PIDS" ]; then
            for pid in $PORT_3000_PIDS; do
                kill -9 $pid 2>/dev/null
            done
        fi
    fi
    
    PORT_5173_PIDS=$(lsof -ti:5173 2>/dev/null)
    if [ ! -z "$PORT_5173_PIDS" ]; then
        for pid in $PORT_5173_PIDS; do
            echo "  Stopping process on port 5173 (PID: $pid)..."
            kill -TERM $pid 2>/dev/null
        done
        sleep 1
        PORT_5173_PIDS=$(lsof -ti:5173 2>/dev/null)
        if [ ! -z "$PORT_5173_PIDS" ]; then
            for pid in $PORT_5173_PIDS; do
                kill -9 $pid 2>/dev/null
            done
        fi
    fi
fi

if command -v fuser >/dev/null 2>&1; then
    fuser -k 3000/tcp 2>/dev/null
    fuser -k 5173/tcp 2>/dev/null
fi

# Wait for processes to fully terminate
sleep 1

# Final cleanup - kill any remaining processes
pkill -f "uvicorn app.main:app" 2>/dev/null
pkill -f "uvicorn.*8000" 2>/dev/null
pkill -f "vite" 2>/dev/null
pkill -f "node.*vite" 2>/dev/null

# Verify processes are stopped
if pgrep -f "uvicorn app.main:app" >/dev/null 2>&1 || pgrep -f "vite" >/dev/null 2>&1; then
    echo "  ⚠️  Warning: Some processes may still be running"
    echo "  Attempting final force kill..."
    pkill -9 -f "uvicorn app.main:app" 2>/dev/null
    pkill -9 -f "vite" 2>/dev/null
    sleep 1
fi

# Final verification
if pgrep -f "uvicorn app.main:app" >/dev/null 2>&1; then
    echo "  ⚠️  Warning: Backend processes may still be running"
else
    echo "  ✓ Backend stopped"
fi

if pgrep -f "vite" >/dev/null 2>&1; then
    echo "  ⚠️  Warning: Frontend processes may still be running"
else
    echo "  ✓ Frontend stopped"
fi

echo ""
echo "✅ Application stopped!"
echo ""

