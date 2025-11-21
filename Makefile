.PHONY: dev backend frontend test lint clean help

# Default target
help:
	@echo "FEC Query - Development Commands"
	@echo ""
	@echo "Available commands:"
	@echo "  make dev       - Start backend and frontend in watch mode"
	@echo "  make backend   - Start backend only"
	@echo "  make frontend  - Start frontend only"
	@echo "  make test      - Run backend and frontend tests"
	@echo "  make lint      - Run linters for backend and frontend"
	@echo "  make clean     - Clean up temporary files and processes"

# Start both backend and frontend in parallel
dev:
	@echo "Starting backend and frontend..."
	@cd backend && if [ -d "venv" ]; then \
		. venv/bin/activate && uvicorn app.main:app --reload --port 8000 & \
	else \
		uvicorn app.main:app --reload --port 8000 & \
	fi
	@cd frontend && npm run dev &
	@echo "Backend: http://localhost:8000"
	@echo "Frontend: http://localhost:3000"
	@echo "Press Ctrl+C to stop all services"

# Start backend only
backend:
	@echo "Starting backend..."
	@cd backend && if [ -d "venv" ]; then \
		. venv/bin/activate && uvicorn app.main:app --reload --port 8000; \
	else \
		uvicorn app.main:app --reload --port 8000; \
	fi

# Start frontend only
frontend:
	@echo "Starting frontend..."
	@cd frontend && npm run dev

# Run all tests
test:
	@echo "Running backend tests..."
	@cd backend && if [ -d "venv" ]; then \
		. venv/bin/activate && pytest; \
	else \
		pytest; \
	fi
	@echo ""
	@echo "Running frontend tests..."
	@cd frontend && npm test -- --run

# Run linters
lint:
	@echo "Linting backend..."
	@cd backend && if [ -d "venv" ]; then \
		. venv/bin/activate && python -m ruff check . || echo "Ruff not installed, skipping..."; \
	else \
		python -m ruff check . || echo "Ruff not installed, skipping..."; \
	fi
	@echo ""
	@echo "Linting frontend..."
	@cd frontend && npm run lint

# Clean up
clean:
	@echo "Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true
	@echo "Cleanup complete"

