# Contributing to FEC Query

Thank you for your interest in contributing to the FEC Campaign Finance Analysis Tool! This document provides guidelines and instructions for contributing.

## Required Tools

Before you begin, ensure you have the following tools installed:

- **Python 3.11 or 3.12** (Python 3.13 may have compatibility issues)
- **Node.js 18+** and npm
- **Git**
- **OpenFEC API Key** (register at https://api.open.fec.gov/developers/)

## Development Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/FEC_Query.git
cd FEC_Query
```

### 2. Backend Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-test.txt
cp env.example .env
# Edit .env and add your FEC_API_KEY
```

### 3. Frontend Setup

```bash
cd frontend
npm install
```

### 4. Database Setup

```bash
cd backend
alembic upgrade head
```

## Branch Strategy

- **main**: Production-ready code
- **develop**: Integration branch for features (if using Git Flow)
- **feature/**: Feature branches (e.g., `feature/add-new-endpoint`)
- **bugfix/**: Bug fix branches (e.g., `bugfix/fix-search-issue`)
- **hotfix/**: Critical production fixes

### Creating a Branch

```bash
git checkout -b feature/your-feature-name
```

## Running the Application

### Using Makefile (Recommended)

```bash
# Start both backend and frontend
make dev

# Start backend only
make backend

# Start frontend only
make frontend
```

### Manual Start

**Backend:**
```bash
cd backend
source venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
npm run dev
```

## Testing

### Backend Tests

```bash
cd backend
pytest
```

### Frontend Tests

```bash
cd frontend
npm test
```

### Run All Tests

```bash
make test
```

## Code Formatting

### Python (Backend)

We use **ruff** for linting and formatting (or **black** if preferred):

```bash
cd backend
# Format code
ruff format .

# Check linting
ruff check .
```

**Python Style Guidelines:**
- Follow PEP 8
- Use type hints for all function parameters and return values
- Add docstrings to all functions and classes
- Maximum line length: 100 characters

### TypeScript/React (Frontend)

We use **ESLint** and **Prettier** (if configured):

```bash
cd frontend
# Lint code
npm run lint

# Format code (if Prettier is configured)
npm run format
```

**TypeScript Style Guidelines:**
- Use TypeScript strict mode
- Prefer functional components with hooks
- Use meaningful variable and function names
- Add JSDoc comments for complex functions

## Pull Request Process

1. **Create a branch** from `main` or `develop`
2. **Make your changes** following the coding standards
3. **Write or update tests** for your changes
4. **Run tests** to ensure everything passes
5. **Update documentation** if needed
6. **Commit your changes** with clear, descriptive messages
7. **Push to your fork** and create a Pull Request

### Commit Message Format

Use clear, descriptive commit messages:

```
feat: Add candidate search by state filter
fix: Resolve database connection pool issue
docs: Update API documentation
test: Add tests for contribution analysis endpoint
refactor: Simplify fraud detection algorithm
```

### Pull Request Checklist

- [ ] Code follows style guidelines
- [ ] Tests pass locally
- [ ] Documentation updated (if needed)
- [ ] No console.log statements (use proper logging)
- [ ] Type hints added (Python) / Types defined (TypeScript)
- [ ] No linter errors

## Code Review

All pull requests require review before merging. Reviewers will check:

- Code quality and style
- Test coverage
- Documentation completeness
- Performance implications
- Security considerations

## Adding New Features

1. **Plan your feature**: Consider the API design, database schema, and user experience
2. **Create a feature branch**: `git checkout -b feature/your-feature`
3. **Implement the feature**: Follow existing patterns and conventions
4. **Add tests**: Ensure adequate test coverage
5. **Update documentation**: README, API docs, or inline comments
6. **Submit PR**: Create a pull request with a clear description

## Reporting Bugs

When reporting bugs, please include:

- **Description**: Clear description of the issue
- **Steps to Reproduce**: Detailed steps to reproduce the bug
- **Expected Behavior**: What should happen
- **Actual Behavior**: What actually happens
- **Environment**: OS, Python version, Node version
- **Screenshots**: If applicable

## Questions?

If you have questions or need help:

- Open an issue on GitHub
- Check existing documentation
- Review the codebase for examples

Thank you for contributing to FEC Query!

