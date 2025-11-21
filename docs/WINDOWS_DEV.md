# Windows Development Setup

This guide provides instructions for setting up and running the FEC Query application on Windows.

## Prerequisites

- Windows 10 or later
- One of the following:
  - **Docker Desktop** (recommended for easiest setup)
  - **WSL 2** (Windows Subsystem for Linux)
  - **Native Windows** with Python and Node.js

## Option 1: Docker (Recommended)

Docker provides the most consistent development environment across platforms.

### Setup

1. **Install Docker Desktop**
   - Download from https://www.docker.com/products/docker-desktop
   - Install and start Docker Desktop

2. **Clone the repository**
   ```powershell
   git clone https://github.com/yourusername/FEC_Query.git
   cd FEC_Query
   ```

3. **Start services with Docker Compose**
   ```powershell
   docker-compose up -d
   ```

4. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/docs

### Development with Docker

For active development, you may want to mount volumes for live reloading:

```powershell
# Edit docker-compose.yml to add volume mounts
# Then rebuild and restart
docker-compose up --build
```

## Option 2: WSL 2 (Windows Subsystem for Linux)

WSL 2 provides a Linux environment on Windows, allowing you to follow the standard Linux setup instructions.

### Setup

1. **Install WSL 2**
   ```powershell
   # Run in PowerShell as Administrator
   wsl --install
   ```

2. **Install Ubuntu** (or your preferred Linux distribution)
   - Follow prompts to set up a user account

3. **Open WSL terminal** and follow the standard setup:
   ```bash
   # Clone repository
   git clone https://github.com/yourusername/FEC_Query.git
   cd FEC_Query
   
   # Follow standard installation instructions from README.md
   ```

4. **Access from Windows**
   - The application will be accessible from Windows browsers
   - Use `localhost` or `127.0.0.1` in URLs

### Tips for WSL 2

- **File paths**: Use `/mnt/c/` to access Windows drives (e.g., `/mnt/c/Users/YourName/`)
- **VS Code**: Install the "Remote - WSL" extension for seamless development
- **Port forwarding**: WSL 2 automatically forwards ports to Windows

## Option 3: Native Windows

You can run the application natively on Windows, though some setup is required.

### Backend Setup

1. **Install Python 3.12**
   - Download from https://www.python.org/downloads/
   - Check "Add Python to PATH" during installation

2. **Create virtual environment**
   ```powershell
   cd backend
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   pip install -r requirements-test.txt
   ```

4. **Configure environment**
   ```powershell
   copy env.example .env
   # Edit .env and add your FEC_API_KEY
   ```

5. **Run database migrations**
   ```powershell
   alembic upgrade head
   ```

6. **Start backend**
   ```powershell
   uvicorn app.main:app --reload --port 8000
   ```

### Frontend Setup

1. **Install Node.js 18+**
   - Download from https://nodejs.org/
   - Install LTS version

2. **Install dependencies**
   ```powershell
   cd frontend
   npm install
   ```

3. **Start frontend**
   ```powershell
   npm run dev
   ```

### Using Makefile on Windows

The Makefile requires a Unix-like environment. Options:

1. **Use WSL 2** (recommended)
2. **Use Git Bash** (comes with Git for Windows)
3. **Use PowerShell scripts** (create equivalent `.ps1` scripts)

## Common Issues

### Port Already in Use

If port 8000 or 3000 is already in use:

```powershell
# Find process using port
netstat -ano | findstr :8000

# Kill process (replace PID with actual process ID)
taskkill /PID <PID> /F
```

### Python Path Issues

Ensure Python is in your PATH:

```powershell
# Check Python version
python --version

# If not found, add Python to PATH in System Environment Variables
```

### SQLite Database Locked

If you encounter database lock errors:

1. Close all connections to the database
2. Restart the backend server
3. For WSL, ensure you're not accessing the database from Windows and WSL simultaneously

### File Permissions

If you encounter permission errors:

1. Run terminal as Administrator
2. Check file permissions in project directory
3. Ensure antivirus isn't blocking file access

## Development Tools

### Recommended IDEs

- **VS Code** with extensions:
  - Python
  - ESLint
  - Prettier
  - Remote - WSL (if using WSL)

- **PyCharm** (for Python development)

### Terminal Options

- **Windows Terminal** (recommended)
- **PowerShell**
- **Git Bash**
- **WSL Terminal** (if using WSL)

## Getting Help

If you encounter issues:

1. Check the main README.md for general setup instructions
2. Review error messages carefully
3. Check that all prerequisites are installed
4. Open an issue on GitHub with:
   - Windows version
   - Setup method (Docker/WSL/Native)
   - Error messages
   - Steps to reproduce

## Next Steps

Once setup is complete, see:
- [CONTRIBUTING.md](../CONTRIBUTING.md) for development guidelines
- [README.md](../README.md) for usage instructions
- [TESTING_GUIDE.md](../TESTING_GUIDE.md) for testing information

