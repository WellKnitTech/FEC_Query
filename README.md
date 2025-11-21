# FEC Campaign Finance Analysis Tool

A comprehensive web application for querying, analyzing, and visualizing Federal Election Commission (FEC) campaign finance data. Features include candidate financial analysis, money flow tracking, and fraud detection capabilities.

## Features

### Core Functionality
- **Candidate Search**: Search for federal candidates by name, office, state, party, and election year
- **Race Analysis**: Compare candidates in the same race with side-by-side financial comparisons
- **Financial Analysis**: View detailed financial summaries including receipts, disbursements, and cash on hand
- **Contribution Analysis**: Analyze contributions by donor, date, amount, and geographic location
- **Donor Aggregation**: Automatically group contributions from the same donor (handles name variations)
- **Money Flow Tracking**: Interactive network graphs showing donor → committee → candidate relationships
- **Fraud Detection**: Automated detection of suspicious patterns including:
  - Smurfing (multiple contributions just under reporting thresholds)
  - Threshold clustering (contributions near legal limits)
  - Temporal anomalies (unusual timing patterns)
  - Round number patterns
  - Same-day multiple contributions
- **Contribution Limits**: Track contribution limits by contributor type (individual, PAC, party) and year
- **Committee Management**: Search and analyze committees, view financials, contributions, and expenditures
- **Independent Expenditures**: Track and analyze independent expenditures supporting or opposing candidates
- **Trend Analysis**: Multi-cycle financial trends and contribution velocity patterns
- **Saved Searches**: Save and reuse common search queries
- **Bulk Data Management**: Download and import FEC bulk CSV files for offline analysis
- **Export Functionality**: Export reports in PDF, DOCX, CSV, Excel, and Markdown formats

### Visualizations
- Time series charts for contribution trends
- Bar charts for top donors and state breakdowns
- Network graphs for money flow relationships
- Distribution charts for contribution amounts
- Interactive fraud alert displays

## Implementation Status

For detailed feature completeness information, see [FEATURE_COMPLETENESS_REPORT.md](FEATURE_COMPLETENESS_REPORT.md).

| Feature | Status | Completeness |
|---------|--------|--------------|
| Candidate Search | ✅ Complete | 100% |
| Candidate Details | ✅ Complete | 100% |
| Financial Summaries | ✅ Complete | 100% |
| Contribution Analysis | ✅ Complete | 100% |
| Money Flow Tracking | ✅ Complete | 100% |
| Fraud Detection | ⚠️ In Progress | 95% (TODOs) |
| Bulk Data Management | ✅ Complete | 100% |
| Export Functionality | ✅ Complete | 100% |
| Independent Expenditures | ✅ Complete | 100% |
| Committee Management | ✅ Complete | 100% |
| Saved Searches | ✅ Complete | 100% |
| Trend Analysis | ✅ Complete | 100% |
| Settings Management | ✅ Complete | 100% |
| Donor Aggregation | ✅ Complete | 100% |
| Contribution Limits | ⚠️ In Progress | 95% (TODOs) |

**Overall Completeness: 85%** - Production ready with minor enhancements recommended.

## Architecture

- **Backend**: FastAPI (Python) with async support
- **Frontend**: React + TypeScript with Vite
- **Data Source**: OpenFEC API (api.open.fec.gov) + Bulk CSV downloads
- **Database**: SQLite with caching layer and local bulk data storage
- **Visualization**: Chart.js, Plotly.js, vis-network

## Prerequisites

- Python 3.11 or 3.12 (recommended) - Python 3.13 may have compatibility issues with some packages
- Node.js 18+
- npm or yarn
- OpenFEC API key (register at https://api.open.fec.gov/developers/)

**Note**: If you encounter build errors with Python 3.13, we recommend using Python 3.12 for better compatibility.

**Windows Users**: See [docs/WINDOWS_DEV.md](docs/WINDOWS_DEV.md) for Windows-specific setup instructions using Docker or WSL.

## Installation

### Backend Setup

1. Navigate to the backend directory:
```bash
cd backend
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file (copy from `env.example`):
```bash
cp env.example .env
```

5. Edit `.env` and add your OpenFEC API key:
```
FEC_API_KEY=your_api_key_here
```

### Frontend Setup

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

## Running the Application

### Start the Backend

From the `backend` directory:
```bash
uvicorn app.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`
API documentation: `http://localhost:8000/docs`

### Start the Frontend

From the `frontend` directory:
```bash
npm run dev
```

The application will be available at `http://localhost:3000`

## Usage

1. **Search for Candidates**: Use the search bar on the homepage to find candidates by name
2. **View Financial Data**: Click on a candidate to see their financial summary and contribution analysis
3. **Explore Money Flows**: View the network graph to see how money flows from donors through committees to candidates
4. **Review Fraud Alerts**: Check the fraud detection section for any suspicious patterns identified

## Screenshots

### Candidate Search
![Candidate Search Interface](docs/screenshots/candidate-search.png)
*Search for federal candidates by name, office, state, party, and election year*

### Contribution List
![Contribution List](docs/screenshots/contribution-list.png)
*View detailed contribution records with filtering and sorting capabilities*

### Fraud Detection Analysis
![Fraud Detection](docs/screenshots/fraud-detection.png)
*Automated fraud detection showing suspicious patterns and risk scores*

*Note: Screenshots will be added to the `docs/screenshots/` directory. Placeholder paths are shown above.*

## API Endpoints

### Candidates
- `GET /api/candidates/search` - Search for candidates
- `GET /api/candidates/race` - Get all candidates for a specific race
- `GET /api/candidates/{candidate_id}` - Get candidate details
- `GET /api/candidates/{candidate_id}/financials` - Get financial summary
- `GET /api/candidates/{candidate_id}/debug-contact` - Debug endpoint for contact information
- `POST /api/candidates/{candidate_id}/refresh-contact-info` - Manually refresh contact information
- `POST /api/candidates/financials/batch` - Get financial summaries for multiple candidates

### Contributions
- `GET /api/contributions/` - Get contributions with filters
- `GET /api/contributions/unique-contributors` - Get unique contributor names matching a search term
- `GET /api/contributions/aggregated-donors` - Get aggregated donors (grouped by name variations)
- `GET /api/contributions/analysis` - Get contribution analysis

### Analysis
- `GET /api/analysis/money-flow` - Get money flow network graph
- `GET /api/analysis/expenditure-breakdown` - Get expenditure breakdown with category aggregation
- `GET /api/analysis/employer-breakdown` - Get contribution breakdown by employer
- `GET /api/analysis/velocity` - Get contribution velocity (contributions per day/week)

### Fraud Detection
- `GET /api/fraud/analyze` - Analyze candidate for fraud patterns
- `GET /api/fraud/analyze-donors` - Analyze fraud using donor aggregation for more accurate detection

### Committees
- `GET /api/committees/search` - Search for committees
- `GET /api/committees/{committee_id}` - Get committee details
- `GET /api/committees/{committee_id}/financials` - Get committee financial summary
- `GET /api/committees/{committee_id}/contributions` - Get contributions received by committee
- `GET /api/committees/{committee_id}/expenditures` - Get expenditures made by committee
- `GET /api/committees/{committee_id}/transfers` - Get committee-to-committee transfers

### Independent Expenditures
- `GET /api/independent-expenditures/` - Get independent expenditures with filters
- `GET /api/independent-expenditures/analysis` - Analyze independent expenditures with aggregations
- `GET /api/independent-expenditures/{candidate_id}/summary` - Get independent expenditure summary for a candidate

### Trends
- `GET /api/trends/candidate/{candidate_id}` - Get multi-cycle financial trends for a candidate
- `POST /api/trends/race` - Compare multiple candidates across cycles
- `GET /api/trends/contribution-velocity/{candidate_id}` - Get historical contribution velocity patterns

### Saved Searches
- `GET /api/saved-searches/` - List all saved searches
- `POST /api/saved-searches/` - Create a new saved search
- `GET /api/saved-searches/{search_id}` - Get a saved search by ID
- `PUT /api/saved-searches/{search_id}` - Update a saved search
- `DELETE /api/saved-searches/{search_id}` - Delete a saved search

### Settings
- `GET /api/settings/api-key` - Get current API key status (masked)
- `POST /api/settings/api-key` - Set or update API key
- `DELETE /api/settings/api-key` - Remove API key (soft delete)

### Bulk Data Management
- `POST /api/bulk-data/download?cycle={year}` - Download and import CSV for a specific cycle
- `POST /api/bulk-data/import-multiple` - Import multiple data types for a cycle
- `POST /api/bulk-data/import-all-types` - Import all implemented data types for a cycle
- `GET /api/bulk-data/data-types` - List all available data types with status
- `GET /api/bulk-data/status` - Get status of bulk data downloads
- `GET /api/bulk-data/status/{cycle}` - Get status for a specific cycle
- `GET /api/bulk-data/cycles` - List available cycles with bulk data
- `POST /api/bulk-data/update` - Trigger update check for bulk data
- `POST /api/bulk-data/backfill-candidate-ids` - Backfill candidate IDs from committee linkages
- `GET /api/bulk-data/backfill-candidate-ids/stats` - Get backfill statistics
- `DELETE /api/bulk-data/contributions` - Delete all contributions data
- `DELETE /api/bulk-data/all-data` - Delete all bulk data
- `POST /api/bulk-data/import-all` - Import all data types for all available cycles
- `POST /api/bulk-data/cleanup-and-reimport` - Clean up and reimport data
- `GET /api/bulk-data/jobs/{job_id}/status` - Get job status
- `POST /api/bulk-data/jobs/{job_id}/cancel` - Cancel a running job
- `POST /api/bulk-data/jobs/{job_id}/resume` - Resume an incomplete job
- `GET /api/bulk-data/jobs/incomplete` - List incomplete jobs
- `GET /api/bulk-data/committee-ids/invalid` - Get invalid committee IDs
- `POST /api/bulk-data/committee-ids/fix` - Fix invalid committee IDs

### Export
- `GET /api/export/candidate/{candidate_id}` - Export candidate report (PDF, DOCX, CSV, Excel, Markdown)
- `POST /api/export/race` - Export race report (PDF, DOCX, CSV, Excel, Markdown)
- `GET /api/export/contributions/csv` - Export contributions as CSV
- `GET /api/export/contributions/excel` - Export contributions as Excel

## Fraud Detection Algorithms

The application includes several fraud detection algorithms:

1. **Smurfing Detection**: Identifies multiple contributions just under the $200 reporting threshold from similar sources (same name, address, or employer)
2. **Threshold Clustering**: Flags contributors with multiple contributions near legal limits
3. **Temporal Anomalies**: Detects unusual timing patterns (many contributions on the same day)
4. **Round Number Patterns**: Identifies excessive use of round number contributions
5. **Same-Day Multiple**: Detects multiple contributions from the same source on the same day

Each pattern is assigned a severity level (low, medium, high) and confidence score.

## Database Migrations

The application uses Alembic for database migrations. Migrations are automatically applied on application startup.

For detailed database schema documentation, see [backend/db/SCHEMA.md](backend/db/SCHEMA.md).

### Running Migrations Manually

**Check current migration version**:
```bash
cd backend
alembic current
```

**Apply pending migrations**:
```bash
alembic upgrade head
```

**Create a new migration**:
```bash
# Auto-generate from model changes
alembic revision --autogenerate -m "Description of changes"

# Create empty migration for manual changes
alembic revision -m "Description of changes"
```

**Rollback migrations**:
```bash
# Rollback one migration
alembic downgrade -1

# Rollback to specific revision
alembic downgrade <revision>
```

**View migration history**:
```bash
alembic history
```

For more details, see `backend/ALEMBIC_SETUP.md`.

## Development

### Project Structure

```
FEC_Query/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI application
│   │   ├── config.py            # Centralized configuration
│   │   ├── api/routes/          # API endpoints
│   │   ├── services/            # Business logic
│   │   ├── models/              # Data models
│   │   ├── db/                  # Database setup
│   │   ├── utils/               # Utility functions
│   │   └── lifecycle/           # Startup/shutdown tasks
│   ├── alembic/                 # Alembic migrations
│   ├── migrations/              # Legacy migrations (for reference)
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/          # React components
│   │   ├── pages/               # Page components
│   │   └── services/            # API client
│   └── package.json
└── README.md
```

### Development Workflow

**1. Local Development Setup**:
- Create virtual environment and install dependencies (see Installation)
- Copy `env.example` to `.env` and configure
- Run database migrations: `alembic upgrade head`
- Start backend: `uvicorn app.main:app --reload`
- Start frontend: `npm run dev`

**2. Testing**:
- Backend tests: `pytest` (from backend directory)
- Frontend tests: `npm test` (from frontend directory)
- Integration tests: See `TESTING_GUIDE.md`

**3. Database Migrations**:
- Create migration: `alembic revision --autogenerate -m "description"`
- Review generated migration file
- Apply: `alembic upgrade head`
- Test rollback: `alembic downgrade -1`

**4. Code Quality**:
- Type hints: All service and route files should have type hints
- Documentation: Module-level docstrings for all modules
- Logging: Use structured logging with context
- Error handling: Use specific exception types with structured responses

### Performance Optimizations

The application includes several performance optimizations:

**Database**:
- Connection pooling with optimized settings for SQLite/PostgreSQL
- Composite indexes on frequently queried columns
- WAL mode for better concurrency
- Periodic WAL checkpointing to prevent file growth
- Chunked processing for large result sets

**Query Optimization**:
- Query limits to prevent memory issues
- Streaming for large datasets
- Parallelized operations using `asyncio.gather()`
- N+1 query pattern fixes with eager loading

**Caching**:
- API response caching with configurable TTLs
- Cache hit/miss metrics
- Automatic cache cleanup

**Bulk Data Processing**:
- Chunked processing with configurable batch sizes
- Savepoint-based error recovery
- Memory management with explicit cleanup
- Resume capability for interrupted imports

## Configuration

The application uses centralized configuration management via `app/config.py`. All configuration values can be set via environment variables with sensible defaults.

### Environment Variables

**Backend (.env)**:
- `FEC_API_KEY`: Your OpenFEC API key (required)
- `FEC_API_BASE_URL`: API base URL (default: https://api.open.fec.gov/v1)
- `DATABASE_URL`: Database connection string (default: sqlite+aiosqlite:///./fec_data.db)
- `CORS_ORIGINS`: Allowed CORS origins (comma-separated, default: http://localhost:3000,http://localhost:5173)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, default: INFO)
- `LOG_JSON`: Use JSON logging format (true/false, default: false)
- `UVICORN_WORKERS`: Number of Uvicorn workers (default: 1)
- `THREAD_POOL_WORKERS`: Number of thread pool workers (default: 4)

**Cache Configuration**:
- `CACHE_TTL_HOURS`: Cache time-to-live in hours (default: 24)
- `CACHE_TTL_CANDIDATES_HOURS`: Cache TTL for candidate data (default: 168 = 7 days)
- `CACHE_TTL_COMMITTEES_HOURS`: Cache TTL for committee data (default: 168 = 7 days)
- `CACHE_TTL_FINANCIALS_HOURS`: Cache TTL for financial data (default: 24 hours)
- `CACHE_TTL_CONTRIBUTIONS_HOURS`: Cache TTL for contribution API responses (default: 24 hours)
- `CACHE_TTL_EXPENDITURES_HOURS`: Cache TTL for expenditure data (default: 24 hours)

**Database Configuration**:
- `SQLITE_POOL_SIZE`: SQLite connection pool size (default: 10)
- `SQLITE_MAX_OVERFLOW`: SQLite max overflow connections (default: 10)
- `POSTGRES_POOL_SIZE`: PostgreSQL connection pool size (default: 20)
- `POSTGRES_MAX_OVERFLOW`: PostgreSQL max overflow connections (default: 30)
- `SQLITE_MAX_BATCH_SIZE`: Maximum batch size for SQLite inserts (default: 90)
- `SQLITE_BULK_BATCH_SIZE`: Batch size for bulk imports (default: 500)

**Performance Configuration**:
- `ANALYSIS_CHUNK_SIZE`: Chunk size for analysis queries (default: 10000)
- `CONTRIBUTION_LOOKBACK_DAYS`: Days to look back when fetching new contributions (default: 30)
- `WAL_CHECKPOINT_INTERVAL_SECONDS`: WAL checkpoint interval (default: 1800 = 30 minutes)
- `INTEGRITY_CHECK_INTERVAL_HOURS`: Database integrity check interval (default: 24)

**Bulk Data Configuration**:
- `BULK_DATA_ENABLED`: Enable bulk CSV data usage (default: true)
- `BULK_DATA_DIR`: Directory for storing CSV files (default: ./data/bulk)
- `BULK_DATA_UPDATE_INTERVAL_HOURS`: Hours between update checks (default: 24)
- `FEC_BULK_DATA_BASE_URL`: Base URL for FEC bulk downloads (default: https://www.fec.gov/files/bulk-downloads/)

### Structured Logging

The application supports structured logging with JSON output for production environments.

**Enable JSON Logging**:
```bash
LOG_JSON=true
```

**Log Levels**:
- `DEBUG`: Detailed debugging information
- `INFO`: General informational messages (default)
- `WARNING`: Warning messages
- `ERROR`: Error messages

Logs include structured fields such as:
- `timestamp`: ISO format timestamp
- `level`: Log level
- `logger`: Logger name
- `message`: Log message
- `module`, `function`, `line`: Code location
- `request_id`: Request identifier (when available)
- Additional context fields as needed

## Bulk CSV Data

To reduce API calls and avoid rate limits, the application supports downloading and using FEC bulk CSV files for contributions data (Schedule A). This dramatically reduces API usage for contribution queries.

### Setting Up Bulk Data

1. **Enable bulk data** (enabled by default):
   ```
   BULK_DATA_ENABLED=true
   ```

2. **Download initial data**:
   - Use the API endpoint: `POST /api/bulk-data/download?cycle=2024`
   - Or trigger via the API docs at `http://localhost:8000/docs`
   - This will download the Schedule A CSV for the specified cycle and import it into the database

3. **Automatic updates**:
   - The system can automatically check for updates (configurable via `BULK_DATA_UPDATE_INTERVAL_HOURS`)
   - Manual updates can be triggered via `POST /api/bulk-data/update`

### How It Works

- When bulk data is enabled, contribution queries first check the local database
- If local data exists and matches the query filters, it's returned immediately (no API call)
- If local data is missing or doesn't match, the system falls back to the API
- This provides the best of both worlds: fast local queries with API fallback

### Benefits

- **Reduced API calls**: Contribution queries use local data, saving API quota
- **Faster queries**: Local database queries are much faster than API calls
- **Offline capability**: Once downloaded, data can be queried without API access
- **Rate limit protection**: Bulk data eliminates the need for most contribution API calls

### Data Storage

- CSV files are stored in the directory specified by `BULK_DATA_DIR` (default: `./data/bulk`)
- Imported data is stored in the SQLite database in the `contributions` table
- Metadata about downloads is tracked in the `bulk_data_metadata` table

## Performance Features

### Chunked Processing
Large datasets are processed in chunks to prevent memory exhaustion. The default chunk size is 10,000 records, configurable via `ANALYSIS_CHUNK_SIZE`.

### Connection Pooling
Optimized connection pool settings for both SQLite and PostgreSQL:
- SQLite: Smaller pools (10 connections) for better performance
- PostgreSQL: Larger pools (20 connections) for higher concurrency

### Query Optimization
- Automatic query limits on analysis queries
- Streaming for large result sets
- Parallelized API and database queries
- Composite indexes on frequently queried columns

### Background Tasks
- Periodic WAL checkpointing (every 30 minutes)
- Daily database integrity checks
- Automatic cache cleanup
- Background contact information updates

## Limitations

- API rate limits: The OpenFEC API has rate limits. The application includes caching and bulk CSV downloads to minimize API calls.
- Data freshness: Cached data may be up to 24 hours old (configurable). Bulk CSV data is updated based on FEC release schedule.
- Fraud detection: The algorithms are heuristic-based and may produce false positives. Always verify suspicious patterns manually.
- Bulk CSV files: Can be very large (GBs). Ensure sufficient disk space and allow time for initial download/import.
- SQLite limitations: SQLite has limited ALTER TABLE support. Complex schema changes may require table recreation.

## Roadmap

Planned improvements and enhancements:

- **Complete FEC Transaction Type Parsing**: Implement full parsing of FEC transaction type codes to improve contributor category determination
- **API Versioning**: Add versioning support for better API stability and backward compatibility
- **Performance Monitoring Dashboard**: Real-time metrics and performance insights
- **Enhanced Fraud Detection Algorithms**: Improved pattern recognition and reduced false positives
- **Real-time WebSocket Updates**: Enhanced real-time updates for bulk data import progress
- **Mobile-Responsive Improvements**: Optimize UI for mobile and tablet devices
- **Advanced Export Templates**: Customizable report templates with branding options
- **Integration Testing Suite**: Comprehensive end-to-end testing coverage

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Required tools and setup
- Branch strategy
- Testing requirements
- Code formatting standards
- Pull request process

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Data provided by the Federal Election Commission via the OpenFEC API
- Built with FastAPI, React, and modern web technologies

