# FEC Campaign Finance Analysis Tool

A comprehensive web application for querying, analyzing, and visualizing Federal Election Commission (FEC) campaign finance data. Features include candidate financial analysis, money flow tracking, and fraud detection capabilities.

## Features

### Core Functionality
- **Candidate Search**: Search for federal candidates by name, office, state, party, and election year
- **Financial Analysis**: View detailed financial summaries including receipts, disbursements, and cash on hand
- **Contribution Analysis**: Analyze contributions by donor, date, amount, and geographic location
- **Money Flow Tracking**: Interactive network graphs showing donor → committee → candidate relationships
- **Fraud Detection**: Automated detection of suspicious patterns including:
  - Smurfing (multiple contributions just under reporting thresholds)
  - Threshold clustering (contributions near legal limits)
  - Temporal anomalies (unusual timing patterns)
  - Round number patterns
  - Same-day multiple contributions

### Visualizations
- Time series charts for contribution trends
- Bar charts for top donors and state breakdowns
- Network graphs for money flow relationships
- Distribution charts for contribution amounts
- Interactive fraud alert displays

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

## API Endpoints

### Candidates
- `GET /api/candidates/search` - Search for candidates
- `GET /api/candidates/{candidate_id}` - Get candidate details
- `GET /api/candidates/{candidate_id}/financials` - Get financial summary

### Contributions
- `GET /api/contributions/` - Get contributions with filters
- `GET /api/contributions/analysis` - Get contribution analysis

### Analysis
- `GET /api/analysis/money-flow` - Get money flow network graph

### Fraud Detection
- `GET /api/fraud/analyze` - Analyze candidate for fraud patterns

### Bulk Data Management
- `POST /api/bulk-data/download?cycle={year}` - Download and import CSV for a specific cycle
- `GET /api/bulk-data/status` - Get status of bulk data downloads
- `GET /api/bulk-data/cycles` - List available cycles with bulk data
- `POST /api/bulk-data/update` - Trigger update check for bulk data

## Fraud Detection Algorithms

The application includes several fraud detection algorithms:

1. **Smurfing Detection**: Identifies multiple contributions just under the $200 reporting threshold from similar sources (same name, address, or employer)
2. **Threshold Clustering**: Flags contributors with multiple contributions near legal limits
3. **Temporal Anomalies**: Detects unusual timing patterns (many contributions on the same day)
4. **Round Number Patterns**: Identifies excessive use of round number contributions
5. **Same-Day Multiple**: Detects multiple contributions from the same source on the same day

Each pattern is assigned a severity level (low, medium, high) and confidence score.

## Development

### Project Structure

```
FEC_Query/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI application
│   │   ├── api/routes/          # API endpoints
│   │   ├── services/            # Business logic
│   │   ├── models/              # Data models
│   │   └── db/                  # Database setup
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/          # React components
│   │   ├── pages/               # Page components
│   │   └── services/            # API client
│   └── package.json
└── README.md
```

## Configuration

### Environment Variables

**Backend (.env)**:
- `FEC_API_KEY`: Your OpenFEC API key (required)
- `FEC_API_BASE_URL`: API base URL (default: https://api.open.fec.gov/v1)
- `DATABASE_URL`: Database connection string
- `CORS_ORIGINS`: Allowed CORS origins (comma-separated)
- `CACHE_TTL_HOURS`: Cache time-to-live in hours (default: 24)
- `CACHE_TTL_CANDIDATES_HOURS`: Cache TTL for candidate data (default: 168 = 7 days)
- `CACHE_TTL_COMMITTEES_HOURS`: Cache TTL for committee data (default: 168 = 7 days)
- `CACHE_TTL_FINANCIALS_HOURS`: Cache TTL for financial data (default: 24 hours)
- `CACHE_TTL_CONTRIBUTIONS_HOURS`: Cache TTL for contribution API responses (default: 24 hours)
- `CACHE_TTL_EXPENDITURES_HOURS`: Cache TTL for expenditure data (default: 24 hours)
- `CONTRIBUTION_LOOKBACK_DAYS`: Days to look back when fetching new contributions to catch late-filed contributions (default: 30)
- `BULK_DATA_ENABLED`: Enable bulk CSV data usage (default: true)
- `BULK_DATA_DIR`: Directory for storing CSV files (default: ./data/bulk)
- `BULK_DATA_UPDATE_INTERVAL_HOURS`: Hours between update checks (default: 24)
- `FEC_BULK_DATA_BASE_URL`: Base URL for FEC bulk downloads (default: https://www.fec.gov/files/bulk-downloads/)

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

## Limitations

- API rate limits: The OpenFEC API has rate limits. The application includes caching and bulk CSV downloads to minimize API calls.
- Data freshness: Cached data may be up to 24 hours old (configurable). Bulk CSV data is updated based on FEC release schedule.
- Fraud detection: The algorithms are heuristic-based and may produce false positives. Always verify suspicious patterns manually.
- Bulk CSV files: Can be very large (GBs). Ensure sufficient disk space and allow time for initial download/import.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Data provided by the Federal Election Commission via the OpenFEC API
- Built with FastAPI, React, and modern web technologies

