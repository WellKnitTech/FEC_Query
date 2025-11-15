"""
Migration script to populate FEC contribution limits table
Run this script to initialize the contribution_limits table with historical data
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.database import AsyncSessionLocal, init_db
from app.services.contribution_limits import ContributionLimitsService


async def main():
    """Populate contribution limits table"""
    print("Initializing database...")
    await init_db()
    
    print("Populating contribution limits...")
    async with AsyncSessionLocal() as session:
        service = ContributionLimitsService(session)
        count = await service.populate_historical_limits()
        print(f"Successfully populated {count} contribution limits")


if __name__ == "__main__":
    asyncio.run(main())

