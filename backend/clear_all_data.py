#!/usr/bin/env python3
"""Script to clear all data from the database"""
import asyncio
from sqlalchemy import delete, text
from app.db.database import (
    AsyncSessionLocal, Contribution, Committee, Candidate, FinancialTotal,
    BulkDataMetadata, BulkImportJob, IndependentExpenditure,
    OperatingExpenditure, CandidateSummary, CommitteeSummary,
    ElectioneeringComm, CommunicationCost
)

async def clear_all_data():
    """Clear all data from the database"""
    async with AsyncSessionLocal() as session:
        deleted_counts = {}
        
        try:
            print("Clearing all data from database...")
            
            # Clear in order to respect foreign key constraints
            # Start with dependent tables first
            
            # Clear contributions
            result = await session.execute(delete(Contribution))
            deleted_counts['contributions'] = result.rowcount
            print(f"Cleared {result.rowcount} contributions")
            
            # Clear financial totals
            result = await session.execute(delete(FinancialTotal))
            deleted_counts['financial_totals'] = result.rowcount
            print(f"Cleared {result.rowcount} financial totals")
            
            # Clear independent expenditures
            result = await session.execute(delete(IndependentExpenditure))
            deleted_counts['independent_expenditures'] = result.rowcount
            print(f"Cleared {result.rowcount} independent expenditures")
            
            # Clear operating expenditures
            result = await session.execute(delete(OperatingExpenditure))
            deleted_counts['operating_expenditures'] = result.rowcount
            print(f"Cleared {result.rowcount} operating expenditures")
            
            # Clear candidate summaries
            result = await session.execute(delete(CandidateSummary))
            deleted_counts['candidate_summaries'] = result.rowcount
            print(f"Cleared {result.rowcount} candidate summaries")
            
            # Clear committee summaries
            result = await session.execute(delete(CommitteeSummary))
            deleted_counts['committee_summaries'] = result.rowcount
            print(f"Cleared {result.rowcount} committee summaries")
            
            # Clear electioneering communications
            result = await session.execute(delete(ElectioneeringComm))
            deleted_counts['electioneering_comm'] = result.rowcount
            print(f"Cleared {result.rowcount} electioneering communications")
            
            # Clear communication costs
            result = await session.execute(delete(CommunicationCost))
            deleted_counts['communication_costs'] = result.rowcount
            print(f"Cleared {result.rowcount} communication costs")
            
            # Clear committees (after clearing dependent data)
            result = await session.execute(delete(Committee))
            deleted_counts['committees'] = result.rowcount
            print(f"Cleared {result.rowcount} committees")
            
            # Clear candidates (after clearing dependent data)
            result = await session.execute(delete(Candidate))
            deleted_counts['candidates'] = result.rowcount
            print(f"Cleared {result.rowcount} candidates")
            
            # Clear bulk data metadata
            result = await session.execute(delete(BulkDataMetadata))
            deleted_counts['bulk_data_metadata'] = result.rowcount
            print(f"Cleared {result.rowcount} bulk data metadata records")
            
            # Clear import jobs
            result = await session.execute(delete(BulkImportJob))
            deleted_counts['import_jobs'] = result.rowcount
            print(f"Cleared {result.rowcount} import jobs")
            
            await session.commit()
            
            total_deleted = sum(deleted_counts.values())
            print(f"\nSuccessfully cleared all data from database!")
            print(f"Total records deleted: {total_deleted}")
            print("\nBreakdown:")
            for table, count in deleted_counts.items():
                print(f"  {table}: {count:,}")
            
            return deleted_counts
            
        except Exception as e:
            await session.rollback()
            print(f"Error clearing database: {e}")
            raise

if __name__ == "__main__":
    asyncio.run(clear_all_data())

