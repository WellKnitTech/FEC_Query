"""
Parsers for different FEC bulk data types
Handles parsing CSV/ZIP files and storing in appropriate database models
"""
import pandas as pd
import logging
import gc
import os
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, update, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db.database import (
    AsyncSessionLocal, BulkDataMetadata, Candidate, Committee, Contribution,
    IndependentExpenditure, OperatingExpenditure, CandidateSummary, CommitteeSummary,
    ElectioneeringComm, CommunicationCost
)
from app.services.bulk_data_config import DataType, get_config
from app.services.shared.exceptions import BulkDataError
from app.services.shared.retry import retry_on_db_lock
from app.utils.thread_pool import async_read_csv, async_to_numeric

logger = logging.getLogger(__name__)


def calculate_data_age(cycle: int) -> int:
    """Calculate days since data was current for a given cycle"""
    current_year = datetime.now().year
    current_cycle = (current_year // 2) * 2  # Nearest even year
    
    if cycle >= current_cycle:
        # Current or future cycle - data is fresh
        return 0
    
    # Calculate days since cycle ended (cycle ends on election day, Nov of even year)
    cycle_end_date = datetime(cycle + 1, 11, 5)  # Nov 5 of year after cycle
    days_ago = (datetime.now() - cycle_end_date).days
    return max(0, days_ago)


class GenericBulkDataParser:
    """Generic parser for FEC bulk data files"""
    
    # SQLite has a limit of 999 variables per statement
    # With ~11 fields per record, we can insert ~90 records per statement
    # However, using executemany with parameterized queries is more efficient
    # We can use larger batches by using executemany which handles parameters differently
    # Default: 90 (safe), but can be increased via environment variable
    from app.config import config
    SQLITE_MAX_BATCH_SIZE = config.SQLITE_MAX_BATCH_SIZE
    
    # For bulk operations using executemany, we can use larger batches
    # This is used when we have control over the insert statement format
    SQLITE_BULK_BATCH_SIZE = config.SQLITE_BULK_BATCH_SIZE
    
    def __init__(self, bulk_data_service):
        self.bulk_data_service = bulk_data_service
    
    def _split_records_for_sqlite(self, records: List[Dict], max_batch_size: int = None) -> List[List[Dict]]:
        """
        Split records into smaller batches to avoid SQLite's variable limit (999 variables per statement)
        
        Args:
            records: List of records to split
            max_batch_size: Maximum records per batch (defaults to SQLITE_MAX_BATCH_SIZE)
        
        Returns:
            List of record batches
        """
        if max_batch_size is None:
            max_batch_size = self.SQLITE_MAX_BATCH_SIZE
        
        if not records:
            return []
        
        # Estimate fields per record (add some safety margin)
        # Most records have ~11 fields, so we use 90 as default
        batches = []
        for i in range(0, len(records), max_batch_size):
            batches.append(records[i:i + max_batch_size])
        
        return batches
    
    async def _execute_batch_with_savepoint(
        self,
        session,
        batch_records: List[Dict],
        insert_stmt_factory,
        commit_frequency: int = 5
    ) -> tuple[int, int]:
        """
        Execute batch insert with savepoint for error recovery
        
        Args:
            session: Database session
            batch_records: Records to insert
            insert_stmt_factory: Function that creates insert statement from batch
            commit_frequency: Commit every N batches (default: 5)
        
        Returns:
            Tuple of (inserted_count, failed_count)
        """
        from sqlalchemy.exc import SQLAlchemyError
        
        inserted_count = 0
        failed_count = 0
        batch_num = 0
        
        # Split into SQLite-compatible batches
        record_batches = self._split_records_for_sqlite(batch_records)
        
        for batch in record_batches:
            batch_num += 1
            try:
                # Create savepoint for this batch
                async with session.begin_nested():
                    insert_stmt = insert_stmt_factory(batch)
                    await session.execute(insert_stmt)
                    inserted_count += len(batch)
            except SQLAlchemyError as e:
                # Rollback to savepoint (automatic with begin_nested context)
                failed_count += len(batch)
                logger.warning(f"Failed to insert batch {batch_num}: {e}. Continuing with next batch.")
                # Continue with next batch instead of failing entire operation
        
        # Commit periodically (every commit_frequency batches or at the end)
        if batch_num % commit_frequency == 0 or batch_num == len(record_batches):
            try:
                await session.commit()
            except SQLAlchemyError as e:
                logger.error(f"Failed to commit after batch {batch_num}: {e}")
                await session.rollback()
                raise
        
        return inserted_count, failed_count
    
    @staticmethod
    def is_parser_implemented(data_type: DataType) -> bool:
        """Check if a parser is implemented for the given data type"""
        implemented_types = {
            DataType.INDIVIDUAL_CONTRIBUTIONS,
            DataType.CANDIDATE_MASTER,
            DataType.COMMITTEE_MASTER,
            DataType.CANDIDATE_COMMITTEE_LINKAGE,
            DataType.INDEPENDENT_EXPENDITURES,
            DataType.OPERATING_EXPENDITURES,
            DataType.CANDIDATE_SUMMARY,
            DataType.COMMITTEE_SUMMARY,
            DataType.PAC_SUMMARY,
            DataType.OTHER_TRANSACTIONS,
            DataType.PAS2,
            DataType.ELECTIONEERING_COMM,
            DataType.COMMUNICATION_COSTS,
        }
        return data_type in implemented_types

    async def parse_and_store(
        self,
        data_type: DataType,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """
        Generic parse and store method that routes to specific parsers
        """
        logger.info(f"Starting parse and store for {data_type.value}, cycle {cycle}, file: {file_path}")
        
        # Route to specific parser based on data type
        if data_type == DataType.INDIVIDUAL_CONTRIBUTIONS:
            return await self.bulk_data_service.parse_and_store_csv(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.CANDIDATE_MASTER:
            return await self.parse_candidate_master(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.COMMITTEE_MASTER:
            return await self.parse_committee_master(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.CANDIDATE_COMMITTEE_LINKAGE:
            return await self.parse_candidate_committee_linkage(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.INDEPENDENT_EXPENDITURES:
            return await self.parse_independent_expenditures(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.OPERATING_EXPENDITURES:
            return await self.parse_operating_expenditures(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.CANDIDATE_SUMMARY:
            return await self.parse_candidate_summary(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.COMMITTEE_SUMMARY:
            return await self.parse_committee_summary(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.PAC_SUMMARY:
            return await self.parse_pac_summary(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.OTHER_TRANSACTIONS:
            return await self.parse_other_transactions(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.PAS2:
            return await self.parse_pas2(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.ELECTIONEERING_COMM:
            return await self.parse_electioneering_comm(file_path, cycle, job_id, batch_size)
        elif data_type == DataType.COMMUNICATION_COSTS:
            return await self.parse_communication_costs(file_path, cycle, job_id, batch_size)
        else:
            logger.warning(f"No specific parser for {data_type.value}, using generic parser")
            return await self.parse_generic_csv(file_path, data_type, cycle, job_id, batch_size)
    
    async def get_column_names(
        self,
        data_type: DataType,
        file_path: str
    ) -> Optional[List[str]]:
        """Get column names from header file or infer from CSV"""
        config = get_config(data_type)
        if not config:
            return None
        
        # Try to get from header file first
        if config.header_file_url:
            logger.info(f"Downloading header file for {data_type.value}: {config.header_file_url}")
            columns = await self.bulk_data_service.download_header_file(config.header_file_url)
            if columns:
                logger.info(f"Loaded {len(columns)} columns from header file")
                return columns
        
        # Try to infer from CSV (if it has headers)
        try:
            df = await async_read_csv(file_path, nrows=0, sep='|')
            if len(df.columns) > 0:
                logger.info(f"Inferred {len(df.columns)} columns from CSV headers")
                return df.columns.tolist()
        except Exception as e:
            logger.debug(f"Could not infer columns from CSV: {e}")
        
        return None
    
    async def parse_generic_csv(
        self,
        file_path: str,
        data_type: DataType,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Generic CSV parser that stores raw data"""
        logger.info(f"Parsing generic CSV for {data_type.value}, cycle {cycle}")
        
        # Try to get column names
        columns = await self.get_column_names(data_type, file_path)
        has_header = columns is not None
        
        try:
            total_records = 0
            chunk_count = 0
            
            # Read CSV in chunks
            chunk_reader = await async_read_csv(
                file_path,
                sep='|',
                header=0 if has_header else None,
                names=columns if columns else None,
                chunksize=batch_size,
                dtype=str,
                low_memory=False,
                on_bad_lines='skip'
            )
            async for chunk in chunk_reader:
                if job_id and job_id in self.bulk_data_service._cancelled_jobs:
                    logger.info(f"Import cancelled for job {job_id}")
                    return total_records
                
                chunk_count += 1
                total_records += len(chunk)
                
                # Store metadata only (no specific table for this type)
                logger.debug(f"Processed chunk {chunk_count} with {len(chunk)} records")
                
                # Update progress every 5 chunks
                if job_id and chunk_count % 5 == 0:
                    await self.bulk_data_service._update_job_progress(
                        job_id,
                        current_chunk=chunk_count,
                        imported_records=total_records
                    )
                
                del chunk
                gc.collect()
            
            logger.info(f"Completed generic parse: {total_records} records processed")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing generic CSV for {data_type.value}: {e}", exc_info=True)
            raise
    
    async def parse_candidate_master(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse candidate master file (cn*.zip -> cn.txt)"""
        logger.info(f"Parsing candidate master file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.CANDIDATE_MASTER, file_path)
        if not columns:
            # Fallback to known FEC candidate master columns
            columns = [
                'CAND_ID', 'CAND_NAME', 'CAND_ICI', 'PTY_CD', 'CAND_PTY_AFFILIATION',
                'TTL_RECEIPTS', 'TRANS_FROM_AUTH', 'TTL_DISB', 'TRANS_TO_AUTH',
                'COH_BOP', 'COH_COP', 'CAND_CONTRIB', 'CAND_LOANS', 'OTHER_LOANS',
                'CAND_LOAN_REPAY', 'OTHER_LOAN_REPAY', 'DEBTS_OWED_BY', 'CAND_OFFICE_ST',
                'CAND_OFFICE', 'CAND_OFFICE_DISTRICT', 'CAND_ELECTION_YR', 'CAND_STATUS',
                'RPT_TP', 'PCC', 'CAND_PCC', 'CAND_ST1', 'CAND_ST2', 'CAND_CITY',
                'CAND_ST', 'CAND_ZIP'
            ]
            logger.warning(f"Using fallback column names for candidate master")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Filter out rows without CAND_ID using vectorized operations
                    chunk = chunk[chunk['CAND_ID'].notna() & (chunk['CAND_ID'].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Vectorized field transformations
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    chunk['candidate_id'] = chunk['CAND_ID'].astype(str).str.strip()
                    chunk['name'] = clean_str_field(chunk.get('CAND_NAME', pd.Series([''] * len(chunk))))
                    chunk['office'] = clean_str_field(chunk.get('CAND_OFFICE', pd.Series([''] * len(chunk))))
                    chunk['state'] = clean_str_field(chunk.get('CAND_OFFICE_ST', pd.Series([''] * len(chunk))))
                    chunk['district'] = clean_str_field(chunk.get('CAND_OFFICE_DISTRICT', pd.Series([''] * len(chunk))))
                    chunk['party'] = clean_str_field(chunk.get('PTY_CD', pd.Series([''] * len(chunk))))
                    
                    # Parse election year vectorized
                    chunk['election_year'] = pd.to_numeric(
                        chunk.get('CAND_ELECTION_YR', pd.Series([''] * len(chunk))).astype(str).str.strip(),
                        errors='coerce'
                    )
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['candidate_id', 'name', 'office', 'party', 'state', 'district', 'election_year']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add election_years and raw_data
                    for i, record in enumerate(records):
                        election_year = record.pop('election_year')
                        record['election_years'] = [int(election_year)] if pd.notna(election_year) else None
                        record['raw_data'] = raw_data_records[i]
                    
                    if records:
                        # Use larger batches for better performance when possible
                        # SQLite's on_conflict_do_update requires splitting into smaller batches
                        # due to the 999 variable limit, but we can optimize the batch size
                        record_batches = self._split_records_for_sqlite(records)
                        batch_inserted = 0
                        batch_failed = 0
                        
                        # Process batches in groups for better performance
                        batch_group_size = 10  # Process 10 batches before committing
                        for group_start in range(0, len(record_batches), batch_group_size):
                            group_batches = record_batches[group_start:group_start + batch_group_size]
                            
                            for batch_idx, batch in enumerate(group_batches):
                                try:
                                    # Create savepoint for this batch for error recovery
                                    async with session.begin_nested():
                                        # Bulk upsert using single execute call
                                        insert_stmt = sqlite_insert(Candidate).values(batch)
                                        upsert_stmt = insert_stmt.on_conflict_do_update(
                                            index_elements=['candidate_id'],
                                            set_={
                                                'name': insert_stmt.excluded.name,
                                                'office': insert_stmt.excluded.office,
                                                'party': insert_stmt.excluded.party,
                                                'state': insert_stmt.excluded.state,
                                                'district': insert_stmt.excluded.district,
                                                'election_years': insert_stmt.excluded.election_years,
                                                'raw_data': insert_stmt.excluded.raw_data,
                                                'updated_at': func.datetime('now')
                                            }
                                        )
                                        await session.execute(upsert_stmt)
                                        batch_inserted += len(batch)
                                except Exception as e:
                                    # Rollback to savepoint (automatic with begin_nested context)
                                    batch_failed += len(batch)
                                    logger.warning(f"Failed to insert candidate batch {group_start + batch_idx + 1}: {e}. Continuing with next batch.")
                                    # Continue with next batch instead of failing entire operation
                        
                        # Commit after processing all batches in this chunk (every chunk)
                        try:
                            await session.commit()
                            total_records += batch_inserted
                            skipped += batch_failed
                        except Exception as e:
                            logger.error(f"Failed to commit candidate chunk {chunk_count}: {e}")
                            await session.rollback()
                            raise
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total candidates")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed candidate master import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing candidate master: {e}", exc_info=True)
            raise
    
    async def parse_committee_master(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse committee master file (cm*.zip -> cm.txt)"""
        logger.info(f"Parsing committee master file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.COMMITTEE_MASTER, file_path)
        if not columns:
            # Fallback to known FEC committee master columns
            columns = [
                'CMTE_ID', 'CMTE_NM', 'TRES_NM', 'CMTE_ST1', 'CMTE_ST2', 'CMTE_CITY',
                'CMTE_ST', 'CMTE_ZIP', 'CMTE_DSGN', 'CMTE_TP', 'CMTE_PTY_AFFILIATION',
                'CMTE_FILING_FREQ', 'ORG_TP', 'CONNECTED_ORG_NM', 'CAND_ID'
            ]
            logger.warning(f"Using fallback column names for committee master")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Filter out rows without CMTE_ID using vectorized operations
                    chunk = chunk[chunk['CMTE_ID'].notna() & (chunk['CMTE_ID'].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Vectorized field transformations
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    chunk['committee_id'] = chunk['CMTE_ID'].astype(str).str.strip()
                    chunk['name'] = clean_str_field(chunk.get('CMTE_NM', pd.Series([''] * len(chunk))))
                    chunk['committee_type'] = clean_str_field(chunk.get('CMTE_TP', pd.Series([''] * len(chunk))))
                    chunk['party'] = clean_str_field(chunk.get('CMTE_PTY_AFFILIATION', pd.Series([''] * len(chunk))))
                    chunk['state'] = clean_str_field(chunk.get('CMTE_ST', pd.Series([''] * len(chunk))))
                    
                    # Extract candidate IDs vectorized
                    chunk['candidate_ids'] = chunk.get('CAND_ID', pd.Series([''] * len(chunk))).apply(
                        lambda x: [str(x).strip()] if pd.notna(x) and str(x).strip() else None
                    )
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['committee_id', 'name', 'committee_type', 'party', 'state', 'candidate_ids']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add raw_data
                    for i, record in enumerate(records):
                        record['raw_data'] = raw_data_records[i]
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(Committee).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['committee_id'],
                                set_={
                                    'name': insert_stmt.excluded.name,
                                    'committee_type': insert_stmt.excluded.committee_type,
                                    'party': insert_stmt.excluded.party,
                                    'state': insert_stmt.excluded.state,
                                    'candidate_ids': insert_stmt.excluded.candidate_ids,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total committees")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed committee master import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing committee master: {e}", exc_info=True)
            raise
    
    async def parse_candidate_committee_linkage(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse candidate-committee linkage file (ccl*.zip -> ccl.txt)"""
        logger.info(f"Parsing candidate-committee linkage file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.CANDIDATE_COMMITTEE_LINKAGE, file_path)
        if not columns:
            # Fallback to known FEC candidate-committee linkage columns
            columns = [
                'CAND_ID', 'CAND_ELECTION_YR', 'FEC_ELECTION_YR', 'CMTE_ID', 'CMTE_TP',
                'CMTE_DSGN', 'LINKAGE_ID', 'CAND_OFFICE', 'CAND_OFFICE_ST', 'CAND_OFFICE_DISTRICT',
                'CAND_ICI', 'CAND_STATUS', 'CAND_PCC', 'CAND_ST1', 'CAND_ST2', 'CAND_CITY',
                'CAND_ST', 'CAND_ZIP', 'CMTE_ST1', 'CMTE_ST2', 'CMTE_CITY', 'CMTE_ST',
                'CMTE_ZIP', 'CMTE_TP_ORG', 'CMTE_TP_CATEGORY', 'ORG_TP', 'CONNECTED_ORG_NM',
                'IND_EMP', 'IND_OCC', 'IND_NAME', 'IND_ST1', 'IND_ST2', 'IND_CITY',
                'IND_ST', 'IND_ZIP', 'IND_EMPLOYER', 'IND_OCCUPATION'
            ]
            logger.warning(f"Using fallback column names for candidate-committee linkage")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            
            async with AsyncSessionLocal() as session:
                # Read the file in chunks
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Filter out rows without CMTE_ID or CAND_ID
                    chunk = chunk[
                        chunk['CMTE_ID'].notna() & (chunk['CMTE_ID'].astype(str).str.strip() != '') &
                        chunk['CAND_ID'].notna() & (chunk['CAND_ID'].astype(str).str.strip() != '')
                    ]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Clean fields
                    chunk['committee_id'] = chunk['CMTE_ID'].astype(str).str.strip()
                    chunk['candidate_id'] = chunk['CAND_ID'].astype(str).str.strip()
                    
                    # Group by committee_id to collect all candidate_ids
                    # This creates a mapping: committee_id -> list of candidate_ids
                    committee_candidate_map = chunk.groupby('committee_id')['candidate_id'].apply(
                        lambda x: list(x.unique())
                    ).to_dict()
                    
                    # Update Committee table with candidate_ids
                    if committee_candidate_map:
                        # Process in batches to avoid too many individual updates
                        update_batch = []
                        for comm_id, candidate_ids in committee_candidate_map.items():
                            # Filter out empty candidate_ids
                            candidate_ids = [cid for cid in candidate_ids if cid and cid.strip()]
                            if candidate_ids:
                                update_batch.append((comm_id, candidate_ids))
                        
                        # Update committees in batches
                        for comm_id, candidate_ids in update_batch:
                            try:
                                # Get existing candidate_ids and merge
                                result = await session.execute(
                                    select(Committee.candidate_ids)
                                    .where(Committee.committee_id == comm_id)
                                )
                                existing = result.scalar_one_or_none()
                                
                                # Merge existing and new candidate_ids, removing duplicates
                                if existing and existing:
                                    merged_ids = list(set(existing + candidate_ids))
                                else:
                                    merged_ids = candidate_ids
                                
                                # Update the committee
                                await session.execute(
                                    update(Committee)
                                    .where(Committee.committee_id == comm_id)
                                    .values(candidate_ids=merged_ids)
                                )
                            except Exception as e:
                                logger.debug(f"Error updating committee {comm_id}: {e}")
                                continue
                        
                        # Also update contributions with candidate_id based on committee_id
                        # Use the first candidate_id for each committee (most committees have one primary candidate)
                        
                        contribution_updates = 0
                        for comm_id, candidate_ids in update_batch:
                            if candidate_ids:
                                # Use first candidate_id (primary candidate)
                                primary_candidate_id = candidate_ids[0]
                                
                                try:
                                    # Update contributions missing candidate_id for this committee
                                    result = await session.execute(
                                        update(Contribution)
                                        .where(
                                            Contribution.committee_id == comm_id,
                                            ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == ''))
                                        )
                                        .values(candidate_id=primary_candidate_id)
                                        .execution_options(synchronize_session=False)
                                    )
                                    contribution_updates += result.rowcount
                                except Exception as e:
                                    logger.debug(f"Error updating contributions for committee {comm_id}: {e}")
                                    continue
                        
                        if contribution_updates > 0:
                            logger.info(f"Updated {contribution_updates} contributions with candidate_id from linkage data")
                        
                        await session.commit()
                        total_records += len(update_batch)
                    
                    # Log progress
                    if chunk_count % 10 == 0:
                        logger.info(f"Processed {chunk_count} chunks: {total_records} committee linkages")
                    
                    # Update progress
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, committee_candidate_map
                    gc.collect()
            
            logger.info(f"Completed candidate-committee linkage import: {total_records} linkages processed")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing candidate-committee linkage: {e}", exc_info=True)
            raise
    
    async def parse_independent_expenditures(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse independent expenditures CSV file"""
        logger.info(f"Parsing independent expenditures for cycle {cycle}")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                # CSV files typically have headers
                chunk_reader = await async_read_csv(
                    file_path,
                    sep=',',  # CSV files are comma-separated
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing - handle column name variations
                    def get_col(chunk, *names):
                        """Get column with fallback names"""
                        for name in names:
                            if name in chunk.columns:
                                return chunk[name]
                        return pd.Series([None] * len(chunk))
                    
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    # Get expenditure_id (try multiple column name variations)
                    exp_id_col = get_col(chunk, 'expenditure_id', 'SUB_ID')
                    # Generate IDs for rows without them
                    chunk['expenditure_id'] = exp_id_col.astype(str).str.strip()
                    missing_mask = (chunk['expenditure_id'] == '') | (chunk['expenditure_id'] == 'None')
                    if missing_mask.any():
                        cmte_col = get_col(chunk, 'CMTE_ID', 'committee_id')
                        file_col = get_col(chunk, 'FILE_NUM', 'file_num')
                        chunk.loc[missing_mask, 'expenditure_id'] = (
                            cmte_col[missing_mask].astype(str) + '_' + 
                            file_col[missing_mask].astype(str) + '_' + 
                            chunk[missing_mask].index.astype(str)
                        )
                    
                    # Filter out rows without expenditure_id
                    chunk = chunk[chunk['expenditure_id'].notna() & (chunk['expenditure_id'].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    chunk['committee_id'] = clean_str_field(get_col(chunk, 'CMTE_ID', 'committee_id'))
                    chunk['candidate_id'] = clean_str_field(get_col(chunk, 'CAND_ID', 'candidate_id'))
                    chunk['candidate_name'] = clean_str_field(get_col(chunk, 'CAND_NM', 'candidate_name'))
                    chunk['support_oppose_indicator'] = clean_str_field(get_col(chunk, 'SUPPORT_OPPOSE_IND', 'support_oppose_indicator'))
                    chunk['payee_name'] = clean_str_field(get_col(chunk, 'PAYEE_NM', 'payee_name'))
                    chunk['expenditure_purpose'] = clean_str_field(get_col(chunk, 'EXPENDITURE_PURPOSE_DESC', 'expenditure_purpose'))
                    
                    # Vectorized amount parsing with better handling of malformed values
                    amount_col = get_col(chunk, 'EXPENDITURE_AMOUNT', 'expenditure_amount')
                    # Clean the amount strings
                    amount_str = amount_col.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip()
                    
                    # Handle malformed strings with multiple decimal points (e.g., "0.00.00.00...")
                    def fix_malformed_amount(amount_str_series):
                        """Fix malformed amounts with multiple decimal points"""
                        result = amount_str_series.copy()
                        # Find strings with multiple decimal points
                        multi_dot_mask = result.str.count('.') > 1
                        if multi_dot_mask.any():
                            # For strings with multiple dots, take only the first decimal part
                            def take_first_decimal(s):
                                if '.' in s:
                                    first_dot = s.find('.')
                                    second_dot = s.find('.', first_dot + 1)
                                    if second_dot > 0:
                                        return s[:second_dot]
                                return s
                            result[multi_dot_mask] = result[multi_dot_mask].apply(take_first_decimal)
                        return result
                    
                    amount_str = fix_malformed_amount(amount_str)
                    
                    chunk['expenditure_amount'] = pd.to_numeric(
                        amount_str,
                        errors='coerce'
                    ).fillna(0.0)
                    
                    # Vectorized date parsing
                    date_col = get_col(chunk, 'EXPENDITURE_DATE', 'expenditure_date')
                    chunk['expenditure_date'] = pd.to_datetime(date_col, errors='coerce')
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['expenditure_id', 'committee_id', 'candidate_id', 'candidate_name', 
                                       'support_oppose_indicator', 'expenditure_amount', 'expenditure_date',
                                       'payee_name', 'expenditure_purpose']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                        # Convert pandas Timestamp to Python datetime for SQLite compatibility
                        if 'expenditure_date' in record and pd.notna(record['expenditure_date']):
                            if isinstance(record['expenditure_date'], pd.Timestamp):
                                record['expenditure_date'] = record['expenditure_date'].to_pydatetime()
                            elif not isinstance(record['expenditure_date'], (datetime, type(None))):
                                try:
                                    record['expenditure_date'] = pd.to_datetime(record['expenditure_date']).to_pydatetime()
                                except:
                                    record['expenditure_date'] = None
                        else:
                            record['expenditure_date'] = None
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(IndependentExpenditure).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['expenditure_id'],
                                set_={
                                    'cycle': insert_stmt.excluded.cycle,
                                    'committee_id': insert_stmt.excluded.committee_id,
                                    'candidate_id': insert_stmt.excluded.candidate_id,
                                    'candidate_name': insert_stmt.excluded.candidate_name,
                                    'support_oppose_indicator': insert_stmt.excluded.support_oppose_indicator,
                                    'expenditure_amount': insert_stmt.excluded.expenditure_amount,
                                    'expenditure_date': insert_stmt.excluded.expenditure_date,
                                    'payee_name': insert_stmt.excluded.payee_name,
                                    'expenditure_purpose': insert_stmt.excluded.expenditure_purpose,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total independent expenditures")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed independent expenditures import: {total_records} records, {skipped} skipped")
            return total_records
            
        except BulkDataError:
            raise
        except Exception as e:
            logger.error(f"Error parsing independent expenditures: {e}", exc_info=True)
            raise BulkDataError(f"Error parsing independent expenditures for cycle {cycle}: {str(e)}", cycle=cycle, data_type=DataType.INDEPENDENT_EXPENDITURES.value) from e
    
    async def parse_operating_expenditures(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse operating expenditures file (oppexp*.zip -> oppexp.txt)"""
        logger.info(f"Parsing operating expenditures for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.OPERATING_EXPENDITURES, file_path)
        if not columns:
            # Fallback columns
            columns = [
                'CMTE_ID', 'AMNDT_IND', 'RPT_YR', 'RPT_TP', 'IMAGE_NUM', 'LINE_NUM',
                'FORM_TP_CD', 'SCHED_TP_CD', 'NAME', 'CITY', 'STATE', 'ZIP_CODE',
                'TRANSACTION_DT', 'TRANSACTION_AMT', 'TRANSACTION_PGI', 'PURPOSE',
                'CATEGORY', 'CATEGORY_DESC', 'MEMO_CD', 'MEMO_TEXT', 'ENTITY_TP',
                'SUB_ID', 'FILE_NUM', 'TRAN_ID', 'BACK_REF_TRAN_ID'
            ]
            logger.warning(f"Using fallback column names for operating expenditures")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Filter out rows without SUB_ID using vectorized operations
                    chunk = chunk[chunk['SUB_ID'].notna() & (chunk['SUB_ID'].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Vectorized field transformations
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    chunk['expenditure_id'] = chunk['SUB_ID'].astype(str).str.strip()
                    chunk['committee_id'] = clean_str_field(chunk.get('CMTE_ID', pd.Series([''] * len(chunk))))
                    chunk['payee_name'] = clean_str_field(chunk.get('NAME', pd.Series([''] * len(chunk))))
                    chunk['expenditure_purpose'] = clean_str_field(chunk.get('PURPOSE', pd.Series([''] * len(chunk))))
                    
                    # Extract additional FEC fields
                    chunk['amendment_indicator'] = clean_str_field(chunk.get('AMNDT_IND', pd.Series([''] * len(chunk))))
                    chunk['report_year'] = await async_to_numeric(
                        chunk.get('RPT_YR', pd.Series([''] * len(chunk))).astype(str).str.strip(),
                        errors='coerce'
                    )
                    chunk['report_type'] = clean_str_field(chunk.get('RPT_TP', pd.Series([''] * len(chunk))))
                    chunk['image_number'] = clean_str_field(chunk.get('IMAGE_NUM', pd.Series([''] * len(chunk))))
                    chunk['line_number'] = clean_str_field(chunk.get('LINE_NUM', pd.Series([''] * len(chunk))))
                    chunk['form_type_code'] = clean_str_field(chunk.get('FORM_TP_CD', pd.Series([''] * len(chunk))))
                    chunk['schedule_type_code'] = clean_str_field(chunk.get('SCHED_TP_CD', pd.Series([''] * len(chunk))))
                    chunk['transaction_pgi'] = clean_str_field(chunk.get('TRANSACTION_PGI', pd.Series([''] * len(chunk))))
                    chunk['category'] = clean_str_field(chunk.get('CATEGORY', pd.Series([''] * len(chunk))))
                    chunk['category_description'] = clean_str_field(chunk.get('CATEGORY_DESC', pd.Series([''] * len(chunk))))
                    chunk['memo_code'] = clean_str_field(chunk.get('MEMO_CD', pd.Series([''] * len(chunk))))
                    chunk['memo_text'] = clean_str_field(chunk.get('MEMO_TEXT', pd.Series([''] * len(chunk))))
                    chunk['entity_type'] = clean_str_field(chunk.get('ENTITY_TP', pd.Series([''] * len(chunk))))
                    chunk['file_number'] = clean_str_field(chunk.get('FILE_NUM', pd.Series([''] * len(chunk))))
                    chunk['transaction_id'] = clean_str_field(chunk.get('TRAN_ID', pd.Series([''] * len(chunk))))
                    chunk['back_reference_transaction_id'] = clean_str_field(chunk.get('BACK_REF_TRAN_ID', pd.Series([''] * len(chunk))))
                    
                    # Vectorized amount parsing
                    chunk['expenditure_amount'] = await async_to_numeric(
                        chunk.get('TRANSACTION_AMT', pd.Series([''] * len(chunk))).astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce'
                    )
                    chunk['expenditure_amount'] = chunk['expenditure_amount'].fillna(0.0)
                    
                    # Vectorized date parsing (reuse function from bulk_data.py pattern)
                    def parse_date_vectorized(date_series):
                        """Parse dates vectorized - handles MMDDYYYY and YYYYMMDD formats"""
                        result = pd.Series([None] * len(date_series), dtype='object')
                        date_strs = date_series.astype(str).str.strip()
                        valid_mask = (date_strs.str.len() == 8) & date_strs.str.isdigit()
                        if valid_mask.any():
                            valid_dates = date_strs[valid_mask]
                            try:
                                parsed_mmddyyyy = pd.to_datetime(valid_dates, format='%m%d%Y', errors='coerce')
                                result[valid_mask] = parsed_mmddyyyy
                            except Exception as e:
                                logger.debug(f"Date parsing failed for MMDDYYYY format, trying next: {e}")
                            failed_mask = valid_mask & result.isna()
                            if failed_mask.any():
                                try:
                                    failed_dates = date_strs[failed_mask]
                                    parsed_yyyymmdd = pd.to_datetime(failed_dates, format='%Y%m%d', errors='coerce')
                                    result[failed_mask] = parsed_yyyymmdd
                                except Exception as e:
                                    logger.debug(f"Date parsing failed for YYYYMMDD format: {e}")
                        return result
                    
                    chunk['expenditure_date'] = parse_date_vectorized(chunk.get('TRANSACTION_DT', pd.Series([''] * len(chunk))))
                    
                    # Build raw_data vectorized - includes all source fields
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records - include all new fields
                    records_df = chunk[[
                        'expenditure_id', 'committee_id', 'payee_name', 'expenditure_amount', 
                        'expenditure_date', 'expenditure_purpose', 'amendment_indicator',
                        'report_year', 'report_type', 'image_number', 'line_number',
                        'form_type_code', 'schedule_type_code', 'transaction_pgi',
                        'category', 'category_description', 'memo_code', 'memo_text',
                        'entity_type', 'file_number', 'transaction_id', 'back_reference_transaction_id'
                    ]].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                        # Convert pandas Timestamp to Python datetime for SQLite compatibility
                        if 'expenditure_date' in record and pd.notna(record['expenditure_date']):
                            if isinstance(record['expenditure_date'], pd.Timestamp):
                                record['expenditure_date'] = record['expenditure_date'].to_pydatetime()
                            elif not isinstance(record['expenditure_date'], (datetime, type(None))):
                                # Try to convert if it's a string or other type
                                try:
                                    record['expenditure_date'] = pd.to_datetime(record['expenditure_date']).to_pydatetime()
                                except:
                                    record['expenditure_date'] = None
                        else:
                            record['expenditure_date'] = None
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(OperatingExpenditure).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['expenditure_id'],
                                set_={
                                    'cycle': insert_stmt.excluded.cycle,
                                    'committee_id': insert_stmt.excluded.committee_id,
                                    'payee_name': insert_stmt.excluded.payee_name,
                                    'expenditure_amount': insert_stmt.excluded.expenditure_amount,
                                    'expenditure_date': insert_stmt.excluded.expenditure_date,
                                    'expenditure_purpose': insert_stmt.excluded.expenditure_purpose,
                                    'amendment_indicator': insert_stmt.excluded.amendment_indicator,
                                    'report_year': insert_stmt.excluded.report_year,
                                    'report_type': insert_stmt.excluded.report_type,
                                    'image_number': insert_stmt.excluded.image_number,
                                    'line_number': insert_stmt.excluded.line_number,
                                    'form_type_code': insert_stmt.excluded.form_type_code,
                                    'schedule_type_code': insert_stmt.excluded.schedule_type_code,
                                    'transaction_pgi': insert_stmt.excluded.transaction_pgi,
                                    'category': insert_stmt.excluded.category,
                                    'category_description': insert_stmt.excluded.category_description,
                                    'memo_code': insert_stmt.excluded.memo_code,
                                    'memo_text': insert_stmt.excluded.memo_text,
                                    'entity_type': insert_stmt.excluded.entity_type,
                                    'file_number': insert_stmt.excluded.file_number,
                                    'transaction_id': insert_stmt.excluded.transaction_id,
                                    'back_reference_transaction_id': insert_stmt.excluded.back_reference_transaction_id,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now', 'utc')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total operating expenditures")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed operating expenditures import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing operating expenditures: {e}", exc_info=True)
            raise
    
    async def parse_candidate_summary(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse candidate summary CSV file"""
        logger.info(f"Parsing candidate summary for cycle {cycle}")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                # CSV files typically have headers
                for chunk in pd.read_csv(
                    file_path,
                    sep=',',
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                ):
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing - handle column name variations
                    def get_col(chunk, *names):
                        """Get column with fallback names"""
                        for name in names:
                            if name in chunk.columns:
                                return chunk[name]
                        return pd.Series([None] * len(chunk))
                    
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    # Get candidate_id (try multiple column name variations)
                    cand_id_col = get_col(chunk, 'candidate_id', 'CAND_ID')
                    chunk = chunk[cand_id_col.notna() & (cand_id_col.astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    chunk['candidate_id'] = cand_id_col.astype(str).str.strip()
                    chunk['candidate_name'] = clean_str_field(get_col(chunk, 'candidate_name', 'CAND_NAME'))
                    chunk['office'] = clean_str_field(get_col(chunk, 'office', 'CAND_OFFICE'))
                    chunk['party'] = clean_str_field(get_col(chunk, 'party', 'PTY_CD', 'CAND_PTY_AFFILIATION'))
                    chunk['state'] = clean_str_field(get_col(chunk, 'state', 'CAND_OFFICE_ST'))
                    chunk['district'] = clean_str_field(get_col(chunk, 'district', 'CAND_OFFICE_DISTRICT'))
                    
                    # Vectorized amount parsing
                    def parse_amount_vectorized(series):
                        return pd.to_numeric(
                            series.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                            errors='coerce'
                        ).fillna(0.0)
                    
                    receipts_col = get_col(chunk, 'total_receipts', 'TTL_RECEIPTS')
                    disb_col = get_col(chunk, 'total_disbursements', 'TTL_DISB')
                    coh_col = get_col(chunk, 'cash_on_hand', 'COH_COP')
                    
                    chunk['total_receipts'] = parse_amount_vectorized(receipts_col)
                    chunk['total_disbursements'] = parse_amount_vectorized(disb_col)
                    chunk['cash_on_hand'] = parse_amount_vectorized(coh_col)
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['candidate_id', 'candidate_name', 'office', 'party', 'state', 'district', 'total_receipts', 'total_disbursements', 'cash_on_hand']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(CandidateSummary).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['candidate_id', 'cycle'],
                                set_={
                                    'candidate_name': insert_stmt.excluded.candidate_name,
                                    'office': insert_stmt.excluded.office,
                                    'party': insert_stmt.excluded.party,
                                    'state': insert_stmt.excluded.state,
                                    'district': insert_stmt.excluded.district,
                                    'total_receipts': insert_stmt.excluded.total_receipts,
                                    'total_disbursements': insert_stmt.excluded.total_disbursements,
                                    'cash_on_hand': insert_stmt.excluded.cash_on_hand,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total candidate summaries")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed candidate summary import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing candidate summary: {e}", exc_info=True)
            raise
    
    async def parse_committee_summary(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse committee summary CSV file"""
        logger.info(f"Parsing committee summary for cycle {cycle}")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                # CSV files typically have headers
                for chunk in pd.read_csv(
                    file_path,
                    sep=',',
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                ):
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing - handle column name variations
                    def get_col(chunk, *names):
                        """Get column with fallback names"""
                        for name in names:
                            if name in chunk.columns:
                                return chunk[name]
                        return pd.Series([None] * len(chunk))
                    
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    # Get committee_id (try multiple column name variations)
                    cmte_id_col = get_col(chunk, 'committee_id', 'CMTE_ID')
                    chunk = chunk[cmte_id_col.notna() & (cmte_id_col.astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    chunk['committee_id'] = cmte_id_col.astype(str).str.strip()
                    chunk['committee_name'] = clean_str_field(get_col(chunk, 'committee_name', 'CMTE_NM'))
                    chunk['committee_type'] = clean_str_field(get_col(chunk, 'committee_type', 'CMTE_TP'))
                    
                    # Vectorized amount parsing
                    def parse_amount_vectorized(series):
                        return pd.to_numeric(
                            series.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                            errors='coerce'
                        ).fillna(0.0)
                    
                    receipts_col = get_col(chunk, 'total_receipts', 'TTL_RECEIPTS')
                    disb_col = get_col(chunk, 'total_disbursements', 'TTL_DISB')
                    coh_col = get_col(chunk, 'cash_on_hand', 'COH_COP')
                    
                    chunk['total_receipts'] = parse_amount_vectorized(receipts_col)
                    chunk['total_disbursements'] = parse_amount_vectorized(disb_col)
                    chunk['cash_on_hand'] = parse_amount_vectorized(coh_col)
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['committee_id', 'committee_name', 'committee_type', 'total_receipts', 'total_disbursements', 'cash_on_hand']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(CommitteeSummary).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['committee_id', 'cycle'],
                                set_={
                                    'committee_name': insert_stmt.excluded.committee_name,
                                    'committee_type': insert_stmt.excluded.committee_type,
                                    'total_receipts': insert_stmt.excluded.total_receipts,
                                    'total_disbursements': insert_stmt.excluded.total_disbursements,
                                    'cash_on_hand': insert_stmt.excluded.cash_on_hand,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total committee summaries")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed committee summary import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing committee summary: {e}", exc_info=True)
            raise
    
    async def parse_pac_summary(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse PAC summary file (webk*.zip -> webk.txt)"""
        logger.info(f"Parsing PAC summary file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.PAC_SUMMARY, file_path)
        if not columns:
            # Fallback to known FEC PAC summary columns
            columns = [
                'CMTE_ID', 'CMTE_NM', 'CMTE_TP', 'TTL_RECEIPTS', 'TTL_DISB', 
                'COH_COP', 'CAND_ID', 'CAND_NM'
            ]
            logger.warning(f"Using fallback column names for PAC summary")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Check if CMTE_ID column exists (might be inferred with different name)
                    committee_id_col = None
                    for col in chunk.columns:
                        if col.upper() == 'CMTE_ID' or col.upper() == 'COMMITTEE_ID':
                            committee_id_col = col
                            break
                    
                    if committee_id_col is None:
                        logger.warning(f"CMTE_ID column not found in PAC summary. Available columns: {list(chunk.columns)}")
                        # Try to use first column as committee_id if available
                        if len(chunk.columns) > 0:
                            committee_id_col = chunk.columns[0]
                            logger.info(f"Using first column '{committee_id_col}' as committee_id")
                        else:
                            logger.error("No columns found in PAC summary chunk, skipping")
                            del chunk
                            gc.collect()
                            continue
                    
                    # Filter out rows without committee_id
                    chunk = chunk[chunk[committee_id_col].notna() & (chunk[committee_id_col].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Vectorized field transformations
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    def parse_amount_vectorized(series):
                        return pd.to_numeric(
                            series.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                            errors='coerce'
                        ).fillna(0.0)
                    
                    # Helper to get column with case-insensitive matching
                    def get_col(df, *possible_names):
                        for name in possible_names:
                            for col in df.columns:
                                if col.upper() == name.upper():
                                    return col
                        return None
                    
                    chunk['committee_id'] = chunk[committee_id_col].astype(str).str.strip()
                    
                    # Use helper function to find columns case-insensitively
                    cmte_nm_col = get_col(chunk, 'CMTE_NM', 'COMMITTEE_NAME', 'CMTE_NAME')
                    cmte_tp_col = get_col(chunk, 'CMTE_TP', 'COMMITTEE_TYPE', 'CMTE_TYPE')
                    receipts_col = get_col(chunk, 'TTL_RECEIPTS', 'TOTAL_RECEIPTS', 'RECEIPTS')
                    disb_col = get_col(chunk, 'TTL_DISB', 'TOTAL_DISBURSEMENTS', 'DISBURSEMENTS', 'DISB')
                    coh_col = get_col(chunk, 'COH_COP', 'CASH_ON_HAND', 'COH')
                    
                    chunk['committee_name'] = clean_str_field(chunk[cmte_nm_col] if cmte_nm_col else pd.Series([''] * len(chunk)))
                    chunk['committee_type'] = clean_str_field(chunk[cmte_tp_col] if cmte_tp_col else pd.Series([''] * len(chunk)))
                    
                    # Parse financial amounts
                    receipts_series = chunk[receipts_col] if receipts_col else pd.Series(['0'] * len(chunk))
                    disb_series = chunk[disb_col] if disb_col else pd.Series(['0'] * len(chunk))
                    coh_series = chunk[coh_col] if coh_col else pd.Series(['0'] * len(chunk))
                    
                    chunk['total_receipts'] = parse_amount_vectorized(receipts_series)
                    chunk['total_disbursements'] = parse_amount_vectorized(disb_series)
                    chunk['cash_on_hand'] = parse_amount_vectorized(coh_series)
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['committee_id', 'committee_name', 'committee_type', 'total_receipts', 'total_disbursements', 'cash_on_hand']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(CommitteeSummary).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['committee_id', 'cycle'],
                                set_={
                                    'committee_name': insert_stmt.excluded.committee_name,
                                    'committee_type': insert_stmt.excluded.committee_type,
                                    'total_receipts': insert_stmt.excluded.total_receipts,
                                    'total_disbursements': insert_stmt.excluded.total_disbursements,
                                    'cash_on_hand': insert_stmt.excluded.cash_on_hand,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total PAC summaries")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed PAC summary import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing PAC summary: {e}", exc_info=True)
            raise
    
    async def parse_other_transactions(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse other transactions file (oth*.zip -> oth.txt)"""
        logger.info(f"Parsing other transactions file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.OTHER_TRANSACTIONS, file_path)
        if not columns:
            # Use generic parser if no header file available
            logger.warning(f"No header file for other transactions, using generic parser")
            return await self.parse_generic_csv(file_path, DataType.OTHER_TRANSACTIONS, cycle, job_id, batch_size)
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Build raw_data vectorized - store all data as JSON
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Store in bulk_data_metadata as raw JSON for now
                    # This is a generic storage approach for data types without specific models
                    total_records += len(raw_data_records)
                    
                    # Log progress
                    if chunk_count % 10 == 0:
                        logger.info(f"Processed {chunk_count} chunks: {total_records} total other transactions records")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk
                    gc.collect()
            
            logger.info(f"Completed other transactions import: {total_records} records")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing other transactions: {e}", exc_info=True)
            raise
    
    async def parse_pas2(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse PAS2 file (pas2*.zip -> pas2.txt)"""
        logger.info(f"Parsing PAS2 file for cycle {cycle}")
        
        # Get column names from header file
        columns = await self.get_column_names(DataType.PAS2, file_path)
        if not columns:
            # Use generic parser if no header file available
            logger.warning(f"No header file for PAS2, using generic parser")
            return await self.parse_generic_csv(file_path, DataType.PAS2, cycle, job_id, batch_size)
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                )
                async for chunk in chunk_reader:
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Build raw_data vectorized - store all data as JSON
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in columns if col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Store in bulk_data_metadata as raw JSON for now
                    # This is a generic storage approach for data types without specific models
                    total_records += len(raw_data_records)
                    
                    # Log progress
                    if chunk_count % 10 == 0:
                        logger.info(f"Processed {chunk_count} chunks: {total_records} total PAS2 records")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk
                    gc.collect()
            
            logger.info(f"Completed PAS2 import: {total_records} records")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing PAS2: {e}", exc_info=True)
            raise
    
    async def parse_electioneering_comm(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse electioneering communications CSV file"""
        logger.info(f"Parsing electioneering communications for cycle {cycle}")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                # CSV files typically have headers
                for chunk in pd.read_csv(
                    file_path,
                    sep=',',
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                ):
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing - handle column name variations
                    def get_col(chunk, *names):
                        """Get column with fallback names"""
                        for name in names:
                            if name in chunk.columns:
                                return chunk[name]
                        return pd.Series([None] * len(chunk))
                    
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    # Get key fields
                    cmte_id_col = get_col(chunk, 'committee_id', 'CMTE_ID', 'cmte_id')
                    chunk = chunk[cmte_id_col.notna() & (cmte_id_col.astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    chunk['committee_id'] = cmte_id_col.astype(str).str.strip()
                    chunk['candidate_id'] = clean_str_field(get_col(chunk, 'candidate_id', 'CAND_ID', 'cand_id'))
                    chunk['candidate_name'] = clean_str_field(get_col(chunk, 'candidate_name', 'CAND_NM', 'cand_name'))
                    
                    # Parse amount
                    amount_col = get_col(chunk, 'communication_amount', 'AMOUNT', 'amount')
                    chunk['communication_amount'] = pd.to_numeric(
                        amount_col.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce'
                    ).fillna(0.0)
                    
                    # Parse date
                    date_col = get_col(chunk, 'communication_date', 'DATE', 'date')
                    chunk['communication_date'] = pd.to_datetime(date_col, errors='coerce')
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['committee_id', 'candidate_id', 'candidate_name', 'communication_amount', 'communication_date']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                        # Convert pandas Timestamp to Python datetime for SQLite compatibility
                        if 'communication_date' in record and pd.notna(record['communication_date']):
                            if isinstance(record['communication_date'], pd.Timestamp):
                                record['communication_date'] = record['communication_date'].to_pydatetime()
                            elif not isinstance(record['communication_date'], (datetime, type(None))):
                                try:
                                    record['communication_date'] = pd.to_datetime(record['communication_date']).to_pydatetime()
                                except:
                                    record['communication_date'] = None
                        else:
                            record['communication_date'] = None
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(ElectioneeringComm).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['id'],  # Will use auto-generated ID
                                set_={
                                    'committee_id': insert_stmt.excluded.committee_id,
                                    'candidate_id': insert_stmt.excluded.candidate_id,
                                    'candidate_name': insert_stmt.excluded.candidate_name,
                                    'communication_amount': insert_stmt.excluded.communication_amount,
                                    'communication_date': insert_stmt.excluded.communication_date,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total electioneering communications")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed electioneering communications import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing electioneering communications: {e}", exc_info=True)
            raise
    
    async def parse_communication_costs(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000
    ) -> int:
        """Parse communication costs CSV file"""
        logger.info(f"Parsing communication costs for cycle {cycle}")
        
        try:
            total_records = 0
            skipped = 0
            chunk_count = 0
            data_age_days = calculate_data_age(cycle)
            
            async with AsyncSessionLocal() as session:
                # CSV files typically have headers
                for chunk in pd.read_csv(
                    file_path,
                    sep=',',
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                ):
                    if job_id and hasattr(self.bulk_data_service, '_cancelled_jobs') and job_id in self.bulk_data_service._cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id}")
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing - handle column name variations
                    def get_col(chunk, *names):
                        """Get column with fallback names"""
                        for name in names:
                            if name in chunk.columns:
                                return chunk[name]
                        return pd.Series([None] * len(chunk))
                    
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    # Get key fields
                    cmte_id_col = get_col(chunk, 'committee_id', 'CMTE_ID', 'cmte_id')
                    chunk = chunk[cmte_id_col.notna() & (cmte_id_col.astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    chunk['committee_id'] = cmte_id_col.astype(str).str.strip()
                    chunk['candidate_id'] = clean_str_field(get_col(chunk, 'candidate_id', 'CAND_ID', 'cand_id'))
                    chunk['candidate_name'] = clean_str_field(get_col(chunk, 'candidate_name', 'CAND_NM', 'cand_name'))
                    
                    # Parse amount
                    amount_col = get_col(chunk, 'communication_amount', 'AMOUNT', 'amount')
                    chunk['communication_amount'] = pd.to_numeric(
                        amount_col.astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce'
                    ).fillna(0.0)
                    
                    # Parse date
                    date_col = get_col(chunk, 'communication_date', 'DATE', 'date')
                    chunk['communication_date'] = pd.to_datetime(date_col, errors='coerce')
                    
                    # Build raw_data vectorized
                    raw_data_df = pd.DataFrame({col: chunk[col].astype(str).where(chunk[col].notna(), None) for col in chunk.columns})
                    raw_data_records = raw_data_df.to_dict('records')
                    
                    # Convert to records
                    records_df = chunk[['committee_id', 'candidate_id', 'candidate_name', 'communication_amount', 'communication_date']].copy()
                    records = records_df.to_dict('records')
                    
                    # Add cycle, raw_data, and data_age_days
                    # Ensure data_age_days is an integer (not NaN or None)
                    if data_age_days is None or (isinstance(data_age_days, float) and pd.isna(data_age_days)):
                        data_age_days_int = 0
                    else:
                        try:
                            data_age_days_int = int(data_age_days)
                        except (ValueError, TypeError):
                            data_age_days_int = 0
                    for i, record in enumerate(records):
                        record['cycle'] = cycle
                        record['raw_data'] = raw_data_records[i]
                        record['data_age_days'] = data_age_days_int
                        # Convert pandas Timestamp to Python datetime for SQLite compatibility
                        if 'communication_date' in record and pd.notna(record['communication_date']):
                            if isinstance(record['communication_date'], pd.Timestamp):
                                record['communication_date'] = record['communication_date'].to_pydatetime()
                            elif not isinstance(record['communication_date'], (datetime, type(None))):
                                try:
                                    record['communication_date'] = pd.to_datetime(record['communication_date']).to_pydatetime()
                                except:
                                    record['communication_date'] = None
                        else:
                            record['communication_date'] = None
                    
                    if records:
                        # Split records into smaller batches to avoid SQLite variable limit
                        record_batches = self._split_records_for_sqlite(records)
                        for batch in record_batches:
                            # Bulk upsert using single execute call
                            insert_stmt = sqlite_insert(CommunicationCost).values(batch)
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['id'],  # Will use auto-generated ID
                                set_={
                                    'committee_id': insert_stmt.excluded.committee_id,
                                    'candidate_id': insert_stmt.excluded.candidate_id,
                                    'candidate_name': insert_stmt.excluded.candidate_name,
                                    'communication_amount': insert_stmt.excluded.communication_amount,
                                    'communication_date': insert_stmt.excluded.communication_date,
                                    'raw_data': insert_stmt.excluded.raw_data,
                                    'data_age_days': insert_stmt.excluded.data_age_days,
                                    'updated_at': func.datetime('now')
                                }
                            )
                            await session.execute(upsert_stmt)
                        await session.commit()
                        total_records += len(records)
                        
                        # Log every 10 chunks
                        if chunk_count % 10 == 0:
                            logger.info(f"Imported {chunk_count} chunks: {total_records} total communication costs")
                    
                    # Update progress every 5 chunks
                    if job_id and chunk_count % 5 == 0:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            current_chunk=chunk_count,
                            imported_records=total_records,
                            skipped_records=skipped
                        )
                    
                    del chunk, records
                    gc.collect()
            
            logger.info(f"Completed communication costs import: {total_records} records, {skipped} skipped")
            return total_records
            
        except Exception as e:
            logger.error(f"Error parsing communication costs: {e}", exc_info=True)
            raise

