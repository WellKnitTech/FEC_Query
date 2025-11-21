# Database Schema Documentation

This document describes the database schema for the FEC Campaign Finance Analysis Tool.

## Overview

The application uses SQLAlchemy ORM with async support. The database can be either SQLite (for development) or PostgreSQL (for production). All models are defined in `backend/app/db/database.py`.

## Database Recommendations

- **SQLite**: Recommended for development and small deployments. Provides good performance for single-user or low-concurrency scenarios. Uses WAL (Write-Ahead Logging) mode for better concurrency.
- **PostgreSQL**: Recommended for production deployments with high concurrency, large datasets, or multi-user access. Provides better performance, advanced features, and scalability.

## Tables

### APICache

Cache table for FEC API responses to reduce API calls and improve performance.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| cache_key | String | Unique cache key (indexed) |
| response_data | JSON | Cached API response data |
| created_at | DateTime | When the cache entry was created |
| expires_at | DateTime | When the cache entry expires |

**Indexes:**
- `idx_cache_key_expires`: Composite index on `cache_key` and `expires_at` for efficient cache lookups

---

### Contribution

Stored contribution data from FEC Schedule A filings.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| contribution_id | String | Unique contribution identifier (indexed) |
| candidate_id | String | Candidate ID (indexed) |
| committee_id | String | Committee ID (indexed) |
| contributor_name | String | Contributor name (indexed) |
| contributor_city | String | Contributor city |
| contributor_state | String | Contributor state (indexed) |
| contributor_zip | String | Contributor ZIP code |
| contributor_employer | String | Contributor employer |
| contributor_occupation | String | Contributor occupation |
| contribution_amount | Float | Contribution amount |
| contribution_date | DateTime | Date of contribution (indexed) |
| contribution_type | String | Type of contribution |
| amendment_indicator | String | Amendment indicator (AMNDT_IND) |
| report_type | String | Report type (RPT_TP, indexed) |
| transaction_id | String | Transaction ID (indexed) |
| entity_type | String | Entity type (ENTITY_TP, indexed) |
| other_id | String | Other ID |
| file_number | String | File number |
| memo_code | String | Memo code (MEMO_CD) |
| memo_text | Text | Memo text |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| data_source | String | Source: 'bulk', 'api', or 'both' |
| last_updated_from | String | Last source that updated this record |

**Indexes:**
- `idx_contributor_name`: Index on contributor name for search
- `idx_contribution_date`: Index on contribution date for date range queries
- `idx_candidate_committee`: Composite index on candidate_id and committee_id
- `idx_report_type`: Index on report type
- `idx_entity_type`: Index on entity type
- `idx_transaction_id`: Index on transaction ID

---

### Candidate

Stored candidate information.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| candidate_id | String | Unique candidate ID (indexed) |
| name | String | Candidate name (indexed) |
| office | String | Office type: P (President), S (Senate), H (House) (indexed) |
| party | String | Political party |
| state | String | State code (indexed) |
| district | String | District number (for House candidates) |
| election_years | JSON | List of election years |
| active_through | Integer | Last active year |
| street_address | String | Contact: street address |
| city | String | Contact: city |
| zip | String | Contact: ZIP code |
| email | String | Contact: email |
| phone | String | Contact: phone |
| website | String | Contact: website |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |

**Indexes:**
- `idx_office_state`: Composite index on office and state
- `idx_state_district`: Composite index on state and district

---

### Committee

Stored committee information.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| committee_id | String | Unique committee ID (indexed) |
| name | String | Committee name (indexed) |
| committee_type | String | Committee type (indexed) |
| committee_type_full | String | Full committee type description |
| candidate_ids | JSON | List of associated candidate IDs |
| party | String | Political party |
| state | String | State code |
| street_address | String | Contact: street address |
| street_address_2 | String | Contact: street address line 2 |
| city | String | Contact: city |
| zip | String | Contact: ZIP code |
| email | String | Contact: email |
| phone | String | Contact: phone |
| website | String | Contact: website |
| treasurer_name | String | Treasurer name |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |

**Indexes:**
- `idx_committee_type`: Index on committee type
- `idx_name`: Index on committee name

---

### FinancialTotal

Stored financial totals for candidates by cycle.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| candidate_id | String | Candidate ID (indexed) |
| cycle | Integer | Election cycle (indexed) |
| total_receipts | Float | Total receipts |
| total_disbursements | Float | Total disbursements |
| cash_on_hand | Float | Cash on hand |
| total_contributions | Float | Total contributions |
| individual_contributions | Float | Individual contributions |
| pac_contributions | Float | PAC contributions |
| party_contributions | Float | Party contributions |
| loan_contributions | Float | Loan contributions |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |

**Indexes:**
- `idx_candidate_cycle`: Unique composite index on candidate_id and cycle

---

### BulkDataMetadata

Metadata for bulk CSV file downloads and imports.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| cycle | Integer | Election cycle (indexed) |
| data_type | String | Data type (e.g., "schedule_a") (indexed) |
| download_date | DateTime | When file was downloaded |
| file_path | String | Path to downloaded file |
| file_size | Integer | File size in bytes |
| file_hash | String | MD5 hash of file content (indexed) |
| imported | Boolean | Whether file has been imported (indexed) |
| record_count | Integer | Number of records imported |
| last_updated | DateTime | When metadata was last updated |
| created_at | DateTime | When record was created |

**Indexes:**
- `idx_cycle_data_type`: Composite index on cycle and data_type
- `idx_file_hash`: Index on file hash for duplicate detection

---

### BulkImportJob

Tracks progress of bulk data import jobs.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Primary key (UUID) |
| job_type | String | Job type: 'single_cycle', 'all_cycles', 'cleanup_reimport' (indexed) |
| status | String | Status: 'pending', 'running', 'completed', 'failed', 'cancelled' (indexed) |
| cycle | Integer | Election cycle (nullable, indexed) |
| cycles | JSON | List of cycles for multi-cycle jobs |
| total_cycles | Integer | Total number of cycles |
| completed_cycles | Integer | Number of completed cycles |
| current_cycle | Integer | Current cycle being processed |
| total_records | Integer | Total records to import |
| imported_records | Integer | Number of records imported |
| skipped_records | Integer | Number of records skipped |
| current_chunk | Integer | Current chunk number |
| total_chunks | Integer | Total number of chunks |
| file_position | Integer | File position in bytes (for resumable imports) |
| data_type | String | Data type being imported |
| file_path | String | Path to file being imported |
| error_message | Text | Error message if job failed |
| started_at | DateTime | When job started (indexed) |
| completed_at | DateTime | When job completed |
| progress_data | JSON | Detailed progress information |

**Indexes:**
- `idx_status_started`: Composite index on status and started_at

---

### IndependentExpenditure

Stored independent expenditure data.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| expenditure_id | String | Unique expenditure ID (indexed) |
| cycle | Integer | Election cycle (indexed) |
| committee_id | String | Committee ID (indexed) |
| candidate_id | String | Candidate ID (indexed) |
| candidate_name | String | Candidate name |
| support_oppose_indicator | String | 'S' for support, 'O' for oppose |
| expenditure_amount | Float | Expenditure amount |
| expenditure_date | DateTime | Date of expenditure (indexed) |
| payee_name | String | Payee name |
| expenditure_purpose | Text | Purpose of expenditure |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_indep_exp_cycle_committee`: Composite index on cycle and committee_id
- `idx_indep_exp_cycle_candidate`: Composite index on cycle and candidate_id
- `idx_indep_exp_date`: Index on expenditure_date

---

### OperatingExpenditure

Stored operating expenditure data from FEC Schedule B filings.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| expenditure_id | String | Unique expenditure ID (indexed) |
| cycle | Integer | Election cycle (indexed) |
| committee_id | String | Committee ID (indexed) |
| payee_name | String | Payee name (indexed) |
| expenditure_amount | Float | Expenditure amount |
| expenditure_date | DateTime | Date of expenditure (indexed) |
| expenditure_purpose | Text | Purpose of expenditure |
| amendment_indicator | String | Amendment indicator (AMNDT_IND) |
| report_year | Integer | Report year (RPT_YR) |
| report_type | String | Report type (RPT_TP, indexed) |
| image_number | String | Image number (IMAGE_NUM) |
| line_number | String | Line number (LINE_NUM) |
| form_type_code | String | Form type code (FORM_TP_CD) |
| schedule_type_code | String | Schedule type code (SCHED_TP_CD) |
| transaction_pgi | String | Transaction PGI (TRANSACTION_PGI) |
| category | String | Expenditure category (indexed) |
| category_description | String | Category description |
| memo_code | String | Memo code (MEMO_CD) |
| memo_text | Text | Memo text |
| entity_type | String | Entity type (ENTITY_TP, indexed) |
| file_number | String | File number |
| transaction_id | String | Transaction ID (indexed) |
| back_reference_transaction_id | String | Back reference transaction ID |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_op_exp_cycle_committee`: Composite index on cycle and committee_id
- `idx_op_exp_date`: Index on expenditure_date
- `idx_op_exp_report_type`: Index on report_type
- `idx_op_exp_category`: Index on category
- `idx_op_exp_entity_type`: Index on entity_type
- `idx_op_exp_transaction_id`: Index on transaction_id

---

### CandidateSummary

Stored candidate summary data from bulk files.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| candidate_id | String | Candidate ID (indexed) |
| cycle | Integer | Election cycle (indexed) |
| candidate_name | String | Candidate name |
| office | String | Office type |
| party | String | Political party |
| state | String | State code |
| district | String | District number |
| total_receipts | Float | Total receipts |
| total_disbursements | Float | Total disbursements |
| cash_on_hand | Float | Cash on hand |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_cand_summary_candidate_cycle`: Unique composite index on candidate_id and cycle
- `idx_cand_summary_office_state`: Composite index on office and state

---

### CommitteeSummary

Stored committee summary data from bulk files.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| committee_id | String | Committee ID (indexed) |
| cycle | Integer | Election cycle (indexed) |
| committee_name | String | Committee name |
| committee_type | String | Committee type |
| total_receipts | Float | Total receipts |
| total_disbursements | Float | Total disbursements |
| cash_on_hand | Float | Cash on hand |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_committee_cycle`: Unique composite index on committee_id and cycle

---

### BulkDataImportStatus

Tracks import status per data type per cycle.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| data_type | String | Data type (indexed) |
| cycle | Integer | Election cycle (indexed) |
| status | String | Status: 'imported', 'not_imported', 'failed', 'in_progress' (indexed) |
| record_count | Integer | Number of records imported |
| last_imported_at | DateTime | When data was last imported |
| error_message | Text | Error message if import failed |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |

**Indexes:**
- `uq_data_type_cycle`: Unique constraint on data_type and cycle
- `idx_data_type_cycle`: Composite index on data_type and cycle
- `idx_status`: Index on status

---

### ElectioneeringComm

Electioneering communications data.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| cycle | Integer | Election cycle (indexed) |
| committee_id | String | Committee ID (indexed) |
| candidate_id | String | Candidate ID (indexed) |
| candidate_name | String | Candidate name |
| communication_date | DateTime | Date of communication (indexed) |
| communication_amount | Float | Communication amount |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_electioneering_cycle_committee`: Composite index on cycle and committee_id
- `idx_electioneering_date`: Index on communication_date

---

### CommunicationCost

Communication costs data.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| cycle | Integer | Election cycle (indexed) |
| committee_id | String | Committee ID (indexed) |
| candidate_id | String | Candidate ID (indexed) |
| candidate_name | String | Candidate name |
| communication_date | DateTime | Date of communication (indexed) |
| communication_amount | Float | Communication amount |
| raw_data | JSON | Raw FEC data |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| data_age_days | Integer | Days since data was current |

**Indexes:**
- `idx_comm_cost_cycle_committee`: Composite index on cycle and committee_id
- `idx_comm_cost_date`: Index on communication_date

---

### SavedSearch

Saved search queries for reuse.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| name | String | Search name |
| search_type | String | Type: 'candidate', 'committee', 'contribution', etc. (indexed) |
| search_params | JSON | Search parameters stored as JSON |
| created_at | DateTime | When search was created (indexed) |
| updated_at | DateTime | When search was last updated |

**Indexes:**
- `idx_saved_search_type`: Index on search_type
- `idx_saved_search_created`: Index on created_at

---

### ApiKeySetting

Stored API key configuration.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| api_key | String | FEC API key (stored as plain text - FEC keys are public) |
| source | String | Source: 'ui' or 'env' |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |
| is_active | Integer | 1 = active, 0 = deleted (soft delete) (indexed) |

**Indexes:**
- `idx_api_key_active`: Index on is_active

---

### ContributionLimit

FEC contribution limits by year, contributor category, and recipient category.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| effective_year | Integer | Year limits take effect (Jan 1) (indexed) |
| contributor_category | String | 'individual', 'multicandidate_pac', etc. (indexed) |
| recipient_category | String | 'candidate', 'pac', 'party_committee', etc. (indexed) |
| limit_amount | Float | Limit amount in dollars |
| limit_type | String | 'per_election', 'per_year', 'per_calendar_year' |
| notes | Text | Additional notes about the limit |
| created_at | DateTime | When record was created |
| updated_at | DateTime | When record was last updated |

**Indexes:**
- `uq_contribution_limit`: Unique constraint on effective_year, contributor_category, recipient_category, and limit_type
- `idx_contribution_limit_lookup`: Composite index on effective_year, contributor_category, and recipient_category

---

### AvailableCycle

Stored available election cycles from FEC API.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| cycle | Integer | Election cycle year (unique, indexed) |
| last_updated | DateTime | When cycle was last verified (indexed) |
| created_at | DateTime | When record was created |

**Indexes:**
- `idx_cycle`: Index on cycle
- `idx_last_updated`: Index on last_updated

---

### PreComputedAnalysis

Stored pre-computed analysis results for performance optimization.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Primary key |
| analysis_type | String | Type: 'donor_states', 'employer', 'velocity' (indexed) |
| candidate_id | String | Candidate ID (nullable, indexed) |
| committee_id | String | Committee ID (nullable, indexed) |
| cycle | Integer | Election cycle (nullable, indexed) |
| result_data | JSON | Analysis result data |
| computed_at | DateTime | When analysis was computed (indexed) |
| last_updated | DateTime | When analysis was last updated |
| data_version | Integer | Data version for incremental updates |

**Indexes:**
- `idx_analysis_type_candidate_cycle`: Composite index on analysis_type, candidate_id, and cycle
- `idx_analysis_type_cycle`: Composite index on analysis_type and cycle
- `idx_analysis_type_candidate`: Composite index on analysis_type and candidate_id
- `idx_analysis_type_committee`: Composite index on analysis_type and committee_id

---

### AnalysisComputationJob

Tracks progress of analysis computation jobs.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Primary key (UUID) |
| job_type | String | Type: 'cycle', 'candidate', 'committee', 'batch' (indexed) |
| status | String | Status: 'pending', 'running', 'completed', 'failed', 'cancelled' (indexed) |
| analysis_type | String | Analysis type (nullable, indexed) |
| candidate_id | String | Candidate ID (nullable, indexed) |
| committee_id | String | Committee ID (nullable, indexed) |
| cycle | Integer | Election cycle (nullable, indexed) |
| total_items | Integer | Total items to process |
| completed_items | Integer | Number of completed items |
| current_item | String | Current item being processed |
| error_message | Text | Error message if job failed |
| started_at | DateTime | When job started (indexed) |
| completed_at | DateTime | When job completed |
| progress_data | JSON | Detailed progress information |

**Indexes:**
- `idx_status_started`: Composite index on status and started_at
- `idx_job_type_status`: Composite index on job_type and status

---

## Key Indexes Summary

The database uses composite indexes extensively to optimize common query patterns:

- **Candidate/Committee lookups**: Indexes on IDs, office, state, district
- **Contribution queries**: Indexes on candidate_id, committee_id, contributor_name, contribution_date
- **Cycle-based queries**: Indexes on cycle combined with other fields
- **Bulk data tracking**: Indexes on cycle, data_type, status
- **Analysis queries**: Composite indexes on analysis_type with candidate_id, committee_id, or cycle

## Foreign Key Relationships

While the schema doesn't explicitly define foreign key constraints (to allow flexibility with FEC data), the relationships are:

- `Contribution.candidate_id` → `Candidate.candidate_id`
- `Contribution.committee_id` → `Committee.committee_id`
- `FinancialTotal.candidate_id` → `Candidate.candidate_id`
- `IndependentExpenditure.candidate_id` → `Candidate.candidate_id`
- `IndependentExpenditure.committee_id` → `Committee.committee_id`
- `OperatingExpenditure.committee_id` → `Committee.committee_id`
- `CandidateSummary.candidate_id` → `Candidate.candidate_id`
- `CommitteeSummary.committee_id` → `Committee.committee_id`

## Notes

- All tables include `created_at` and `updated_at` timestamps for audit trails
- Many tables include `raw_data` JSON fields to store original FEC API responses
- Tables with bulk data include `data_age_days` to track data freshness
- Soft deletes are used where appropriate (e.g., `ApiKeySetting.is_active`)
- Unique constraints prevent duplicate records where appropriate
- Composite indexes optimize common query patterns

