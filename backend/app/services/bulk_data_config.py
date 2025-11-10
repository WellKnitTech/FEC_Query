"""
Configuration for FEC bulk data types
Defines URL patterns, file formats, and metadata for different data types
"""
from typing import Dict, Optional, List
from enum import Enum


class DataType(str, Enum):
    """FEC bulk data types"""
    INDIVIDUAL_CONTRIBUTIONS = "individual_contributions"  # Schedule A
    CANDIDATE_MASTER = "candidate_master"  # cn*.zip
    COMMITTEE_MASTER = "committee_master"  # cm*.zip
    CANDIDATE_COMMITTEE_LINKAGE = "candidate_committee_linkage"  # ccl*.zip
    INDEPENDENT_EXPENDITURES = "independent_expenditures"  # independent_expenditure_*.csv
    OPERATING_EXPENDITURES = "operating_expenditures"  # oppexp*.zip
    CANDIDATE_SUMMARY = "candidate_summary"  # candidate_summary_*.csv
    COMMITTEE_SUMMARY = "committee_summary"  # committee_summary_*.csv
    PAC_SUMMARY = "pac_summary"  # webk*.zip
    OTHER_TRANSACTIONS = "other_transactions"  # oth*.zip
    PAS2 = "pas2"  # pas2*.zip
    ELECTIONEERING_COMM = "electioneering_comm"  # ElectioneeringComm_*.csv
    COMMUNICATION_COSTS = "communication_costs"  # CommunicationCosts_*.csv


class FileFormat(str, Enum):
    """File format types"""
    ZIP = "zip"
    CSV = "csv"


class DataTypeConfig:
    """Configuration for a specific data type"""
    
    def __init__(
        self,
        data_type: DataType,
        url_pattern: str,
        file_format: FileFormat,
        zip_internal_file: Optional[str] = None,
        header_file_url: Optional[str] = None,
        priority: int = 0,  # Higher priority = more important
        min_cycle: int = 1980,  # Earliest cycle available
        max_cycle: Optional[int] = None,  # Latest cycle (None = current)
        description: str = ""
    ):
        self.data_type = data_type
        self.url_pattern = url_pattern  # Use {YEAR} and {YY} placeholders
        self.file_format = file_format
        self.zip_internal_file = zip_internal_file  # File name inside ZIP
        self.header_file_url = header_file_url
        self.priority = priority
        self.min_cycle = min_cycle
        self.max_cycle = max_cycle
        self.description = description
    
    def get_url(self, cycle: int, base_url: str) -> str:
        """Generate URL for a specific cycle"""
        year_suffix = str(cycle)[-2:]
        url = self.url_pattern.format(YEAR=cycle, YY=year_suffix)
        return f"{base_url}{url}"


# High-value data types configuration
DATA_TYPE_CONFIGS: Dict[DataType, DataTypeConfig] = {
    DataType.INDIVIDUAL_CONTRIBUTIONS: DataTypeConfig(
        data_type=DataType.INDIVIDUAL_CONTRIBUTIONS,
        url_pattern="{YEAR}/indiv{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="itcont.txt",
        header_file_url="data_dictionaries/indiv_header_file.csv",
        priority=10,
        min_cycle=1980,
        description="Individual contributions (Schedule A)"
    ),
    DataType.CANDIDATE_MASTER: DataTypeConfig(
        data_type=DataType.CANDIDATE_MASTER,
        url_pattern="{YEAR}/cn{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="cn.txt",  # Typically cn.txt in the ZIP
        header_file_url="data_dictionaries/cn_header_file.csv",
        priority=9,
        min_cycle=1980,
        description="Candidate master file"
    ),
    DataType.COMMITTEE_MASTER: DataTypeConfig(
        data_type=DataType.COMMITTEE_MASTER,
        url_pattern="{YEAR}/cm{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="cm.txt",  # Typically cm.txt in the ZIP
        header_file_url="data_dictionaries/cm_header_file.csv",
        priority=9,
        min_cycle=1980,
        description="Committee master file"
    ),
    DataType.CANDIDATE_COMMITTEE_LINKAGE: DataTypeConfig(
        data_type=DataType.CANDIDATE_COMMITTEE_LINKAGE,
        url_pattern="{YEAR}/ccl{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="ccl.txt",
        header_file_url="data_dictionaries/ccl_header_file.csv",
        priority=8,
        min_cycle=2000,
        description="Candidate-committee linkages"
    ),
    DataType.INDEPENDENT_EXPENDITURES: DataTypeConfig(
        data_type=DataType.INDEPENDENT_EXPENDITURES,
        url_pattern="{YEAR}/independent_expenditure_{YEAR}.csv",
        file_format=FileFormat.CSV,
        header_file_url=None,  # CSV files typically have headers
        priority=8,
        min_cycle=2010,
        description="Independent expenditures"
    ),
    DataType.OPERATING_EXPENDITURES: DataTypeConfig(
        data_type=DataType.OPERATING_EXPENDITURES,
        url_pattern="{YEAR}/oppexp{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="oppexp.txt",
        header_file_url="data_dictionaries/oppexp_header_file.csv",
        priority=7,
        min_cycle=2004,
        description="Operating expenditures"
    ),
    DataType.CANDIDATE_SUMMARY: DataTypeConfig(
        data_type=DataType.CANDIDATE_SUMMARY,
        url_pattern="{YEAR}/candidate_summary_{YEAR}.csv",
        file_format=FileFormat.CSV,
        priority=7,
        min_cycle=2008,
        description="Candidate summary data"
    ),
    DataType.COMMITTEE_SUMMARY: DataTypeConfig(
        data_type=DataType.COMMITTEE_SUMMARY,
        url_pattern="{YEAR}/committee_summary_{YEAR}.csv",
        file_format=FileFormat.CSV,
        priority=7,
        min_cycle=2008,
        description="Committee summary data"
    ),
    DataType.PAC_SUMMARY: DataTypeConfig(
        data_type=DataType.PAC_SUMMARY,
        url_pattern="{YEAR}/webk{YY}.zip",
        file_format=FileFormat.ZIP,
        zip_internal_file="webk.txt",
        priority=6,
        min_cycle=1980,
        description="PAC and party summary"
    ),
    DataType.ELECTIONEERING_COMM: DataTypeConfig(
        data_type=DataType.ELECTIONEERING_COMM,
        url_pattern="{YEAR}/ElectioneeringComm_{YEAR}.csv",
        file_format=FileFormat.CSV,
        priority=6,
        min_cycle=2010,
        description="Electioneering communications"
    ),
    DataType.COMMUNICATION_COSTS: DataTypeConfig(
        data_type=DataType.COMMUNICATION_COSTS,
        url_pattern="{YEAR}/CommunicationCosts_{YEAR}.csv",
        file_format=FileFormat.CSV,
        priority=6,
        min_cycle=2010,
        description="Communication costs"
    ),
}


def get_config(data_type: DataType) -> Optional[DataTypeConfig]:
    """Get configuration for a data type"""
    return DATA_TYPE_CONFIGS.get(data_type)


def get_high_priority_types() -> List[DataType]:
    """Get data types ordered by priority (highest first)"""
    configs = sorted(
        DATA_TYPE_CONFIGS.values(),
        key=lambda c: c.priority,
        reverse=True
    )
    return [c.data_type for c in configs]


def get_available_cycles(data_type: DataType, current_year: int) -> List[int]:
    """Get list of available cycles for a data type"""
    config = get_config(data_type)
    if not config:
        return []
    
    max_cycle = config.max_cycle or ((current_year // 2) * 2 + 2)  # Next even year
    cycles = []
    for year in range(config.min_cycle, max_cycle + 1, 2):
        cycles.append(year)
    return cycles

