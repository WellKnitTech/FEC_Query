from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()


@lru_cache()
def get_fec_api_key() -> str:
    """Get FEC API key from environment"""
    api_key = os.getenv("FEC_API_KEY")
    if not api_key:
        raise ValueError("FEC_API_KEY environment variable is not set")
    return api_key


@lru_cache()
def get_fec_api_base_url() -> str:
    """Get FEC API base URL from environment"""
    return os.getenv("FEC_API_BASE_URL", "https://api.open.fec.gov/v1")

