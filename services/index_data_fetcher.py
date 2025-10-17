# -*- coding: utf-8 -*-
# services/index_data_fetcher.py
# مسئول واکشی داده‌های شاخص بازار (با منطق بازگشتی: اولویت pytse-client، سپس BrsApi.ir)

import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import requests

# --- وابستگی‌ها به pytse_client ---
try:
    import pytse_client as tse
    PYTSE_CLIENT_AVAILABLE = True
except ImportError:
    PYTSE_CLIENT_AVAILABLE = False
    logging.warning("pytse_client is not installed. Will only use BrsApi.ir.")

# ⚠️ Suppress InsecureRequestWarning
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- BrsApi.ir Settings ---
B_R_S_API_KEY = "BvhdYHBjqiyIQ7eTuQBKN17ZuLpHkQZ1"
B_R_S_API_URL = "https://brsapi.ir/Api/Tsetmc/Index.php"
API_TYPE_PARAM = 3 

logger = logging.getLogger(__name__)

# --- Index Name Mapping (Required Indices) ---
# Maps API/pytse name to internal friendly key
INDEX_NAME_MAPPING = {
    "شاخص کل": "Total_Index",
    "شاخص کل (هم وزن)": "Equal_Weighted_Index",
    "شاخص قیمت (هم وزن)": "Price_Equal_Weighted_Index",
}
REQUIRED_INDICES_COUNT = len(INDEX_NAME_MAPPING)


# --- Headers ---
CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*"
}

# ---------------------------
# Helper Functions
# ---------------------------
def _default_index_payload() -> Dict[str, Dict[str, Any]]:
    """Default safe payload for the required indices."""
    now_date = datetime.now().strftime("%Y-%m-%d")
    return {
        "Total_Index": {"value": None, "change": None, "percent": None, "date": now_date},
        "Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": now_date},
        "Price_Equal_Weighted_Index": {"value": None, "change": None, "percent": None, "date": now_date},
    }

def _safe_to_float(x) -> Optional[float]:
    """Safely convert value to float and round."""
    try:
        val = pd.to_numeric(x, errors="coerce")
        # Rounding to 4 decimal places for index values
        return round(float(val), 4) if pd.notna(val) else None
    except Exception:
        return None

# ---------------------------
# Source 2: BrsApi.ir (Fallback)
# ---------------------------
def _fetch_indices_from_brsapi() -> Dict[str, Dict[str, Any]]:
    """Fetches market indices from BrsApi.ir (Fallback source)."""
    logger.info("⬅️ Attempting to fetch market index data from BrsApi.ir (Fallback).")
    result = _default_index_payload()
    params = {'key': B_R_S_API_KEY, 'type': API_TYPE_PARAM}

    try:
        with requests.Session() as session:
            response = session.get(
                B_R_S_API_URL, 
                params=params, 
                headers=CUSTOM_HEADERS, 
                timeout=15, 
                verify=False # Ignore SSL
            ) 
            response.raise_for_status() 

            data_list = response.json()
            if not isinstance(data_list, list) or not data_list:
                logger.warning(f"BrsApi.ir response is empty or has an incorrect structure.")
                return result
            
            successful_fetches = 0
            for index_item in data_list:
                index_name_raw = index_item.get("name")
                friendly_name = INDEX_NAME_MAPPING.get(index_name_raw)
                if not friendly_name: continue
                    
                value = _safe_to_float(index_item.get("index"))
                change = _safe_to_float(index_item.get("index_change")) 
                percent = _safe_to_float(index_item.get("index_change_percent"))
                date_fmt = datetime.now().strftime("%Y-%m-%d")

                # Must have complete data to be considered successful
                if value is not None and change is not None and percent is not None:
                    result[friendly_name] = {
                        "value": value,
                        "change": change,
                        "percent": percent,
                        "date": date_fmt,
                    }
                    successful_fetches += 1
            
            if successful_fetches == REQUIRED_INDICES_COUNT:
                logger.info("✅ Market index data successfully retrieved from BrsApi.ir.")
                return result
            else:
                logger.warning(f"⚠️ BrsApi.ir fetch was incomplete. Only {successful_fetches}/{REQUIRED_INDICES_COUNT} indices were fully received.")
                return _default_index_payload()
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error communicating with BrsApi.ir API: {e}", exc_info=False)
    except Exception as e:
        logger.error(f"❌ Unexpected error processing BrsApi.ir data: {e}", exc_info=True)

    return _default_index_payload()


# ---------------------------
# Source 1: pytse-client (Primary)
# ---------------------------
def _fetch_indices_from_pytse_client() -> Dict[str, Dict[str, Any]]:
    """
    Fetches market indices from pytse-client (Primary source). 
    Returns {} if any of the three required indices cannot be fully fetched.
    """
    if not PYTSE_CLIENT_AVAILABLE:
        return {} # Triggers Fallback

    logger.info("➡️ Attempting to fetch market index data from pytse-client (Primary).")
    result = {}
    
    index_symbols_persian = list(INDEX_NAME_MAPPING.keys())
    successful_fetches = 0
    
    for symbol_name in index_symbols_persian:
        friendly_name = INDEX_NAME_MAPPING.get(symbol_name)
        if not friendly_name: continue

        try:
            # Use FinancialIndex class
            index_obj = tse.FinancialIndex(symbol=symbol_name)
            value = _safe_to_float(index_obj.last_value)
            
            # Fetch history to calculate change and percent
            history_df = index_obj.history
            change = None
            percent = None

            if history_df is not None and len(history_df) >= 2:
                # Yesterday's closing price (second to last row)
                yesterday_close = history_df.iloc[-2]['close'] 
                
                if value is not None and yesterday_close is not None and yesterday_close != 0:
                    change = value - yesterday_close
                    percent = (change / yesterday_close) * 100
                
            date_fmt = datetime.now().strftime("%Y-%m-%d")

            # Check for data completeness (value, change, and percent MUST be available)
            if value is not None and change is not None and percent is not None:
                result[friendly_name] = {
                    "value": value,
                    "change": round(change, 2),
                    "percent": round(percent, 2),
                    "date": date_fmt,
                }
                successful_fetches += 1
            else:
                logger.warning(f"⚠️ Incomplete data (value/change/percent) for index {symbol_name} from pytse-client. Failing over.")
                return {} # Fallback immediately if data is incomplete

        except Exception as e:
            logger.warning(f"⚠️ Critical error fetching index {symbol_name} from pytse-client: {e}. Falling back.", exc_info=False)
            return {} # Fallback immediately if an error occurs
            
    # Only return the result if all required indices were successfully fetched
    if successful_fetches == REQUIRED_INDICES_COUNT:
        logger.info("✅ All required indices successfully retrieved from pytse-client.")
        final_result = _default_index_payload()
        final_result.update(result)
        return final_result

    # If the fetch was incomplete, return an empty dict to trigger Fallback
    return {}

# ---------------------------
# Main function (Fallback Logic)
# ---------------------------
def get_market_indices() -> Dict[str, Dict[str, Any]]:
    """
    Retrieves market indices, prioritizing pytse-client then BrsApi.ir.
    
    Output:
        Dict[str, Dict[str, Any]]: Contains Total_Index, Equal_Weighted_Index, and Price_Equal_Weighted_Index
    """
    
    # 1. Try fetching from the Primary source (pytse-client)
    pytse_result = _fetch_indices_from_pytse_client()
    
    # Check for successful fetch (if Total_Index is present and we have the required count)
    if pytse_result.get("Total_Index", {}).get("value") is not None and len(pytse_result) == REQUIRED_INDICES_COUNT:
        return pytse_result
        
    # 2. If unsuccessful, try fetching from the Fallback source (BrsApi.ir)
    logger.warning("↩️ pytse-client fetch was unsuccessful or incomplete. Falling back to BrsApi.ir...")
    brsapi_result = _fetch_indices_from_brsapi()
    
    # 3. Return Fallback result
    if brsapi_result.get("Total_Index", {}).get("value") is not None and len(brsapi_result) == REQUIRED_INDICES_COUNT:
        return brsapi_result
        
    # 4. Finally, return default (empty) data
    logger.error("❌ Both market index sources failed to fetch data.")
    return _default_index_payload()


# ---------------------------
# Exports
# ---------------------------
__all__ = [
    "get_market_indices",
    "INDEX_NAME_MAPPING"
]