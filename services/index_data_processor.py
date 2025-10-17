# -*- coding: utf-8 -*-
# services/index_data_processor.py
# Responsible for processing and storing market index data in the database.

import logging
from datetime import datetime
from typing import Dict, Any
from sqlalchemy.exc import SQLAlchemyError
# Using insert dialect for Upsert logic (suitable for various SQL engines)
from sqlalchemy.dialects.sqlite import insert 
from sqlalchemy import create_engine # Needed for example context only
from sqlalchemy.orm import sessionmaker, Session

# --- Dependencies (Assuming these are available in the project structure) ---
# NOTE: Replace 'models' and 'utils' with the correct import paths for your project.
from models import DailyIndexData 
from services.index_data_fetcher import get_market_indices, INDEX_NAME_MAPPING 
from services.technical_analysis_utils import convert_gregorian_to_jalali 

# Mock/Placeholder for convert_gregorian_to_jalali for completeness
def convert_gregorian_to_jalali(gregorian_date_obj):
    """Placeholder for converting Gregorian date object to Persian date string (YYYY-MM-DD)."""
    # In a real app, this function should be imported from utils.py
    try:
        import jdatetime
        jdate_obj = jdatetime.date.fromgregorian(date=gregorian_date_obj)
        return jdate_obj.strftime("%Y-%m-%d")
    except ImportError:
        # Fallback for systems without jdatetime
        return gregorian_date_obj.strftime("%Y-%m-%d") # Use Gregorian date as fallback

logger = logging.getLogger(__name__)

def store_market_indices_data(db_session: Session) -> bool:
    """
    Fetches market indices and stores them in the daily_index_data table using an Upsert logic.
    
    :param db_session: A SQLAlchemy Session object for executing transactions.
    :return: True if storage is successful, False otherwise.
    """
    logger.info("Starting market index data fetch and store process...")
    
    # 1. Fetch data from sources
    index_data = get_market_indices()
    
    # Check if valid data was received (Total Index value is the minimum check)
    if not index_data or index_data.get("Total_Index", {}).get("value") is None:
        logger.error("❌ Market index data was not fetched or is invalid. Storage cancelled.")
        return False

    # 2. Convert Gregorian date to Persian (Jalali) date
    try:
        gregorian_date_str = index_data["Total_Index"]["date"] 
        gregorian_date_obj = datetime.strptime(gregorian_date_str, "%Y-%m-%d").date()
        # Uses the imported (or mocked) conversion utility
        jalali_date = convert_gregorian_to_jalali(gregorian_date_obj)
    except Exception as e:
        logger.error(f"❌ Date conversion error: {e}", exc_info=False)
        return False
        
    if not jalali_date:
        logger.error("❌ Invalid Persian date created. Storage cancelled.")
        return False
        
    records_to_save = []
    
    # 3. Prepare records for storage
    # Filter only the indices defined in the mapping
    valid_index_types = set(INDEX_NAME_MAPPING.values())
    
    for friendly_name, data in index_data.items():
        if data["value"] is not None and friendly_name in valid_index_types:
            record = {
                'jdate': jalali_date,
                'index_type': friendly_name,
                'value': data['value'],
                'percent_change': data['percent'] if data['percent'] is not None else 0.0,
                'created_at': datetime.now()
            }
            records_to_save.append(record)
        else:
            logger.warning(f"⚠️ Index {friendly_name} with incomplete or irrelevant data was skipped from storage.")


    if not records_to_save:
        logger.warning("No valid index records found to save.")
        return True
        
    # 4. Store or Update (Upsert) data in the database
    try:
        for record in records_to_save:
            # SQLAlchemy Upsert logic (Insert on Conflict Update)
            
            # 💡 NOTE: This syntax is tailored for SQLite/PostgreSQL dialects.
            # You might need to adjust based on your specific RDBMS (e.g., MySQL uses ON DUPLICATE KEY UPDATE).
            insert_stmt = insert(DailyIndexData).values(record)
            
            # Define what to do on conflict (based on the UniqueConstraint jdate/index_type)
            on_conflict_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['jdate', 'index_type'], 
                set_=dict(
                    value=record['value'],
                    percent_change=record['percent_change'],
                    created_at=datetime.now() 
                )
            )
            
            db_session.execute(on_conflict_stmt)

        db_session.commit()
        logger.info(f"✅ Success: {len(records_to_save)} market index records for {jalali_date} stored/updated successfully.")
        return True
        
    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"❌ Database error while storing indices: {e}", exc_info=True)
        return False
    except Exception as e:
        db_session.rollback() # Ensure rollback on general errors too
        logger.error(f"❌ Unexpected error in index storage service: {e}", exc_info=True)
        return False

# ---------------------------
# Exports
# ---------------------------
__all__ = [
    "store_market_indices_data"
]