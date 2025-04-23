#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Deals Summary Data Update Script
--------------------------------
This script fetches deal summary data from Refinitiv, processes it, and updates a MySQL database.
It's designed to run as an automated Windows task.

Author: Converted from Jupyter notebook
Date: 2025-03-19
"""

import os
import time
import logging
import shutil
from datetime import datetime
import mysql.connector
import pandas as pd
import refinitiv.data as rd
import numpy as np
from tqdm import tqdm
# Import database utility functions
from db_utils import connect_to_database, initialize_refinitiv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"deals_update_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_data(query_string, default_fields):
    """Fetch data from Refinitiv with retry mechanism."""
    max_retries = 3
    retry_delay = 5  # seconds
    all_deals = []
    
    for retry in range(max_retries):
        try:
            logger.info(f"Fetching data from Refinitiv (attempt {retry+1}/{max_retries})...")
            deal = rd.get_data(
                universe=query_string,
                fields=default_fields,
                parameters={"Curn": "USD"},
                use_field_names_in_headers=True
            )
            logger.info(f"Successfully fetched data with {len(deal)} records")
            all_deals.append(deal)
            break  # Successfully fetched data
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching data: {error_msg}")
            if retry < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum retries reached for data fetching")
                raise
    
    return all_deals

def process_data(all_deals):
    """Process the fetched data."""
    try:
        logger.info("Processing fetched data...")
        
        # Filter first dataset for deals > $1B
        tmp = all_deals[0]
        all_deals[0] = tmp[tmp['TR.MNARANKVALUEINCNETDEBT'] > 100000000]

        tmp = all_deals[1]
        all_deals[1] = tmp[tmp['TR.MNARANKVALUEINCNETDEBT'] > 100000000]
        
        # Combine all data
        big_table = pd.concat(all_deals, ignore_index=True)
        big_table = big_table.drop(columns=['Instrument'])
        
        # Generate deals_summary
        fields = {
            'TR.MNASDCDEALNUMBER': 'deal_id',
            'TR.MNASTATUS': 'status',
            'TR.MNAANNDATE': 'date_announced',
            'TR.MNAEFFECTIVEDATE': 'date_effective',
            'TR.MNARANKVALUEINCNETDEBT': 'rank_value',
            'TR.MNATARGETPERMID': 'target_permid',
            'TR.MNATARGET': 'target_name',
            'TR.MNATARGETPARENTPERMID': 'target_parent_permid',
            'TR.MNATARGETPARENT': 'target_parent_name',
            'TR.MNATARGETULTPARENTPERMID': 'target_ultimate_parent_permid',
            'TR.MNATARGETULTPARENT': 'target_ultimate_parent_name',
            'TR.MNAACQUIRORPERMID': 'acquiror_permid',
            'TR.MNAACQUIROR': 'acquiror_name',
            'TR.MNAACQUIRORPARENTPERMID': 'acquiror_parent_permid',
            'TR.MNAACQUIRORPARENT': 'acquiror_parent_name',
            'TR.MNAACQUIRORULTPARENTPERMID': 'acquiror_ultimate_parent_permid',
            'TR.MNAACQUIRORULTPARENT': 'acquiror_ultimate_parent_name',
            'TR.MNAFORMTYPE': 'form'
        }
        
        deals_summary = big_table[fields.keys()].rename(columns=fields)
        logger.info(f"Processed data into {len(deals_summary)} rows")
        return deals_summary
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def export_to_csv(deals_summary, conn=None):
    """Export deals_summary to CSV with backup."""
    try:
        # Generate filename with today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Create a backup of the deals_summary table from the database
        if conn:
            try:
                logger.info("Creating backup from deals_summary table...")
                backup_query = "SELECT * FROM deals_summary"
                df_backup = pd.read_sql(backup_query, conn)
                
                # Remove any existing deals_summary CSV files first
                for file in os.listdir("."):
                    if file.startswith("deals_summary") and file.endswith(".csv"):
                        try:
                            os.remove(file)
                            logger.info(f"Deleted existing file: {file}")
                        except OSError as e:
                            logger.error(f"Error deleting file {file}: {e}")
                
                # Create the backup CSV with date
                backup_filename = f"deals_summary_{today_date}.csv"
                df_backup.to_csv(backup_filename, index=False)
                logger.info(f"Created backup from deals_summary table as {backup_filename}")
            except Exception as e:
                logger.error(f"Error creating backup from database: {e}")
                # Continue with the process even if backup fails
        else:
            # If no connection provided, just delete existing CSV files
            for file in os.listdir("."):
                if file.startswith("deals_summary") and file.endswith(".csv"):
                    try:
                        os.remove(file)
                        logger.info(f"Deleted existing file: {file}")
                    except OSError as e:
                        logger.error(f"Error deleting file {file}: {e}")
        
        # Export to CSV
        deals_summary.to_csv("deals_summary.csv", index=False)
        logger.info("Exported data to deals_summary.csv")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

def create_dummy_table(conn):
    """Create a dummy deals_summary_2 table in MySQL if it doesn't exist."""
    try:
        cursor = conn.cursor()
        
        # Check if deals_summary_2 table exists
        cursor.execute("SHOW TABLES LIKE 'deals_summary_2'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.info("Creating deals_summary_2 table...")
            
            # Get the structure of deals_summary table
            cursor.execute("SHOW CREATE TABLE deals_summary")
            create_table_stmt = cursor.fetchone()[1]
            
            # Modify the statement to create deals_summary_2
            create_table_stmt = create_table_stmt.replace("CREATE TABLE `deals_summary`", 
                                                         "CREATE TABLE `deals_summary_2`")
            
            # Create the table
            cursor.execute(create_table_stmt)
            conn.commit()
            logger.info("deals_summary_2 table created successfully")
        else:
            logger.info("deals_summary_2 table already exists")
    except Exception as e:
        logger.error(f"Error creating dummy table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def update_database(conn, deals_summary):
    """Update the database with new data."""
    try:
        cursor = conn.cursor()
        
        # Truncate the target table
        logger.info("Truncating deals_summary_2 table...")
        cursor.execute("TRUNCATE TABLE deals_summary_2;")
        conn.commit()
        
        # Prepare for batch insert
        batch_size = 1000  # Adjust based on performance testing
        cols = ', '.join(f"`{col}`" for col in deals_summary.columns)
        placeholders = ', '.join(['%s'] * len(deals_summary.columns))
        insert_query = f"INSERT INTO deals_summary_2 ({cols}) VALUES ({placeholders})"
        
        # Insert data in batches with progress tracking
        total_rows = len(deals_summary)
        logger.info(f"Inserting {total_rows} rows in batches of {batch_size}...")
        
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = deals_summary.iloc[start:end]
            
            # Convert Pandas/Numpy types to Python built-in types
            # Ensure dates are formatted as 'YYYY-MM-DD' strings without time portion
            data_tuples = []
            for row in batch.itertuples(index=False, name=None):
                processed_row = []
                for v in row:
                    if pd.isna(v):
                        processed_row.append(None)
                    elif isinstance(v, (pd.Timestamp, datetime)):
                        # Format dates as 'YYYY-MM-DD' strings
                        processed_row.append(v.strftime('%Y-%m-%d'))
                    elif hasattr(v, 'item'):
                        # Convert other numpy/pandas types to Python types
                        processed_row.append(v.item())
                    else:
                        processed_row.append(v)
                data_tuples.append(tuple(processed_row))
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            logger.info(f"Inserted rows {start} to {end} ({end-start} rows)")
        
        logger.info(f"Database update completed successfully. Total rows: {total_rows}")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def main():
    """Main function to orchestrate the data update process."""
    start_time = time.time()
    logger.info("Starting deals summary data update process")
    
    try:
        # Initialize connections using the db_utils functions
        conn = connect_to_database()
        initialize_refinitiv()
        
        # Create dummy table if it doesn't exist
        create_dummy_table(conn)
        
        # Define default fields
        default_fields = [
            'TR.MNASDCDEALNUMBER',
            'TR.MNASTATUS',
            'TR.MNAANNDATE',
            'TR.MNAEFFECTIVEDATE',
            'TR.MNARANKVALUEINCNETDEBT',
            'TR.MNATARGETPERMID',
            'TR.MNATARGET',
            'TR.MNATARGETPARENTPERMID',
            'TR.MNATARGETPARENT',
            'TR.MNATARGETULTPARENTPERMID',
            'TR.MNATARGETULTPARENT',
            'TR.MNAACQUIRORPERMID',
            'TR.MNAACQUIROR',
            'TR.MNAACQUIRORPARENTPERMID',
            'TR.MNAACQUIRORPARENT',
            'TR.MNAACQUIRORULTPARENTPERMID',
            'TR.MNAACQUIRORULTPARENT',
            'TR.MNAFORMTYPE'
        ]
        
        # Define query strings
        # >1bn exclude PJT
        query_string1 = (
        'SCREEN('
        'U(IN(DEALS)), '  # Base filter for deals
        'IN(TR.MnAStatus,"P","C"), '  # MnA Status filter: Pending or Completed
        'IN(TR.MnAType,"1"), '  # MnA Type filter: Specific type: Disclosed Value
        'IN(TR.MnATargetNation,"US") OR IN(TR.MnAAcquirorNation,"US"), '  # Nation filter: Target or Acquiror is US
        'BETWEEN(TR.MnAAnnDate,20100101,20250414), '  # Date range filter: 2010-01-01 to 2024-11-22
        'TR.MnARankValueIncNetDebt(Scale=8)>=1, '  # Rank value filter: >= 1 (scaled to billions)
        'TR.MnAHasDisclosedFee==true, '  # Disclosed fee filter: Include only deals with disclosed fees
        ')'
        )

        query_string2 = (
        'SCREEN('
        'U(IN(DEALS)), '  # Base filter for deals
        'IN(TR.MnAStatus,"P","C"), '  # MnA Status filter: Pending or Completed
        'IN(TR.MnAType,"1"), '  # MnA Type filter: Specific type: Disclosed Value
        'IN(TR.MnATargetNation,"US") OR IN(TR.MnAAcquirorNation,"US"), '  # Nation filter: Target or Acquiror is US
        'BETWEEN(TR.MnAAnnDate,19850101,20091231), '  # Date range filter: 2010-01-01 to 2024-11-22
        'TR.MnARankValueIncNetDebt(Scale=8)>=1, '  # Rank value filter: >= 1 (scaled to billions)
        'TR.MnAHasDisclosedFee==true, '  # Disclosed fee filter: Include only deals with disclosed fees
        ')'
        )
        
        # Fetch data
        logger.info("Fetching data for query 1 (>1bn exclude PJT)...")
        all_deals = []
        all_deals = fetch_data(query_string1, default_fields)
        
        logger.info("Fetching data for query 2 (All PJT deals after 1 Oct 2015)...")
        deals2 = fetch_data(query_string2, default_fields)
        all_deals.extend(deals2)
        
        # Process data
        deals_summary = process_data(all_deals)
        
        # Export to CSV with backup
        export_to_csv(deals_summary, conn)
        
        # Update database
        update_database(conn, deals_summary)
        
        # Close connections
        if conn:
            conn.close()
            logger.info("Database connection closed")
        
        rd.close_session()
        logger.info("Refinitiv session closed")
        
        # Calculate and log execution time
        execution_time = time.time() - start_time
        logger.info(f"Data update process completed successfully in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Data update process failed: {e}")
        # Ensure connections are closed even if an error occurs
        try:
            if 'conn' in locals() and conn:
                conn.close()
                logger.info("Database connection closed")
            rd.close_session()
            logger.info("Refinitiv session closed")
        except:
            pass
        raise

if __name__ == "__main__":
    main()
