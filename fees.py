#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Advisor Fees Data Update Script

This script fetches advisor fees data from Refinitiv and updates the MySQL database.
It creates a dummy advisor_fees_2 table for testing updates and exports data to CSV files.
"""

import os
import logging
import time
from datetime import datetime
import math
import hashlib
import uuid
import copy
import json
import numpy as np
import pandas as pd
import mysql.connector
import refinitiv.data as rd
# Import database utility functions
from db_utils import connect_to_database, initialize_refinitiv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Test mode flag - set to True to use minimal data for testing
TEST_MODE = False
# Test table suffix - append to table names when test mode is active
TEST_TABLE_SUFFIX = "_test"

def get_data_from_database(conn):
    """Get advisor role and fees data from the database."""
    try:
        # Determine table names based on test mode
        advisor_role_table = "advisor_role" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        advisor_fees_table = "advisor_fees" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        
        logger.info(f"Fetching advisor role data from {advisor_role_table} table...")
        
        # Get advisor role data
        query = f"SELECT * FROM {advisor_role_table}"
        roles = pd.read_sql(query, conn)
        
        # Get existing advisor fees data
        query = f"SELECT * FROM {advisor_fees_table}"
        df_fees = pd.read_sql(query, conn)
        
        logger.info(f"Retrieved {len(roles)} advisor roles and {len(df_fees)} advisor fees records")
        
        return roles, df_fees
    except Exception as e:
        logger.error(f"Error fetching data from database: {e}")
        raise

def fetch_data(query_string, max_retries=3, retry_delay=5):
    """Fetch data from Refinitiv with retry mechanism."""
    all_deals = []
    
    for retry in range(max_retries):
        try:
            logger.info(f"Fetching data for query: {query_string[:50]}...")
            fields = {
                'TR.MNASDCDEALNUMBER': 'deal_id',
                'TR.MNASTATUS': 'status',
                'TR.MNAANNDATE': 'date_announced',
                'TR.MNAEFFECTIVEDATE': 'date_effective',
                'TR.MNARANKVALUEINCNETDEBT': 'rank_value',
                'TR.MNATARGETTOTALFEES': 'target_advisor_disclosed_fee_total',
                'TR.MNATARGETFINADVISORFEESOURCE': 'target_advisor_fee_source',
                'TR.MNAACQUIRORTOTALFEES': 'acquiror_advisor_disclosed_fee_total',
                'TR.MNAACQUIRORFINADVISORFEESOURCE': 'acquiror_advisor_fee_source',
                'TR.MNATARGET': 'target_name',
                'TR.MNATARGETPERMID': 'target_permid',
                'TR.MNAACQUIROR': 'acquiror_name',
                'TR.MNAACQUIRORPERMID': 'acquiror_permid',
                'TR.MNAFORMTYPE': 'form',
                'TR.MNAALLTARGETFINADVISOR': 'target_advisor_name',
                'TR.MNAALLTARGETFINADVISOR.ADVISORCODE': 'target_advisor_code',
                'TR.MNAALLTARGETFINADVISORFEEASSIGNMENT': 'target_advisor_disclosed_fee_role',
                'TR.MNAALLTARGETFINADVISORASSIGNMENTFEE': 'target_advisor_disclosed_fee',
                'TR.MNAALLACQUIRORFINADVISOR': 'acquiror_advisor_name',
                'TR.MNAALLACQUIRORFINADVISOR.ADVISORCODE': 'acquiror_advisor_code',
                'TR.MNAALLACQUIRORFINADVISORFEEASSIGNMENT': 'acquiror_advisor_disclosed_fee_role',
                'TR.MNAALLACQUIRORFINADVISORASSIGNMENTFEE': 'acquiror_advisor_disclosed_fee',
            }

            
            deal = rd.get_data(universe=query_string, fields=list(fields.keys()), parameters={"Curn": "USD"}, use_field_names_in_headers=True)
            all_deals.append(deal)
            logger.info(f"Successfully fetched data for query")
            break  # Successfully fetched data
        except Exception as e:
            if retry < max_retries - 1:
                logger.error(f"Error fetching data: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Error fetching data: {e}. Maximum retries reached.")
                break
    
    return all_deals

def process_deals_data():
    """Process deals data to get advisor fees information."""
    try:
        logger.info("Processing deals data...")
        
        # Define query strings for Refinitiv
        if TEST_MODE:
            # Use a very limited query for testing
            logger.info("TEST MODE: Using limited query for testing")
            # Limit the date range to get fewer results for testing
            query_string1 = (
            'SCREEN('
            'U(IN(DEALS)), '  # Base filter for deals
            'IN(TR.MnAStatus,"P","C"), '  # MnA Status filter: Pending or Completed
            'IN(TR.MnAType,"1"), '  # MnA Type filter: Specific type: Disclosed Value
            'IN(TR.MnATargetNation,"US") OR IN(TR.MnAAcquirorNation,"US"), '  # Nation filter: Target or Acquiror is US
            'BETWEEN(TR.MnAAnnDate,20240101,20250414), '  # Limited date range for testing
            'TR.MnARankValueIncNetDebt(Scale=8)>=5, '  # Higher rank value threshold for testing
            'TR.MnAHasDisclosedFee==true, '  # Disclosed fee filter: Include only deals with disclosed fees
            ')'
            )
            query_string2 = query_string1  # Use the same limited query for testing
        else:
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
            
        # Fetch data from Refinitiv
        all_deals = []
        all_deals.extend(fetch_data(query_string1))
        if not TEST_MODE or len(all_deals) == 0 or len(all_deals[0]) < 5:
            # If in test mode, only fetch second query if first query didn't return enough data
            all_deals.extend(fetch_data(query_string2))
        
        # Combine all deals data
        deals = pd.concat(all_deals, ignore_index=True)
        
        if TEST_MODE:
            # In test mode, limit to a small sample
            logger.info(f"TEST MODE: Limiting from {len(deals)} to max 20 deals")
            deals = deals.head(20)
        
        deals = deals[deals['TR.MNARANKVALUEINCNETDEBT'] > 100000000]
        
        # Rename columns
        fields = {
            'TR.MNASDCDEALNUMBER': 'deal_id',
            'TR.MNASTATUS': 'status',
            'TR.MNAANNDATE': 'date_announced',
            'TR.MNAEFFECTIVEDATE': 'date_effective',
            'TR.MNARANKVALUEINCNETDEBT': 'rank_value',
            'TR.MNATARGETTOTALFEES': 'target_advisor_disclosed_fee_total',
            'TR.MNATARGETFINADVISORFEESOURCE': 'target_advisor_fee_source',
            'TR.MNAACQUIRORTOTALFEES': 'acquiror_advisor_disclosed_fee_total',
            'TR.MNAACQUIRORFINADVISORFEESOURCE': 'acquiror_advisor_fee_source',
            'TR.MNATARGET': 'target_name',
            'TR.MNATARGETPERMID': 'target_permid',
            'TR.MNAACQUIROR': 'acquiror_name',
            'TR.MNAACQUIRORPERMID': 'acquiror_permid',
            'TR.MNAFORMTYPE': 'form',
            'TR.MNAALLTARGETFINADVISOR': 'target_advisor_name',
            'TR.MNAALLTARGETFINADVISOR.ADVISORCODE': 'target_advisor_code',
            'TR.MNAALLTARGETFINADVISORFEEASSIGNMENT': 'target_advisor_disclosed_fee_role',
            'TR.MNAALLTARGETFINADVISORASSIGNMENTFEE': 'target_advisor_disclosed_fee',
            'TR.MNAALLACQUIRORFINADVISOR': 'acquiror_advisor_name',
            'TR.MNAALLACQUIRORFINADVISOR.ADVISORCODE': 'acquiror_advisor_code',
            'TR.MNAALLACQUIRORFINADVISORFEEASSIGNMENT': 'acquiror_advisor_disclosed_fee_role',
            'TR.MNAALLACQUIRORFINADVISORASSIGNMENTFEE': 'acquiror_advisor_disclosed_fee',
        }
        deals = deals.rename(columns=fields)
        
        # Drop the instrument column which is not needed
        if 'Instrument' in deals.columns:
            deals = deals.drop(columns=['Instrument'])
        
        # Process target advisor fees
        target_fees = pd.DataFrame({
            'instrument': deals['deal_id'],
            'type': 'target',
            'advisor_code': deals['target_advisor_code'],
            'advisor_disclosed_fee_role': deals['target_advisor_disclosed_fee_role'],
            'advisor_disclosed_fee': deals['target_advisor_disclosed_fee'],
        })
        target_fees = target_fees.dropna(subset=['advisor_code'])
        
        # Process acquiror advisor fees
        acquiror_fees = pd.DataFrame({
            'instrument': deals['deal_id'],
            'type': 'acquiror',
            'advisor_code': deals['acquiror_advisor_code'],
            'advisor_disclosed_fee_role': deals['acquiror_advisor_disclosed_fee_role'],
            'advisor_disclosed_fee': deals['acquiror_advisor_disclosed_fee'],
        })
        acquiror_fees = acquiror_fees.dropna(subset=['advisor_code'])
        
        # Combine target and acquiror fees
        deals_fee = pd.concat([target_fees, acquiror_fees], ignore_index=True)
        
        # Get a DataFrame with just deal_id and the instrument column
        deals_no = pd.DataFrame({'instrument': deals['deal_id'], 'deal_id': deals['deal_id']})
        deals_no = deals_no.drop_duplicates()
        
        # Merge fees data with deal_id data
        deals_fee = deals_fee.merge(deals_no, on='instrument')[['deal_id', 'type', 'advisor_code', 'advisor_disclosed_fee_role', 'advisor_disclosed_fee']]
        
        logger.info(f"Processed data into {len(deals_fee)} advisor fee records")
        return deals_fee
    except Exception as e:
        logger.error(f"Error processing deals data: {e}")
        raise

def string_to_int(string, alphabet):
    """Convert string to integer using the given alphabet."""
    number = 0
    alpha_len = len(alphabet)
    for char in string[::-1]:
        number = number * alpha_len + alphabet.index(char)
    return number

def init_bet():
    """Initialize alphabet for ID generation."""
    alphabet = list("0123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
    new_alphabet = list(sorted(set(alphabet)))
    if len(new_alphabet) > 1:
        alphabet = new_alphabet
    return alphabet

def create_id(original=None, digit=8):
    """Create ID from original string."""
    ori_str = original if original is not None else uuid.uuid1().hex[-12:]
    s = string_to_int(ori_str, init_bet())
    return str(int(math.pow(10, digit - 1) + abs(hash(s)) % (10**(digit - 2))))

def _create_id(**kwargs):
    """Create ID from keyword arguments."""
    feature = copy.deepcopy(kwargs)
    s = hashlib.md5(json.dumps(feature).encode(encoding="utf-8")).hexdigest()
    return "{0}".format(create_id(original=s, digit=15))

def generate_id(row):
    """Generate ID for a row in the dataframe."""
    return _create_id(deal_id=row['deal_id'], type=row['type'], advisor_id=row['advisor_id'], advisor_role_id=row['advisor_role_id'])

def create_dummy_table(conn, source_table, target_table):
    """Create a dummy table by replicating the structure of another table."""
    try:
        cursor = conn.cursor()
        
        # Check if target table exists
        logger.info(f"Checking if {target_table} table exists...")
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            logger.info(f"{target_table} table already exists")
        else:
            logger.info(f"Creating {target_table} table with same structure as {source_table} table...")
            cursor.execute(f"CREATE TABLE {target_table} LIKE {source_table}")
            conn.commit()
            logger.info(f"Dummy table {target_table} created successfully")
    except Exception as e:
        logger.error(f"Error creating dummy table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def export_to_csv(df_advisor_fees, conn=None, test_mode=False):
    """Export advisor_fees data to CSV with backup."""
    try:
        today_date = datetime.now().strftime("%Y-%m-%d")
        
        # Set file prefix based on test mode
        file_prefix = "advisor_fees_test" if test_mode else "advisor_fees"
        
        # Remove any existing advisor_fees CSV files
        logger.info(f"Removing existing {file_prefix} CSV files...")
        for file in os.listdir("."):
            if file.startswith(file_prefix) and file.endswith(".csv"):
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except OSError as e:
                    logger.error(f"Error deleting file {file}: {e}")
        
        # Create a backup of the advisor_fees table from the database
        if conn:
            # Determine table name based on test mode
            table_name = "advisor_fees" + (TEST_TABLE_SUFFIX if test_mode else "")
            backup_query = f"SELECT * FROM {table_name}"
            df_backup = pd.read_sql(backup_query, conn)
            backup_filename = f"{file_prefix}_{today_date}.csv"
            df_backup.to_csv(backup_filename, index=False)
            logger.info(f"Created backup from database: {backup_filename}")
        
        # Export current advisor_fees data to CSV
        output_filename = f"{file_prefix}_{today_date}.csv"
        df_advisor_fees.to_csv(output_filename, index=False)
        logger.info(f"Exported data to {output_filename}")
        
        # Also create a standard advisor_fees.csv file
        standard_filename = f"{file_prefix}.csv"
        df_advisor_fees.to_csv(standard_filename, index=False)
        logger.info(f"Also created standard {standard_filename} file")
    except Exception as e:
        logger.error(f"Error exporting data to CSV: {e}")
        raise

def update_database(conn, df_advisor_fees, target_table):
    """Update the database with new data."""
    try:
        cursor = conn.cursor()
        
        # Truncate the target table
        logger.info(f"Truncating {target_table} table...")
        cursor.execute(f"TRUNCATE TABLE {target_table};")
        conn.commit()
        
        # Prepare for batch insert into target_table
        logger.info(f"Inserting {len(df_advisor_fees)} records into {target_table} table...")
        
        # Replace NaN values with None for proper NULL handling in MySQL
        df_advisor_fees = df_advisor_fees.replace({np.nan: None})
        
        # For numeric amounts, ensure NaN stays as NULL (don't convert to 0)
        if 'advisor_fee' in df_advisor_fees.columns:
            df_advisor_fees['advisor_fee'] = df_advisor_fees['advisor_fee'].replace('', np.nan)
        
        # Ensure proper NULL handling for string columns (empty strings become NULL)
        string_columns = ['deal_id', 'type', 'advisor_id', 'advisor_role_id', 'advisor_role_name', 'advisor_fees_id']
        for col in string_columns:
            if col in df_advisor_fees.columns:
                df_advisor_fees[col] = df_advisor_fees[col].replace('', np.nan)
        
        # Format date columns to 'YYYY-MM-DD' strings
        for col in df_advisor_fees.columns:
            if df_advisor_fees[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df_advisor_fees[col]):
                df_advisor_fees[col] = df_advisor_fees[col].dt.strftime('%Y-%m-%d')
        
        # Convert DataFrame to list of tuples for batch insert
        advisor_fees_columns = df_advisor_fees.columns.tolist()
        
        # Process values to ensure dates are formatted correctly
        advisor_fees_values = []
        for _, row in df_advisor_fees.iterrows():
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
            advisor_fees_values.append(processed_row)
        
        # Create placeholders for SQL query
        placeholders = ', '.join(['%s'] * len(advisor_fees_columns))
        columns = ', '.join(advisor_fees_columns)
        
        # Insert data in batches
        batch_size = 100 if TEST_MODE else 1000
        for i in range(0, len(advisor_fees_values), batch_size):
            batch = advisor_fees_values[i:i+batch_size]
            cursor.executemany(
                f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})",
                batch
            )
            conn.commit()
            logger.info(f"Inserted batch {i//batch_size + 1}/{(len(advisor_fees_values)-1)//batch_size + 1} into {target_table}")
        
        logger.info("Database update completed successfully")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def main(test_mode=False):
    """Main function to orchestrate the advisor fees data update process."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    try:
        logger.info(f"{mode_prefix} Starting advisor fees data update process")
        
        # Initialize Refinitiv API
        initialize_refinitiv()
        
        # Connect to database
        conn = connect_to_database()
        
        # Define source and target tables based on test mode
        source_table = "advisor_fees"
        target_table = "advisor_fees_2" if not TEST_MODE else "advisor_fees_2_test"
        
        # Create test tables if in test mode
        if TEST_MODE:
            logger.info(f"{mode_prefix} Creating test tables...")
            create_dummy_table(conn, "advisor_role", "advisor_role_test")
            create_dummy_table(conn, "advisor_fees", "advisor_fees_test")
            create_dummy_table(conn, "advisor_fees_2", "advisor_fees_2_test")
        else:
            # Create dummy table if it doesn't exist
            create_dummy_table(conn, source_table, "advisor_fees_2")
        
        # Get advisor role data from database
        roles, _ = get_data_from_database(conn)
        
        # Process deals data to get advisor fees
        deals_fee = process_deals_data()
        
        # Rename columns for consistency with database
        deals_fee.columns = ['deal_id', 'type', 'advisor_id', 'advisor_role_name', 'advisor_fee']
        
        # Merge with roles data to get role IDs
        roles = roles[['advisor_role_id', 'advisor_role_name']]
        deals_fee = deals_fee.merge(roles, on='advisor_role_name', how='left')
        
        # Prepare final dataset
        deals_fee = deals_fee[['deal_id', 'type', 'advisor_id', 'advisor_role_id', 'advisor_fee']]
        deals_fee['advisor_role_id'] = deals_fee['advisor_role_id'].fillna('NA')
        deals_fee['advisor_fees_id'] = deals_fee.apply(generate_id, axis=1)
        deals_fee = deals_fee.drop_duplicates()
        deals_fee = deals_fee[deals_fee['deal_id'] != '']
        
        # Export to CSV
        export_to_csv(deals_fee, conn, TEST_MODE)
        
        # Update database
        update_database(conn, deals_fee, target_table)
        
        # Close database connection
        conn.close()
        logger.info("Database connection closed")
        
        # Close Refinitiv session
        rd.close_session()
        logger.info("Refinitiv API session closed")
        
        logger.info(f"{mode_prefix} Advisor fees data update process completed successfully")
    except Exception as e:
        logger.error(f"{mode_prefix} Advisor fees data update process failed: {e}")
        try:
            # Close connections if error occurs
            if 'conn' in locals() and conn:
                conn.close()
                logger.info("Database connection closed")
            rd.close_session()
            logger.info("Refinitiv API session closed")
        except:
            pass
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Advisor Fees Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
