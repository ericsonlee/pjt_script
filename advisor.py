#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Advisor Data Update Script
--------------------------
This script fetches advisor data from Refinitiv, processes it, and updates a MySQL database.
It's designed to run as an automated Windows task.
"""

import os
import time
import logging
from datetime import datetime
import pandas as pd
import refinitiv.data as rd
import numpy as np
from tqdm import tqdm
import mysql.connector
from script.db_utils import connect_to_database, initialize_refinitiv

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

def process(df):
    """Process the fetched advisor data."""
    try:
        logger.info("Processing advisor data...")
        
        # Rename columns to match expected format
        df.columns = ['instrument', 'deal_no', 'target_advisor_name', 'target_advisor_code', 
                     'acquiror_advisor_name', 'acquiror_advisor_code',
                     'parent_target_advisor_name', 'parent_target_advisor_code', 
                     'parent_acquiror_advisor_name', 'parent_acquiror_advisor_code']
        
        # Create advisor dataframe
        advisor = pd.DataFrame()
        advisor['name'] = list(df['target_advisor_name']) + list(df['acquiror_advisor_name']) + list(df['parent_target_advisor_name']) + list(df['parent_acquiror_advisor_name'])
        advisor['code'] = list(df['target_advisor_code']) + list(df['acquiror_advisor_code']) + list(df['parent_target_advisor_code']) + list(df['parent_acquiror_advisor_code'])
        advisor = advisor[advisor['name'] != '']
        advisor = advisor.dropna(subset=['code']).drop_duplicates().reset_index(drop=True)
        
        # Create parent dataframe
        parent = pd.DataFrame()
        parent['code'] = list(df['target_advisor_code']) + list(df['acquiror_advisor_code'])
        parent['parent_code'] = list(df['parent_target_advisor_code']) + list(df['parent_acquiror_advisor_code'])
        parent = parent.dropna().drop_duplicates().reset_index(drop=True)
        parent = parent[parent['code'] != '']
        
        # Merge advisor and parent information
        advisor = advisor.merge(parent, how='left', on='code').merge(advisor, how='left', left_on='parent_code', right_on='code')
        advisor.columns = ['name', 'code', 'parent_code', 'parent_name', 'tmp']
        advisor = advisor.drop(columns=['tmp'])
        
        # Add timestamp and flag fields
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        advisor['create_time'] = current_time
        advisor['update_time'] = current_time
        advisor['flag'] = 1
        
        logger.info(f"Processed advisor data into {len(advisor)} rows")
        return advisor
    except Exception as e:
        logger.error(f"Error processing advisor data: {e}")
        raise

def fetch_data(year):
    """Fetch advisor data from Refinitiv for a specific year with retry mechanism."""
    fields = [
        "TR.MNASDCDEALNUMBER",
        "TR.MNATARGETFINADVISOR",
        "TR.MNATARGETFINADVISOR.ADVISORCODE",
        "TR.MNAACQUIRORFINADVISOR",
        "TR.MNAACQUIRORFINADVISOR.ADVISORCODE",
        "TR.MNATARGETFINADVISORPARENT",
        "TR.MNATARGETFINADVISORPARENT.ADVISORCODE",
        "TR.MNAACQUIRORFINADVISORPARENT",
        "TR.MNAACQUIRORFINADVISORPARENT.ADVISORCODE",
    ]
    
    # If in test mode, limit the query to fewer years
    if TEST_MODE and year < datetime.now().year - 2:
        logger.info(f"TEST MODE: Skipping year {year} to limit data volume")
        return pd.DataFrame()  # Return empty DataFrame for older years in test mode
    
    query_string = f"SCREEN(U(IN(DEALS)),BETWEEN(TR.MNAANNDATE,{str(year)}0101,{str(year)}1231))"
    logger.info(f"Fetching advisor data for year {year}...")
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for retry in range(max_retries):
        try:
            deal = rd.get_data(
                universe=query_string,
                fields=fields,
                parameters={"Curn": "USD"}
            )
            
            # In test mode, limit the number of records
            if TEST_MODE and len(deal) > 20:
                logger.info(f"TEST MODE: Limiting from {len(deal)} to 20 records for year {year}")
                deal = deal.head(20)
                
            logger.info(f"Successfully fetched advisor data for year {year} with {len(deal)} records")
            return deal
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching data for year {year}: {error_msg}")
            if retry < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Maximum retries reached for year {year}")
                return pd.DataFrame()  # Return empty DataFrame if all retries fail

def export_to_csv(advisor_df, conn=None, test_mode=False):
    """Export advisor data to CSV with backup."""
    try:
        # Generate filename with today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Set file prefix based on test mode
        file_prefix = "advisor_test" if test_mode else "advisor"
        
        # Remove any existing advisor CSV files
        logger.info(f"Removing existing {file_prefix} CSV files...")
        for file in os.listdir("."):
            if file.startswith(file_prefix) and file.endswith(".csv"):
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except OSError as e:
                    logger.error(f"Error deleting file {file}: {e}")
        
        # Create a backup of the advisor table from the database
        if conn:
            # Determine table name based on test mode
            table_name = "advisor" + (TEST_TABLE_SUFFIX if test_mode else "")
            backup_query = f"SELECT * FROM {table_name}"
            df_backup = pd.read_sql(backup_query, conn)
            backup_filename = f"{file_prefix}_{today_date}.csv"
            df_backup.to_csv(backup_filename, index=False)
            logger.info(f"Created backup from database: {backup_filename}")
        
        # Export current data to CSV with date
        dated_filename = f"{file_prefix}_{today_date}.csv"
        advisor_df.to_csv(dated_filename, index=False)
        logger.info(f"Exported data to {dated_filename}")
        
        # Also create a standard advisor.csv file
        standard_filename = f"{file_prefix}.csv"
        advisor_df.to_csv(standard_filename, index=False)
        logger.info(f"Also created standard {standard_filename} file")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

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
            
            # Copy data if needed
            logger.info(f"Copying data from {source_table} to {target_table}...")
            cursor.execute(f"INSERT INTO {target_table} SELECT * FROM {source_table}")
            conn.commit()
            logger.info(f"Data copied to {target_table} successfully")
    except Exception as e:
        logger.error(f"Error creating dummy table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def truncate_table(conn, table_name):
    """Truncate the specified table before updating it with new data."""
    try:
        cursor = conn.cursor()
        
        # Truncate the table
        logger.info(f"Truncating {table_name} table...")
        truncate_query = f"TRUNCATE TABLE {table_name};"
        cursor.execute(truncate_query)
        
        conn.commit()
        logger.info(f"{table_name} table truncated successfully")
    except Exception as e:
        logger.error(f"Error truncating {table_name} table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def update_database(conn, advisor_df, target_table):
    """Update the database with new advisor data, ignoring entries with the same index."""
    try:
        cursor = conn.cursor()
        
        # Prepare for insert/update
        batch_size = 1000 if not TEST_MODE else 100  # Smaller batch size for test mode
        cols = ', '.join(f"`{col}`" for col in advisor_df.columns)
        placeholders = ', '.join(['%s'] * len(advisor_df.columns))
        
        # Use INSERT IGNORE to skip entries with the same primary key (code)
        insert_query = f"""
        INSERT IGNORE INTO {target_table} ({cols}) 
        VALUES ({placeholders})
        """
        
        # Replace NaN values with None for proper NULL handling in MySQL
        advisor_df = advisor_df.replace({np.nan: None})
        
        # Ensure proper NULL handling for string columns (empty strings become NULL)
        string_columns = ['name', 'code', 'parent_code', 'parent_name']
        for col in string_columns:
            if col in advisor_df.columns:
                advisor_df[col] = advisor_df[col].replace('', np.nan)
        
        # Insert data in batches with progress tracking
        total_rows = len(advisor_df)
        logger.info(f"Inserting {total_rows} rows in batches of {batch_size} (ignoring duplicates)...")
        
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = advisor_df.iloc[start:end]
            
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
            logger.info(f"Processed rows {start} to {end} ({end-start} rows)")
        
        logger.info(f"Database update completed successfully. Total rows processed: {total_rows}")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def main(test_mode=False):
    """Main function to orchestrate the advisor data update process."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    start_time = time.time()
    logger.info(f"{mode_prefix} Starting advisor data update process")
    
    try:
        # Initialize connections
        initialize_refinitiv()
        conn = connect_to_database()
        
        # Define source and target tables based on test mode
        source_table = "advisor"
        target_table = "advisor_2" if not TEST_MODE else "advisor_2_test"
        
        # Create test tables if in test mode
        if TEST_MODE:
            logger.info(f"{mode_prefix} Creating test tables...")
            create_dummy_table(conn, "advisor", "advisor_test")
            create_dummy_table(conn, "advisor_2", "advisor_2_test")
        else:
            # Create dummy table for testing
            create_dummy_table(conn, source_table, "advisor_2")
        
        # Fetch existing advisor data from database
        logger.info(f"{mode_prefix} Fetching existing advisor data from database...")
        query = f"SELECT * FROM {source_table}"
        df_roles = pd.read_sql(query, conn)
        logger.info(f"{mode_prefix} Fetched {len(df_roles)} existing advisor records from database")
        
        # Fetch data for each year
        all_advisors = []
        current_year = datetime.now().year
        
        # In test mode, limit the years to process
        start_year = current_year - 2 if TEST_MODE else 1985
        
        # Process years in reverse order (most recent first) to prioritize newer data
        for year in range(current_year, start_year, -1):
            deal_data = fetch_data(year)
            if not deal_data.empty:
                processed_data = process(deal_data)
                if not processed_data.empty:
                    all_advisors.append(processed_data)
                    logger.info(f"{mode_prefix} Added {len(processed_data)} advisor records from year {year}")
        
        # Combine all data
        if all_advisors:
            # Concatenate all advisor data
            advisor_df = pd.concat(all_advisors, ignore_index=True)
            
            # Remove duplicates, keeping the first occurrence (which will be from more recent years)
            advisor_df = advisor_df.drop_duplicates(subset=['code'])
            
            # Rename columns to match database schema
            advisor_df = advisor_df.rename(columns={
                'name': 'advisor_name',
                'code': 'advisor_id',
                'parent_code': 'advisor_parent_id',
                'parent_name': 'advisor_parent_name'
            })
            
            logger.info(f"{mode_prefix} Combined advisor data: {len(advisor_df)} unique advisors")
            
            # Create advisor_df_new by combining existing and new data
            logger.info(f"{mode_prefix} Creating combined advisor dataset...")
            advisor_df_new = pd.concat([df_roles, advisor_df], axis=0).drop_duplicates(subset=['advisor_id'])
            
            # Update timestamps for new records
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # For records that don't have create_time (new records), set it to current time
            advisor_df_new['create_time'] = advisor_df_new['create_time'].fillna(current_time)
            # Update the update_time for all records
            advisor_df_new['update_time'] = current_time
            # Set flag to 1 for all records
            advisor_df_new['flag'] = 1
            
            logger.info(f"{mode_prefix} Combined dataset contains {len(advisor_df_new)} unique advisors")
            
            # Export to CSV with backup
            export_to_csv(advisor_df_new, conn, TEST_MODE)
            
            # Truncate target table before updating
            truncate_table(conn, target_table)
            
            # Update database
            update_database(conn, advisor_df_new, target_table)
        else:
            logger.error(f"{mode_prefix} No advisor data was fetched")
        
        # Close connections
        if conn:
            conn.close()
            logger.info("Database connection closed")
        
        rd.close_session()
        logger.info("Refinitiv session closed")
        
        # Calculate and log execution time
        execution_time = time.time() - start_time
        logger.info(f"{mode_prefix} Advisor data update process completed successfully in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"{mode_prefix} Advisor data update process failed: {e}")
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
    import argparse
    
    parser = argparse.ArgumentParser(description="Advisor Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
