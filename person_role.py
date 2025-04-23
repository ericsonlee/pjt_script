#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Person Role Data Update Script

This script fetches person role data from Refinitiv and updates the MySQL database.
It creates a dummy person_roles_2 table for testing updates and exports data to CSV files.
"""

import os
import logging
import time
from datetime import datetime
import numpy as np
import pandas as pd
import mysql.connector
from tqdm import tqdm
import refinitiv.data as rd
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

def get_company_data(conn):
    """Get company data from the database."""
    try:
        logger.info("Fetching company data from database...")
        
        # Determine table names based on test mode
        company_table = "company_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        position_code_table = "person_position_code" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        
        # Get company data
        query = f"SELECT * FROM {company_table}"
        master_mapping = pd.read_sql(query, conn)
        
        # Get position code data
        query = f"SELECT * FROM {position_code_table}"
        codified_roles_df = pd.read_sql(query, conn)
        
        logger.info(f"Retrieved {len(master_mapping)} companies and {len(codified_roles_df)} position codes")
        
        return master_mapping, codified_roles_df
    except Exception as e:
        logger.error(f"Error fetching company data: {e}")
        raise

def load_officer_codes():
    """Load officer codes from Excel file."""
    try:
        logger.info("Loading officer codes from Excel file...")
        officer_codes = pd.read_excel('officer_codes_new.xlsx', index_col=None)
        
        od_details_codes = officer_codes[officer_codes['category']=='details']['trcode'].tolist()
        
        od_position_codes = officer_codes[officer_codes['category']=='position']['trcode'].tolist()
        od_position_names = ['PermID'] + officer_codes[officer_codes['category']=='position']['name'].tolist()
        
        od_position_code_codes = officer_codes[officer_codes['category']=='position_code']['trcode'].tolist()
        od_position_code_names = ['PermID'] + officer_codes[officer_codes['category']=='position_code']['name'].tolist()
        
        logger.info("Officer codes loaded successfully")
        return od_details_codes, od_position_codes, od_position_names, od_position_code_codes, od_position_code_names
    except Exception as e:
        logger.error(f"Error loading officer codes: {e}")
        raise

def fetch_data(ticker, fields, parameters=None):
    """Fetch data from Refinitiv with retry mechanism."""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            if parameters:
                return rd.get_data(universe=ticker, fields=fields, parameters=parameters)
            else:
                return rd.get_data(universe=ticker, fields=fields)
        except Exception as e:
            logger.error(f"Error fetching data for ticker {ticker}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Maximum retries reached for ticker {ticker}")
                raise

def process_company_data(unpulled_tickers, od_details_codes, od_position_codes, od_position_names, 
                         od_position_code_codes, od_position_code_names, codified_roles_df):
    """Process company data to get person role information."""
    try:
        logger.info(f"Processing data for {len(unpulled_tickers)} companies...")
        
        # In test mode, limit the number of companies to process
        if TEST_MODE and len(unpulled_tickers) > 10:
            logger.info(f"TEST MODE: Limiting from {len(unpulled_tickers)} to 10 companies")
            unpulled_tickers = unpulled_tickers[:10]
        
        # Split list into chunks to avoid API limitations
        max_companies = 50 if TEST_MODE else 150
        def split_list(l, n):
            return [l[x: x+n] for x in range(0, len(l), n)]
        
        tickers_chunks = split_list(unpulled_tickers, max_companies)
        logger.info(f"Split into {len(tickers_chunks)} chunks of max {max_companies} companies each")
        
        # Initialize lists to store results
        all_details_df = []
        all_position_df = []
        all_position_codes_df = []
        
        # Process each chunk of tickers
        for chunk_idx, tickers_chunk in enumerate(tickers_chunks):
            logger.info(f"Processing chunk {chunk_idx+1}/{len(tickers_chunks)} with {len(tickers_chunk)} companies...")
            
            # Fetch details data for the entire chunk
            try:
                logger.info(f"Fetching details data for chunk {chunk_idx+1}...")
                data_details = fetch_data(tickers_chunk, od_details_codes)
                data_details = data_details.rename(columns={'Instrument': 'PermID'})
                all_details_df.append(data_details)
                logger.info(f"Successfully fetched details data for chunk {chunk_idx+1}")
            except Exception as e:
                logger.error(f"Error fetching details data for chunk {chunk_idx+1}: {e}")
            
            # Fetch position data for the entire chunk
            try:
                logger.info(f"Fetching position data for chunk {chunk_idx+1}...")
                data_position = fetch_data(tickers_chunk, od_position_codes)
                data_position.columns = od_position_names
                all_position_df.append(data_position)
                logger.info(f"Successfully fetched position data for chunk {chunk_idx+1}")
            except Exception as e:
                logger.error(f"Error fetching position data for chunk {chunk_idx+1}: {e}")
            
            # Fetch position code data for the entire chunk
            try:
                logger.info(f"Fetching position code data for chunk {chunk_idx+1}...")
                data_position_code = fetch_data(tickers_chunk, od_position_code_codes)
                data_position_code.columns = od_position_code_names
                all_position_codes_df.append(data_position_code)
                logger.info(f"Successfully fetched position code data for chunk {chunk_idx+1}")
            except Exception as e:
                logger.error(f"Error fetching position code data for chunk {chunk_idx+1}: {e}")
        
        # Combine all fetched data
        logger.info("Combining all fetched data...")
        details_df = pd.concat(all_details_df, ignore_index=True) if all_details_df else pd.DataFrame()
        position_df = pd.concat(all_position_df, ignore_index=True) if all_position_df else pd.DataFrame()
        position_codes_df = pd.concat(all_position_codes_df, ignore_index=True) if all_position_codes_df else pd.DataFrame()
        
        # Process the data
        logger.info("Processing position data...")
        position_df = position_df.merge(
            details_df[['PermID', 'OD Officer Rank', 'Person PermID', 'Officer PermID']], 
            how='left', 
            on=['PermID', 'OD Officer Rank']
        )
        
        position_df_with_codes_exploded = position_df.merge(
            position_codes_df.drop(columns=['Officer PermID']).rename(columns={'Officer Position Order': 'Position Order'}), 
            how='outer', 
            on=['PermID', 'Position Order', 'Person PermID']
        )
        
        # Clean up data
        logger.info("Cleaning up data...")
        position_df_with_codes_exploded = position_df_with_codes_exploded.drop_duplicates()
        position_df_with_codes_exploded = position_df_with_codes_exploded.dropna(subset=['Person PermID'])
        
        # Create person_roles dataframe
        logger.info("Creating person_roles dataframe...")
        person_roles_df = position_df_with_codes_exploded[[
            'PermID', 'Person PermID', 'Position Order', 'Position Name', 'Position Code'
        ]].drop_duplicates()
        
        # Merge with codified roles
        logger.info("Merging with codified roles...")
        person_roles_df = person_roles_df.merge(
            codified_roles_df[['position_code', 'position_code_id']], 
            how='left', 
            left_on='Position Code', 
            right_on='position_code'
        )
        
        # Rename columns
        person_roles_df = person_roles_df.rename(columns={
            'PermID': 'company_id',
            'Person PermID': 'person_id',
            'Position Order': 'position_order',
            'Position Name': 'position_name',
            'Position Code': 'position_code_original'
        })
        
        # Generate person_role_id
        logger.info("Generating person_role IDs...")
        person_roles_df['person_role_id'] = person_roles_df.apply(
            lambda row: f"{row['company_id']}_{row['person_id']}_{row['position_order']}",
            axis=1
        )
        
        # Add timestamp fields
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        person_roles_df['create_time'] = current_time
        person_roles_df['update_time'] = current_time
        person_roles_df['flag'] = 1
        
        # Clean up data
        logger.info("Final data cleanup...")
        # Replace empty strings with NaN for proper NULL handling
        person_roles_df = person_roles_df.replace('', np.nan)
        
        # Drop rows with missing essential data
        person_roles_df = person_roles_df.dropna(subset=['person_role_id', 'company_id', 'person_id'])
        
        logger.info(f"Final person_roles dataframe has {len(person_roles_df)} rows")
        
        return person_roles_df
    except Exception as e:
        logger.error(f"Error processing company data: {e}")
        raise

def create_dummy_table(conn):
    """Create a dummy person_roles_2 table for testing updates."""
    try:
        cursor = conn.cursor()
        
        # Determine table names based on test mode
        source_table = "person_roles"
        target_table = "person_roles_2" if not TEST_MODE else "person_roles_2_test"
        
        # Create test tables if in test mode
        if TEST_MODE:
            # Create test tables for original tables first
            logger.info("Creating test tables for original tables...")
            for source, target in [
                ("person_roles", "person_roles_test"),
                ("person_position_code", "person_position_code_test")
            ]:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{target}'")
                table_exists = cursor.fetchone()
                
                if not table_exists:
                    logger.info(f"Creating {target} table...")
                    cursor.execute(f"CREATE TABLE {target} LIKE {source}")
                    conn.commit()
                    
                    # Copy data
                    logger.info(f"Copying data from {source} to {target}...")
                    cursor.execute(f"INSERT INTO {target} SELECT * FROM {source}")
                    conn.commit()
                    logger.info(f"{target} table created and populated successfully")
                else:
                    logger.info(f"{target} table already exists")
        
        # Check if target table exists
        logger.info(f"Checking if {target_table} table exists...")
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.info(f"Creating {target_table} table...")
            cursor.execute(f"CREATE TABLE {target_table} LIKE {source_table}")
            conn.commit()
            logger.info(f"{target_table} table created successfully")
        else:
            logger.info(f"{target_table} table already exists")
        
        conn.commit()
        logger.info("Dummy table created successfully")
    except Exception as e:
        logger.error(f"Error creating dummy table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def export_to_csv(df_person_roles, conn=None, test_mode=False):
    """Export person_roles data to CSV with backup."""
    try:
        # Generate filename with today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Set file prefix based on test mode
        file_prefix = "person_roles_test" if test_mode else "person_roles"
        
        # Remove any existing person_roles CSV files
        logger.info(f"Removing existing {file_prefix} CSV files...")
        for file in os.listdir("."):
            if file.startswith(file_prefix) and file.endswith(".csv"):
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except OSError as e:
                    logger.error(f"Error deleting file {file}: {e}")
        
        # Create a backup of the person_roles table from the database
        if conn:
            # Determine table name based on test mode
            table_name = "person_roles" + (TEST_TABLE_SUFFIX if test_mode else "")
            backup_query = f"SELECT * FROM {table_name}"
            df_backup = pd.read_sql(backup_query, conn)
            backup_filename = f"{file_prefix}_{today_date}.csv"
            df_backup.to_csv(backup_filename, index=False)
            logger.info(f"Created backup from database: {backup_filename}")
        
        # Export current person_roles data to CSV
        output_filename = f"{file_prefix}_{today_date}.csv"
        df_person_roles.to_csv(output_filename, index=False)
        logger.info(f"Exported data to {output_filename}")
        
        # Also create a standard person_roles.csv file
        standard_filename = f"{file_prefix}.csv"
        df_person_roles.to_csv(standard_filename, index=False)
        logger.info(f"Also created standard {standard_filename} file")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

def update_database(conn, df_person_roles):
    """Update the database with new data."""
    try:
        cursor = conn.cursor()
        
        # Determine target table based on test mode
        target_table = "person_roles_2" if not TEST_MODE else "person_roles_2_test"
        
        # Truncate the target table
        logger.info(f"Truncating {target_table} table...")
        cursor.execute(f"TRUNCATE TABLE {target_table};")
        conn.commit()
        
        # Replace NaN values with None for proper NULL handling in MySQL
        df_person_roles = df_person_roles.replace({np.nan: None})
        
        # Ensure proper NULL handling for string columns (empty strings become NULL)
        string_columns = ['person_id', 'company_id', 'position_name', 'position_code_original', 'person_role_id']
        for col in string_columns:
            if col in df_person_roles.columns:
                df_person_roles[col] = df_person_roles[col].replace('', np.nan)
        
        # Prepare for batch insert
        logger.info(f"Inserting {len(df_person_roles)} rows into {target_table} table...")
        batch_size = 100 if TEST_MODE else 1000
        
        # Prepare for batch insert
        cols = ', '.join(f"`{col}`" for col in df_person_roles.columns)
        placeholders = ', '.join(['%s'] * len(df_person_roles.columns))
        
        # Process data in batches
        for start in range(0, len(df_person_roles), batch_size):
            end = min(start + batch_size, len(df_person_roles))
            batch = df_person_roles.iloc[start:end]
            
            # Convert to list of tuples
            data_tuples = []
            for row in batch.itertuples(index=False, name=None):
                processed_row = []
                for v in row:
                    if pd.isna(v):
                        processed_row.append(None)
                    elif isinstance(v, (pd.Timestamp, datetime)):
                        processed_row.append(v.strftime('%Y-%m-%d'))
                    elif hasattr(v, 'item'):
                        processed_row.append(v.item())
                    else:
                        processed_row.append(v)
                data_tuples.append(tuple(processed_row))
            
            # Insert batch
            cursor.executemany(
                f"INSERT INTO {target_table} ({cols}) VALUES ({placeholders})",
                data_tuples
            )
            conn.commit()
            logger.info(f"Inserted rows {start} to {end}")
        
        logger.info("Database update completed successfully")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def main(test_mode=False):
    """Main function to orchestrate the person role data update process."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    start_time = time.time()
    logger.info(f"{mode_prefix} Starting person role data update process")
    
    try:
        # Initialize connections
        initialize_refinitiv()
        conn = connect_to_database()
        
        # Create dummy table
        create_dummy_table(conn)
        
        # Get company data from database
        master_mapping, codified_roles_df = get_company_data(conn)
        
        # Load officer codes
        od_details_codes, od_position_codes, od_position_names, od_position_code_codes, od_position_code_names = load_officer_codes()
        
        # Get list of company PermIDs to process
        unpulled_tickers = master_mapping['permid'].dropna().astype(str).tolist()
        
        # Process company data
        df_person_roles = process_company_data(
            unpulled_tickers, od_details_codes, od_position_codes, od_position_names, 
            od_position_code_codes, od_position_code_names, codified_roles_df
        )
        
        # Export to CSV
        export_to_csv(df_person_roles, conn, TEST_MODE)
        
        # Update database
        update_database(conn, df_person_roles)
        
        # Close connections
        conn.close()
        logger.info("Database connection closed")
        
        rd.close_session()
        logger.info("Refinitiv session closed")
        
        # Calculate and log execution time
        execution_time = time.time() - start_time
        logger.info(f"{mode_prefix} Person role data update process completed successfully in {execution_time:.2f} seconds")
    except Exception as e:
        logger.error(f"{mode_prefix} Person role data update process failed: {e}")
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
    
    parser = argparse.ArgumentParser(description="Person Role Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
