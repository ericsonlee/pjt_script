#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Person History Data Update Script
---------------------------------
This script fetches person and position data from Refinitiv, processes it, and updates a MySQL database.
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

def get_company_data(conn):
    """Get company data from the database."""
    try:
        logger.info("Fetching company data from database...")
        
        # Determine table names based on test mode
        company_table = "company_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        position_code_table = "person_position_code" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        
        # Read company table
        query = f"SELECT * FROM {company_table}"
        master_mapping = pd.read_sql(query, conn)
        
        # Read position code table
        query = f"SELECT * FROM {position_code_table}"
        codified_roles_df = pd.read_sql(query, conn)
        
        logger.info(f"Retrieved {len(master_mapping)} companies and {len(codified_roles_df)} position codes")
        return master_mapping, codified_roles_df
    except Exception as e:
        logger.error(f"Error getting company data: {e}")
        raise

def load_officer_codes():
    """Load officer codes from Excel file."""
    try:
        logger.info("Loading officer codes from Excel file...")
        officer_codes = pd.read_excel('officer_codes_new.xlsx', index_col=None)
        
        # Process details codes
        od_details_codes = officer_codes[officer_codes['category']=='details']['trcode'].apply(
            lambda x: x.replace('ODRnk=R1:R100','ODRnk=All')).tolist()[:]
        
        # Process position codes
        od_position_codes = officer_codes[officer_codes['category']=='position']['trcode'].apply(
            lambda x: x.replace('ODRnk=R1:R100','ODRnk=All')).tolist()
        od_position_names = ['PermID'] + officer_codes[officer_codes['category']=='position']['name'].tolist()
        od_position_names.append('Person PermID')
        od_position_codes.append('TR.ODOFFICERPOSITIONORDER(ODPOSITIONORDER=0:100,ODSTATUS=ACTIVE:INACTIVE,ODRNK=ALL).PERSONID')
        
        # Process position code codes
        od_position_code_codes = officer_codes[officer_codes['category']=='position_code']['trcode'].apply(
            lambda x: x.replace('ODRnk=R1:R100','ODRnk=All')).tolist()
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
    
    for retry in range(max_retries):
        try:
            if parameters:
                data = rd.get_data(universe=ticker, fields=fields, parameters=parameters)
            else:
                data = rd.get_data(universe=ticker, fields=fields)
            return data
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching data for ticker {ticker}: {error_msg}")
            if retry < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Maximum retries reached for ticker {ticker}")
                raise

def process_company_data(unpulled_tickers, od_details_codes, od_position_codes, od_position_names, 
                         od_position_code_codes, od_position_code_names, codified_roles_df):
    """Process company data to get person and position information."""
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
        
        # Initialize dataframes to store results
        details_df_list = []
        position_codes_df_list = []
        position_df_list = []
        
        # Process each chunk of tickers
        for chunk_idx, tickers_chunk in enumerate(tqdm(tickers_chunks, desc='Processing chunks')):
            logger.info(f"Processing chunk {chunk_idx+1}/{len(tickers_chunks)} with {len(tickers_chunk)} companies...")
            
            # Fetch details data for the entire chunk
            try:
                logger.info(f"Fetching details for chunk {chunk_idx+1}...")
                data_details = fetch_data(tickers_chunk, od_details_codes)
                data_details = data_details.rename(columns={'Instrument': 'PermID'})
                details_df_list.append(data_details)
                logger.info(f"Retrieved {len(data_details)} rows of details data")
            except Exception as e:
                logger.error(f"Error fetching details for chunk {chunk_idx+1}: {e}")
            
            # Fetch position data for the entire chunk
            try:
                logger.info(f"Fetching positions for chunk {chunk_idx+1}...")
                data_positions = fetch_data(tickers_chunk, od_position_codes)
                data_positions.columns = od_position_names
                position_df_list.append(data_positions)
                logger.info(f"Retrieved {len(data_positions)} rows of position data")
            except Exception as e:
                logger.error(f"Error fetching positions for chunk {chunk_idx+1}: {e}")
            
            # Fetch position code data for the entire chunk
            try:
                logger.info(f"Fetching position codes for chunk {chunk_idx+1}...")
                data_position_codes = fetch_data(tickers_chunk, od_position_code_codes)
                data_position_codes.columns = od_position_code_names
                position_codes_df_list.append(data_position_codes)
                logger.info(f"Retrieved {len(data_position_codes)} rows of position code data")
            except Exception as e:
                logger.error(f"Error fetching position codes for chunk {chunk_idx+1}: {e}")
            
            # Export intermediate results for debugging
            if details_df_list:
                temp_details_df = pd.concat(details_df_list, ignore_index=True)
                logger.info(f"After chunk {chunk_idx+1}, details_df has {len(temp_details_df)} rows")
                        
        # Combine all data from chunks
        logger.info("Combining all fetched data...")
        details_df = pd.concat(details_df_list, ignore_index=True) if details_df_list else pd.DataFrame()
        position_df = pd.concat(position_df_list, ignore_index=True) if position_df_list else pd.DataFrame()
        position_codes_df = pd.concat(position_codes_df_list, ignore_index=True) if position_codes_df_list else pd.DataFrame()
        
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
        
        # Create person dataframe
        logger.info("Creating person dataframe...")
        person_df = position_df_with_codes_exploded[['Person PermID', 'Person Name', 'Person Age']].drop_duplicates()
        person_df = person_df.rename(columns={
            'Person PermID': 'person_id',
            'Person Name': 'person_name',
            'Person Age': 'person_age'
        })
        
        # Add timestamp fields
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        person_df['create_time'] = current_time
        person_df['update_time'] = current_time
        person_df['flag'] = 1
        
        # Create history dataframe
        logger.info("Creating history dataframe...")
        history_df = position_df_with_codes_exploded[[
            'PermID', 'Person PermID', 'Position Order', 'Position Name', 'Position Code', 
            'Position Start Date', 'Position End Date', 'Position Status'
        ]].drop_duplicates()
        
        # Merge with codified roles
        logger.info("Merging with codified roles...")
        history_df = history_df.merge(
            codified_roles_df[['position_code', 'position_code_id']], 
            how='left', 
            left_on='Position Code', 
            right_on='position_code'
        )
        
        # Rename columns
        history_df = history_df.rename(columns={
            'PermID': 'company_id',
            'Person PermID': 'person_id',
            'Position Order': 'position_order',
            'Position Name': 'position_name',
            'Position Code': 'position_code_original',
            'Position Start Date': 'start_date',
            'Position End Date': 'end_date',
            'Position Status': 'status'
        })
        
        # Add timestamp fields
        history_df['create_time'] = current_time
        history_df['update_time'] = current_time
        history_df['flag'] = 1
        
        # Generate history_id
        logger.info("Generating history IDs...")
        history_df['history_id'] = history_df.apply(
            lambda row: f"{row['company_id']}_{row['person_id']}_{row['position_order']}",
            axis=1
        )
        
        # Clean up data
        logger.info("Final data cleanup...")
        # Replace empty strings with NaN for proper NULL handling
        person_df = person_df.replace('', np.nan)
        history_df = history_df.replace('', np.nan)
        
        # Convert date columns to datetime
        for col in ['start_date', 'end_date']:
            if col in history_df.columns:
                history_df[col] = pd.to_datetime(history_df[col], errors='coerce')
        
        # Drop rows with missing essential data
        person_df = person_df.dropna(subset=['person_id'])
        history_df = history_df.dropna(subset=['history_id', 'company_id', 'person_id'])
        
        logger.info(f"Final person dataframe has {len(person_df)} rows")
        logger.info(f"Final history dataframe has {len(history_df)} rows")
        
        return person_df, history_df
    except Exception as e:
        logger.error(f"Error processing company data: {e}")
        raise

def create_dummy_tables(conn):
    """Create dummy person_2 and person_history_2 tables for testing updates."""
    try:
        cursor = conn.cursor()
        
        # Determine table names based on test mode
        source_person = "person"
        source_history = "person_history"
        target_person = "person_2" if not TEST_MODE else "person_2_test"
        target_history = "person_history_2" if not TEST_MODE else "person_history_2_test"
        
        # Create test tables if in test mode
        if TEST_MODE:
            # Create test tables for original tables first
            logger.info("Creating test tables for original tables...")
            for source, target in [
                ("person", "person_test"),
                ("person_history", "person_history_test"),
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
        
        # Check if person_2 table exists
        logger.info(f"Checking if {target_person} table exists...")
        cursor.execute(f"SHOW TABLES LIKE '{target_person}'")
        person_table_exists = cursor.fetchone()
        
        if not person_table_exists:
            logger.info(f"Creating {target_person} table...")
            cursor.execute(f"CREATE TABLE {target_person} LIKE {source_person}")
            conn.commit()
            logger.info(f"{target_person} table created successfully")
        else:
            logger.info(f"{target_person} table already exists")
        
        # Check if person_history_2 table exists
        logger.info(f"Checking if {target_history} table exists...")
        cursor.execute(f"SHOW TABLES LIKE '{target_history}'")
        history_table_exists = cursor.fetchone()
        
        if not history_table_exists:
            logger.info(f"Creating {target_history} table...")
            cursor.execute(f"CREATE TABLE {target_history} LIKE {source_history}")
            conn.commit()
            logger.info(f"{target_history} table created successfully")
        else:
            logger.info(f"{target_history} table already exists")
        
        conn.commit()
        logger.info("Dummy tables created successfully")
    except Exception as e:
        logger.error(f"Error creating dummy tables: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def export_to_csv(df_person, df_history, conn=None, test_mode=False):
    """Export person and person_history data to CSV with backup."""
    try:
        # Generate filename with today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Set file prefix based on test mode
        person_prefix = "person_test" if test_mode else "person"
        history_prefix = "person_history_test" if test_mode else "person_history"
        
        # Remove any existing person CSV files
        logger.info(f"Removing existing {person_prefix} and {history_prefix} CSV files...")
        for file in os.listdir("."):
            if (file.startswith(person_prefix) or file.startswith(history_prefix)) and file.endswith(".csv"):
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except OSError as e:
                    logger.error(f"Error deleting file {file}: {e}")
        
        # Create backups from the database
        if conn:
            # Determine table names based on test mode
            person_table = "person" + (TEST_TABLE_SUFFIX if test_mode else "")
            history_table = "person_history" + (TEST_TABLE_SUFFIX if test_mode else "")
            
            # Backup person table
            backup_query = f"SELECT * FROM {person_table}"
            df_person_backup = pd.read_sql(backup_query, conn)
            person_backup_filename = f"{person_prefix}_{today_date}.csv"
            df_person_backup.to_csv(person_backup_filename, index=False)
            logger.info(f"Created backup from database: {person_backup_filename}")
            
            # Backup person_history table
            backup_query = f"SELECT * FROM {history_table}"
            df_history_backup = pd.read_sql(backup_query, conn)
            history_backup_filename = f"{history_prefix}_{today_date}.csv"
            df_history_backup.to_csv(history_backup_filename, index=False)
            logger.info(f"Created backup from database: {history_backup_filename}")
        
        # Export current person data to CSV
        person_filename = f"{person_prefix}.csv"
        df_person.to_csv(person_filename, index=False)
        logger.info(f"Exported person data to {person_filename}")
        
        # Export current person_history data to CSV
        history_filename = f"{history_prefix}.csv"
        df_history.to_csv(history_filename, index=False)
        logger.info(f"Exported person_history data to {history_filename}")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

def update_database(conn, df_person, df_history):
    """Update the database with new data."""
    try:
        cursor = conn.cursor()
        
        # Determine target tables based on test mode
        target_person = "person_2" if not TEST_MODE else "person_2_test"
        target_history = "person_history_2" if not TEST_MODE else "person_history_2_test"
        
        # Truncate the target tables
        logger.info(f"Truncating {target_person} and {target_history} tables...")
        cursor.execute(f"TRUNCATE TABLE {target_person};")
        cursor.execute(f"TRUNCATE TABLE {target_history};")
        conn.commit()
        
        # Replace NaN values with None for proper NULL handling in MySQL
        df_person = df_person.replace({np.nan: None})
        df_history = df_history.replace({np.nan: None})
        
        # For numeric amounts, ensure NaN stays as NULL (don't convert to 0)
        numeric_columns = ['person_age']
        for col in numeric_columns:
            if col in df_person.columns:
                df_person[col] = df_person[col].replace('', np.nan)
        
        # Ensure proper NULL handling for string columns (empty strings become NULL)
        string_columns = ['person_id', 'person_name', 'company_id', 'position_name', 'position_code_original', 'status']
        for df, cols in [(df_person, ['person_id', 'person_name']), 
                         (df_history, ['person_id', 'company_id', 'position_name', 'position_code_original', 'status'])]:
            for col in cols:
                if col in df.columns:
                    df[col] = df[col].replace('', np.nan)
        
        # Format date columns to 'YYYY-MM-DD' strings
        for col in df_history.columns:
            if df_history[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df_history[col]):
                df_history[col] = df_history[col].dt.strftime('%Y-%m-%d')
        
        # Insert person data
        logger.info(f"Inserting {len(df_person)} rows into {target_person} table...")
        batch_size = 100 if TEST_MODE else 1000
        
        # Prepare for batch insert
        person_cols = ', '.join(f"`{col}`" for col in df_person.columns)
        person_placeholders = ', '.join(['%s'] * len(df_person.columns))
        
        # Process person data in batches
        for start in range(0, len(df_person), batch_size):
            end = min(start + batch_size, len(df_person))
            batch = df_person.iloc[start:end]
            
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
                f"INSERT INTO {target_person} ({person_cols}) VALUES ({person_placeholders})",
                data_tuples
            )
            conn.commit()
            logger.info(f"Inserted person rows {start} to {end}")
        
        # Insert history data
        logger.info(f"Inserting {len(df_history)} rows into {target_history} table...")
        
        # Prepare for batch insert
        history_cols = ', '.join(f"`{col}`" for col in df_history.columns)
        history_placeholders = ', '.join(['%s'] * len(df_history.columns))
        
        # Process history data in batches
        for start in range(0, len(df_history), batch_size):
            end = min(start + batch_size, len(df_history))
            batch = df_history.iloc[start:end]
            
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
                f"INSERT INTO {target_history} ({history_cols}) VALUES ({history_placeholders})",
                data_tuples
            )
            conn.commit()
            logger.info(f"Inserted history rows {start} to {end}")
        
        logger.info("Database update completed successfully")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def main(test_mode=False):
    """Main function to orchestrate the person history data update process."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    start_time = time.time()
    logger.info(f"{mode_prefix} Starting person history data update process")
    
    try:
        # Initialize connections
        initialize_refinitiv()
        conn = connect_to_database()
        
        # Create dummy tables
        create_dummy_tables(conn)
        
        # Get company data from database
        master_mapping, codified_roles_df = get_company_data(conn)
        
        # Load officer codes
        od_details_codes, od_position_codes, od_position_names, od_position_code_codes, od_position_code_names = load_officer_codes()
        
        # Get list of company PermIDs to process
        unpulled_tickers = master_mapping['permid'].dropna().astype(str).tolist()
        
        # Process company data
        df_person, df_history = process_company_data(
            unpulled_tickers, od_details_codes, od_position_codes, od_position_names, 
            od_position_code_codes, od_position_code_names, codified_roles_df
        )
        
        # Export to CSV
        export_to_csv(df_person, df_history, conn, TEST_MODE)
        
        # Update database
        update_database(conn, df_person, df_history)
        
        # Close connections
        conn.close()
        logger.info("Database connection closed")
        
        rd.close_session()
        logger.info("Refinitiv session closed")
        
        # Calculate and log execution time
        execution_time = time.time() - start_time
        logger.info(f"{mode_prefix} Person history data update process completed successfully in {execution_time:.2f} seconds")
    except Exception as e:
        logger.error(f"{mode_prefix} Person history data update process failed: {e}")
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
    
    parser = argparse.ArgumentParser(description="Person History Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
