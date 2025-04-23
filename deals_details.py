#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Deals Details Data Update Script
--------------------------------
This script fetches deal data from Refinitiv, processes it, and updates a MySQL database.
It's designed to run as an automated Windows task.

Author: Converted from Jupyter notebook
Date: 2025-03-19
"""

import os
import time
import logging
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

def get_field_lists(conn):
    """Get field lists from the database."""
    try:
        logger.info("Fetching field lists from database...")
        
        # Read refinitiv_fields table
        query = "SELECT * FROM refinitiv_fields"
        df_fields = pd.read_sql(query, conn)
        
        # Read deal_sheet table
        query = "SELECT * FROM deal_sheet"
        df_ds = pd.read_sql(query, conn)
        
        # Filter and process field lists
        df_ds = df_ds[df_ds['source'] == "Refinitiv"]
        ds_list = df_ds['code'].to_list()
        
        default_fields_noconcat = df_fields[(df_fields['default_flag']==1) & 
                                           (df_fields['concat_flag']!=1)]['trcode'].to_list()
        
        concat_fields = df_fields[df_fields['concat_flag'] == 1]['trcode'].to_list()
        ds_list_noconcat = [code for code in ds_list if code not in concat_fields]
        noconcat_fields = list(set(default_fields_noconcat + ds_list_noconcat))
        noconcat_fields = ['TR.' + field.upper() for field in noconcat_fields]
        
        logger.info(f"Retrieved {len(noconcat_fields)} fields for data fetching")
        return noconcat_fields
    except Exception as e:
        logger.error(f"Error getting field lists: {e}")
        raise

def fetch_data(query_string, noconcat_fields):
    """Fetch data from Refinitiv with retry mechanism."""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for retry in range(max_retries):
        try:
            logger.info(f"Fetching data from Refinitiv (attempt {retry+1}/{max_retries})...")
            deal = rd.get_data(
                universe=query_string,
                fields=noconcat_fields,
                parameters={"Curn": "USD"},
                use_field_names_in_headers=True
            )
            logger.info(f"Successfully fetched data with {len(deal)} records")
            return deal
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching data: {error_msg}")
            if retry < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum retries reached for data fetching")
                raise

def process_data(all_deals):
    """Process the fetched data."""
    try:
        logger.info("Processing fetched data...")
        all_deals[0].columns = all_deals[0].columns.str.upper()
        all_deals[1].columns = all_deals[1].columns.str.upper()
        # Filter first dataset for deals > $1B
        tmp = all_deals[0]
        all_deals[0] = tmp[tmp['TR.MNARANKVALUEINCNETDEBT'] > 100000000]

        tmp = all_deals[1]
        all_deals[1] = tmp[tmp['TR.MNARANKVALUEINCNETDEBT'] > 100000000]
        
        # Combine all data
        big_table = pd.concat(all_deals, ignore_index=True)
        big_table = big_table.drop(columns=['INSTRUMENT'])
        
        # Rename and restructure
        fields = {'TR.MNASDCDEALNUMBER': 'deal_id'}
        big_table = big_table.rename(columns=fields)
        
        # Transform to long format
        dt = big_table.set_index('deal_id')
        dt = dt.unstack()
        dt.name = 'value'
        dt.index.names = ['name', 'deal_id']
        dt = dt.reset_index()
        
        # Clean data - ensure proper null handling
        dt['value'] = dt['value'].replace('', np.nan)
        dt['value'] = dt['value'].replace('nan', np.nan)
        dt = dt.drop_duplicates(subset=['deal_id', 'name', 'value'])
        dt['name'] = dt['name'].str.replace('^TR\\.', '', regex=True).str.lower()
        
        logger.info(f"Processed data into {len(dt)} rows")
        return dt
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def export_to_csv(df_deals_details):
    """Export current deals_details to CSV with date."""
    try:
        # Generate filename with today's date
        today_date = datetime.now().strftime('%Y-%m-%d')
        filename = f"deals_details_{today_date}.csv"
        
        # Remove any existing deals_details CSV files
        for file in os.listdir("."):
            if file.startswith("deals_details") and file.endswith(".csv"):
                try:
                    os.remove(file)
                    logger.info(f"Deleted existing file: {file}")
                except OSError as e:
                    logger.error(f"Error deleting file {file}: {e}")
        
        # Export to CSV
        df_deals_details.to_csv(filename, index=False)
        logger.info(f"Exported data to {filename}")
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

def update_database(conn, dt):
    """Update the database with new data."""
    try:
        cursor = conn.cursor()
        
        # Truncate the target table
        logger.info("Truncating deals_details_2 table...")
        cursor.execute("TRUNCATE TABLE deals_details_2;")
        conn.commit()
        
        # Prepare for batch insert
        batch_size = 20000  # Adjust based on performance testing
        cols = ', '.join(f"`{col}`" for col in dt.columns)
        placeholders = ', '.join(['%s'] * len(dt.columns))
        insert_query = f"INSERT INTO deals_details_2 ({cols}) VALUES ({placeholders})"
        
        # Insert data in batches with progress tracking
        total_rows = len(dt)
        logger.info(f"Inserting {total_rows} rows in batches of {batch_size}...")
        
        # Create a list of field names that should be treated as dates
        date_fields = [
            'mnaanndate', 'mnaeffectivedate', 'mnawithdrawdate', 'mnacompletiondate', 
            'mnacloseddate', 'mnaexpectedcompletiondate', 'mnalastbiddate', 'mnarankdate',
            'mnaexpectedsynergydate', 'mnaexpectedaccretivedate'
        ]
        
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = dt.iloc[start:end]
            
            # Convert Pandas/Numpy types to Python built-in types
            # Ensure dates are formatted as 'YYYY-MM-DD' strings without time portion
            data_tuples = []
            for row in batch.itertuples(index=False, name=None):
                processed_row = []
                
                # Get field name and value pairs for this row
                field_values = list(zip(dt.columns, row))
                
                for col, v in field_values:
                    # Handle null values
                    if pd.isna(v):
                        processed_row.append(None)
                    # Handle date fields - check if the name column contains a date field value
                    elif col == 'name' and v in date_fields and 'value' in dt.columns:
                        # This is a date field name, so the corresponding value should be formatted as a date
                        processed_row.append(v)
                    # Handle date values - check if this is a value column and the corresponding name is a date field
                    elif col == 'value' and len(field_values) > 1:
                        # Get the name value from the same row
                        name_col = [fv[1] for fv in field_values if fv[0] == 'name']
                        if name_col and name_col[0] in date_fields:
                            # This is a date value
                            if isinstance(v, (pd.Timestamp, datetime)):
                                # Format datetime objects as 'YYYY-MM-DD' strings
                                processed_row.append(v.strftime('%Y-%m-%d'))
                            elif isinstance(v, str) and ' 00:00:00' in v:
                                # Remove time portion from string dates
                                processed_row.append(v.split(' ')[0])
                            else:
                                processed_row.append(v)
                        elif hasattr(v, 'item'):
                            # Convert other numpy/pandas types to Python types
                            processed_row.append(v.item())
                        else:
                            processed_row.append(v)
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
    logger.info("Starting deals details data update process")
    
    try:
        # Initialize connections using db_utils functions
        conn = connect_to_database()
        initialize_refinitiv()
        
        # Get field lists
        noconcat_fields = get_field_lists(conn)
        
        # Define query strings
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
        all_deals = []
        logger.info("Fetching data for query 1 (>1bn exclude PJT)...")
        all_deals.append(fetch_data(query_string1, noconcat_fields))
        
        logger.info("Fetching data for query 2 (All PJT deals after 1 Oct 2015)...")
        all_deals.append(fetch_data(query_string2, noconcat_fields))
        
        # Process data
        dt = process_data(all_deals)
        
        # Read current deals_details table
        logger.info("Reading current deals_details table...")
        query = "SELECT * FROM deals_details"
        df_deals_details = pd.read_sql(query, conn)
        
        # Export to CSV
        export_to_csv(df_deals_details)
        
        # Update database
        update_database(conn, dt)
        
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
