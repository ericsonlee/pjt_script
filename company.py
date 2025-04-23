#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Company Data Update Script
--------------------------
This script fetches company data from Refinitiv, processes it, and updates a MySQL database.
It's designed to run as an automated Windows task.

Author: Converted from Jupyter notebook
Date: 2025-04-14
"""

import os
import time
import logging
import shutil
from datetime import datetime
import mysql.connector
import pandas as pd
import numpy as np
import refinitiv.data as rd
from tqdm.auto import tqdm
# Import database utility functions
from db_utils import connect_to_database, initialize_refinitiv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"company_update_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Test mode flag - set to True to use minimal data for testing
TEST_MODE = False
# Test table suffix - append to table names when test mode is active
TEST_TABLE_SUFFIX = "_test"

def process_company_data():
    """Process company data from Refinitiv and existing database."""
    try:
        logger.info("Processing company data...")
        
        # Connect to database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Determine table names based on test mode
        company_table = "company" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        company2_table = "company_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        deals_summary_table = "deals_summary_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        
        # Read data from database
        logger.info(f"Reading data from {deals_summary_table} table...")
        query = f"SELECT * FROM {deals_summary_table}"
        df = pd.read_sql(query, conn)
        
        logger.info(f"Reading data from {company_table} table...")
        query = f"SELECT * FROM {company_table}"
        company = pd.read_sql(query, conn)
        
        # Read mapping data
        logger.info("Reading mapping data...")
        df_mapping = pd.read_excel('master_mapping_240327.xlsx')
        df_mapping = df_mapping[['RIC','Name','PermID','MktCap(US$bn)']]
        df_mapping = df_mapping.rename(columns={
            'RIC': 'RIC',
            'Name': 'Company Common Name',
            'PermID': 'Organization PermID',
            'MktCap(US$bn)': 'Company Market Cap'
        })
        df_mapping = df_mapping.sort_values(by='Company Market Cap', ascending=False)
        
        # If in test mode, use a very small subset of the data
        if TEST_MODE:
            logger.info("TEST MODE: Using minimal dataset")
            df_mapping = df_mapping.head(50)
        else:
            df_mapping = df_mapping.head(2000)
        
        col = ['TR.RIC', 'TR.CommonName', 'TR.OrganizationID', 'TR.CompanyMarketCap(Scale=9,Curn=USD)']
        
        # If in test mode, use a very limited set of tickers
        if TEST_MODE:
            global_tickers = ['MSFT.O', 'AAPL.O', 'AMZN.O']  # Just a few major tickers for testing
            logger.info(f"TEST MODE: Using only {len(global_tickers)} tickers for Refinitiv data")
        else:
            global_tickers = ['0#.TRXFLDGLPU']  # Refinitiv Global 12,113
        
        df_r = rd.get_data(universe=global_tickers, fields=col)
        df_r = df_r.sort_values(by='Company Market Cap', ascending=False)
        
        # If in test mode, limit the number of rows
        if TEST_MODE:
            df_r = df_r.head(10)
        else:
            df_r = df_r.head(2000)
        
        if not TEST_MODE:
            global_tickers = ['0#.SPX']  # S&P500
            df_spx = rd.get_data(universe=global_tickers, fields=col)
        else:
            # In test mode, just use a subset of previous data
            df_spx = df_r.head(5)
        
        eikon_df = pd.concat([df_r, df_spx, df_mapping])
        
        companies_list = (
            df['target_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['target_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['target_ultimate_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_ultimate_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            eikon_df['Organization PermID'].dropna().apply(lambda x: str(int(float(x)))).to_list()
        )
        companies_list = list(set(companies_list))
        
        # If in test mode, limit the list to a small subset
        if TEST_MODE:
            logger.info(f"TEST MODE: Limiting companies from {len(companies_list)} to 20")
            companies_list = companies_list[:20]
        
        tc = (
            df['target_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['target_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['target_ultimate_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list() +
            df['acquiror_ultimate_parent_permid'].dropna().apply(lambda x: str(int(float(x)))).to_list()
        )
        
        def split_list(l, n):
            return [l[x: x+n] for x in range(0, len(l), n)]
        
        # Adjust batch size for test mode
        batch_size = 50 if TEST_MODE else 250
        tickers = split_list(companies_list, batch_size)
        
        mapping_fields = [
            "TR.RIC",
            "TR.CommonName",
            "TR.CompanyMarketCap(Scale=9,Curn=USD)",
            "TR.CompanyMarketCap.date",
            "TR.HeadquartersCountry",
            "TR.PrimaryExchangeName",  # Changed from CF_EXCHNG to TR.PrimaryExchangeName
            "TR.ExchangeCountry",
            "TR.HQCountryCode",
            "TR.OrganizationID",  # PermID
            "TR.ISIN",  # ISIN
            "TR.CUSIP",  # CUSIP
            "TR.ExchangeTicker",  # Exchange Ticker
            "TR.SEDOL",  # SEDOL
            "TR.GICSSector",  # GICS Sector Name
            "TR.GICSIndustry",  # GICS Industry
            "TR.BusinessSummary",  # TR Business Description
            "TR.UltimateParent",  # Ultimate Parent Name
            "TR.UltimateParentId",  # Ultimate Parent ID
            'TR.CIKNUMBER',  # CIK
        ]
        
        mapping_columns = {
            'Instrument': 'PermID',
            'TR.RIC': 'RIC',
            'TR.COMMONNAME': 'Name',
            'TR.COMPANYMARKETCAP.DATE': 'Date',
            'TR.ISIN': 'ISIN',
            'TR.CUSIP': 'CUSIP',
            'TR.SEDOL': 'SEDOL',
            'TR.COMPANYMARKETCAP(SCALE=9,CURN=USD)': 'MktCap(US$bn)',
            'TR.ULTIMATEPARENT': 'ParentCompany',
            'TR.HEADQUARTERSCOUNTRY': 'HQCountry',
            'TR.HQCOUNTRYCODE': 'HQCode',
            'TR.PRIMARYEXCHANGENAME': 'Exchange',
            'TR.EXCHANGECOUNTRY': 'ExchangeCountry',
            'TR.ORGANIZATIONID': 'PermID',
            'TR.EXCHANGETICKER': 'Ticker',
            'TR.GICSSECTOR': 'gics_sector',
            'TR.GICSINDUSTRYGROUP': 'gics_industry_group',
            'TR.GICSINDUSTRY': 'gics_industry',
            'TR.GICSSUBINDUSTRY': 'gics_sub_industry',
            'TR.GICSSECTORCODE': 'gics_sector_code',
            'TR.GICSINDUSTRYGROUPCODE': 'gics_industry_group_code',
            'TR.GICSINDUSTRYCODE': 'gics_industry_code',
            'TR.GICSSUBINDUSTRYCODE': 'gics_sub_industry_code',
            'TR.BUSINESSSUMMARY': 'description',
            'TR.ULTIMATEPARENTID': 'UltimateParentPermID',
            'TR.CIKNUMBER': 'cik_num'
        }
        
        # Initialize output dataframe
        updated_map = rd.get_data('5029074088', fields=mapping_fields, use_field_names_in_headers=True)
        
        i = 0
        max_attempts = 3
        
        # Process data in batches (using tqdm for progress bar)
        logger.info("Processing Refinitiv data in batches...")
        
        # For test mode, use fewer batches if needed
        if TEST_MODE:
            logger.info(f"TEST MODE: Processing only {min(3, len(tickers))} batches instead of {len(tickers)}")
            tickers = tickers[:min(3, len(tickers))]
        
        for ticker in tqdm(tickers):
            attempts = 0
            while attempts < max_attempts:
                try:
                    data = rd.get_data(universe=ticker, fields=mapping_fields, use_field_names_in_headers=True)
                    updated_map = pd.concat([updated_map, data])
                    break  # Exit the retry loop on success
                except Exception as e:
                    attempts += 1
                    logger.error(f"Error fetching data for {ticker}: {e}. Attempt {attempts} of {max_attempts}.")
                    if attempts >= max_attempts:
                        logger.error(f"Failed to fetch data for {ticker} after {max_attempts} attempts.")
                        # Optionally, you can handle logging or store the ticker for later review here
        
        # Process data like in the notebook
        df_company = updated_map.reset_index(drop=True)
        df_company = df_company.drop_duplicates(subset=['Instrument','TR.CIKNUMBER'])
        df_cik = df_company.groupby('Instrument')['TR.CIKNUMBER'].apply(lambda x: ','.join(map(str, x.dropna().tolist()))).reset_index()
        df_company = df_company.drop_duplicates(subset='Instrument')
        df_company = df_company.drop('TR.CIKNUMBER',axis=1).merge(df_cik)
        
        # Print the first few rows of the dataframe to see the column values
        logger.info("Raw dataframe columns: %s", df_company.columns.tolist())
        
        # Rename columns
        df_company.columns = [
            'permid', 'company_id', 'name', 'mkt_cap', 'date', 'country_headquarters',
            'country_exchange', 'country_code', 'organization_id', 'isin', 'cusip',
            'exchange_ticker', 'sedol', 'gics_sector', 'gics_industry', 'business_description',
            'ultimate_parent_name', 'ultimate_parent_id', 'ciks'
        ]
        
        # Copy country_exchange to exchange column to match the notebook
        df_company['exchange'] = df_company['country_exchange']
        
        # Print the first few rows of the dataframe after renaming to see the column values
        logger.info("Renamed dataframe columns: %s", df_company.columns.tolist())
        logger.info("First few rows of exchange column: %s", df_company['exchange'].head(10).tolist())
        
        # Reorder columns to match the notebook
        df_company = df_company[['permid', 'company_id', 'ciks', 'name', 'mkt_cap',
                               'country_headquarters', 'country_exchange', 'exchange',
                               'isin', 'cusip', 'gics_sector', 'gics_industry',
                               'business_description', 'ultimate_parent_id', 'ultimate_parent_name']]
        
        # Convert permid to float
        df_company['permid'] = df_company['permid'].astype(float)
        
        # Drop unnecessary columns from company table
        company = company.drop(['create_time', 'update_time', 'flag', 'id'], axis=1)
        
        # Print a sample of the company table to see its structure
        logger.info("Company table columns: %s", company.columns.tolist())
        logger.info("Sample of company table (first row):")
        if len(company) > 0:
            for col in company.columns:
                logger.info(f"  {col}: {company[col].iloc[0] if len(company) > 0 else 'N/A'}")
        
        # Print a sample of df_company to see its structure before merging
        logger.info("df_company columns: %s", df_company.columns.tolist())
        logger.info("Sample of df_company (first row):")
        for col in df_company.columns:
            logger.info(f"  {col}: {df_company[col].iloc[0] if len(df_company) > 0 else 'N/A'}")
        
        # Handle missing values for numeric columns
        # Replace empty strings with NaN, but do NOT replace NaN with 0 for mkt_cap
        df_company['mkt_cap'] = df_company['mkt_cap'].replace('', np.nan)  # Keep NULL values as NULL
        
        # Ensure permid and ultimate_parent_id are numeric
        df_company['permid'] = pd.to_numeric(df_company['permid'], errors='coerce').fillna(0)
        df_company['ultimate_parent_id'] = pd.to_numeric(df_company['ultimate_parent_id'], errors='coerce').fillna(0)
        
        # For string columns, replace empty strings with NaN (which will be converted to NULL in SQL)
        # DO NOT use fillna('') as we want NaNs to become NULL in the database
        string_columns = ['company_id', 'name', 'country_headquarters', 'country_exchange', 
                         'exchange', 'isin', 'cusip', 'gics_sector', 'gics_industry', 
                         'business_description', 'ultimate_parent_name']
        
        for col in string_columns:
            df_company[col] = df_company[col].replace('', np.nan)
        
        # For ciks, we'll handle empty values as NULL as well
        df_company['ciks'] = df_company['ciks'].replace('', np.nan)
        
        # Instead of using only the new data, combine with existing company data as in the notebook
        # But we need to preserve the exchange values from the new data
        
        # First, create a mapping of permids to exchange values from the new data
        exchange_mapping = dict(zip(df_company['permid'], df_company['exchange']))
        
        # Merge new and existing data
        df_company_final = pd.concat([company, df_company]).drop_duplicates(subset='permid')
        
        # Update exchange values in the final dataframe using the mapping
        for permid, exchange in exchange_mapping.items():
            if not pd.isna(exchange):  # Only update if exchange is not NaN
                df_company_final.loc[df_company_final['permid'] == permid, 'exchange'] = exchange
        
        # Print a sample of the final dataframe to check exchange column
        logger.info("Sample of final dataframe:")
        sample_df = df_company_final.head(10)
        for idx, row in sample_df.iterrows():
            logger.info(f"Row {idx}: permid={row['permid']}, exchange={row['exchange']}")
        
        # Export to CSV
        output_filename = 'company_test.csv' if TEST_MODE else 'company.csv'
        logger.info(f"Exporting company data to {output_filename}...")
        df_company_final.to_csv(output_filename, index=False)
        
        return df_company_final, company2_table
        
    except Exception as e:
        logger.error(f"Error processing company data: {e}")
        raise

def create_dummy_table(conn, source_table, target_table):
    """Create a dummy table in MySQL by replicating the structure of another table."""
    try:
        cursor = conn.cursor()
        
        # Check if target table exists
        cursor.execute(f"SHOW TABLES LIKE '{target_table}'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            logger.info(f"Creating {target_table} table...")
            
            # Get the structure of source table
            cursor.execute(f"SHOW CREATE TABLE {source_table}")
            create_table_stmt = cursor.fetchone()[1]
            
            # Modify the statement to create target table
            create_table_stmt = create_table_stmt.replace(f"CREATE TABLE `{source_table}`", 
                                                         f"CREATE TABLE `{target_table}`")
            
            # Create the table
            cursor.execute(create_table_stmt)
            conn.commit()
            logger.info(f"{target_table} table created successfully")
        else:
            logger.info(f"{target_table} table already exists")
    except Exception as e:
        logger.error(f"Error creating dummy table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def update_database(conn, df_company_final, target_table):
    """Update the database with new company data."""
    try:
        cursor = conn.cursor()
        
        # Truncate the target table
        logger.info(f"Truncating {target_table} table...")
        cursor.execute(f"TRUNCATE TABLE {target_table};")
        conn.commit()
        
        # Prepare for batch insert
        batch_size = 100 if TEST_MODE else 1000  # Smaller batches in test mode
        cols = ', '.join(f"`{col}`" for col in df_company_final.columns)
        placeholders = ', '.join(['%s'] * len(df_company_final.columns))
        insert_query = f"INSERT INTO {target_table} ({cols}) VALUES ({placeholders})"
        
        # Insert data in batches with progress tracking
        total_rows = len(df_company_final)
        logger.info(f"Inserting {total_rows} rows in batches of {batch_size}...")
        
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = df_company_final.iloc[start:end]
            
            # Convert Pandas/Numpy types to Python built-in types
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

def main(test_mode=False):
    """Main function to orchestrate the company data update process."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    start_time = time.time()
    logger.info(f"{mode_prefix} Starting company data update process")
    
    try:
        # Initialize Refinitiv
        initialize_refinitiv()
        
        # Connect to database
        conn = connect_to_database()
        
        # Determine source and target tables based on test mode
        source_table = "company"
        target_table = "company_2" if not TEST_MODE else "company_2_test"
        
        # Create test tables if in test mode
        if TEST_MODE:
            logger.info(f"{mode_prefix} Creating test tables...")
            create_dummy_table(conn, "company", "company_test")
            create_dummy_table(conn, "company_2", "company_2_test")
            create_dummy_table(conn, "deals_summary_2", "deals_summary_2_test")
        else:
            # Create company_2 table if it doesn't exist
            logger.info("Creating company_2 table if it doesn't exist...")
            create_dummy_table(conn, "company", "company_2")
        
        # Process company data
        df_company_final, target_table = process_company_data()
        
        # Update database
        update_database(conn, df_company_final, target_table)
        
        # Close connections
        if conn:
            conn.close()
            logger.info("Database connection closed")
        
        rd.close_session()
        logger.info("Refinitiv session closed")
        
        # Calculate and log execution time
        execution_time = time.time() - start_time
        logger.info(f"{mode_prefix} Company data update process completed successfully in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"{mode_prefix} Company data update process failed: {e}")
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
    
    parser = argparse.ArgumentParser(description="Company Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
