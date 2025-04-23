#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SEC Documents Data Update Script
--------------------------
This script fetches SEC filing documents for M&A deals, processes them, and updates a MySQL database.
It retrieves documents from the SEC EDGAR database using company CIK numbers from deals in the database.
"""

import os
import time
import logging
import pandas as pd
import requests
import random
import urllib.parse
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import mysql.connector
import edgar
from db_utils import connect_to_database

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

# Tell the SEC who you are (required for API usage)
edgar.set_identity("Michael Mccallum mike.mccalum@indigo.com")

# SEC API configuration
SEC_SEARCH_INDEX_URL = 'https://efts.sec.gov/LATEST/search-index?'
SEC_HEADERS_LIST = [
    {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    },
    {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    },
    {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0'
    },
    {
        'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81'
    },
    {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0'
    },
    {
        'User-Agent':
        'Mozilla/5.0 (X11; Linux i686; rv:124.0) Gecko/20100101 Firefox/124.0'
    },
]

# Define filing types and form types
FILING_TYPES = [
    "8-K", "10-K", "10-Q", "DEF 14A", "20-F", "S-1", "S-4", 
    "13D", "13G", "SC 14D-1", "SC 13D", "SC 13D/A", "SC 13E-3", 
    "SC 14D-1", "SC 14D-1/A", "SC 14D-9", "SC 14D-9/A", "SC TO-I", 
    "SC TO-I/A", "SC TO-T", "SC TO-T/A", "SC 14D-1/A"
]

FORM_TYPES = [
    "8-K", "10-K", "10-Q", "DEF 14A", "20-F", "S-1", "S-4", 
    "13D", "13G", "SC 14D-1", "SC 13D", "SC 13D/A", "SC 13E-3", 
    "SC 14D-1", "SC 14D-1/A", "SC 14D-9", "SC 14D-9/A", "SC TO-I", 
    "SC TO-I/A", "SC TO-T", "SC TO-T/A", "SC 14D-1/A"
]

class SECSearcher:
    """Class for searching SEC filings through the EDGAR database."""
    
    @staticmethod
    def search_filings_api_multiple_cik(ciks: str,
                                        filter_cik: str = None,
                                        query: str = None,
                                        start_date: str = None,
                                        end_date: str = None,
                                        file_types: str = None,
                                        include_exhibits: bool = False):
        """Search for SEC filings for multiple CIKs (company identifiers)."""
        cik_list = ciks.split(',')
        
        # In test mode, limit the number of CIKs to process
        if TEST_MODE and len(cik_list) > 5:
            logger.info(f"TEST MODE: Limiting from {len(cik_list)} to 5 CIKs")
            cik_list = cik_list[:5]
            
        filings = [
            SECSearcher.search_filings_api(cik, filter_cik, query, start_date,
                                           end_date, file_types, include_exhibits)
            for cik in cik_list if cik  # Skip empty CIK values
        ]
        flattened_list = [item for sublist in filings for item in sublist]
        
        # In test mode, limit the number of results
        if TEST_MODE and len(flattened_list) > 20:
            logger.info(f"TEST MODE: Limiting from {len(flattened_list)} to 20 results")
            flattened_list = flattened_list[:20]
            
        return flattened_list

    @staticmethod
    def search_filings_api(cik: str,
                           filter_cik: str = None,
                           query: str = None,
                           start_date: str = None,
                           end_date: str = None,
                           file_types: str = None,
                           include_exhibits: bool = False):
        """Search for SEC filings for a specific CIK (company identifier)."""
        if not cik or cik.strip() == '':
            return []
            
        def process_cik(original_cik):
            if len(original_cik) == 10:
                return original_cik
            return '0' * (10 - len(original_cik)) + original_cik

        def process_url(cik, url_raw):
            url_template = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}'
            url1, url2 = url_raw.split(':')
            url1 = url1.replace('-', '')
            return url_template.format(cik, url1, url2)

        # 1. Search for filings using the SEC search index api
        try:
            params = {'ciks': process_cik(cik)}
            if filter_cik is not None:
                params['filter_ciks'] = process_cik(filter_cik)
            if query is not None:
                params['q'] = query
            if start_date is not None:
                params['startdt'] = start_date
            if end_date is not None:
                params['enddt'] = end_date
            if file_types is not None:
                params['forms'] = file_types
                
            url = SEC_SEARCH_INDEX_URL + urllib.parse.urlencode(params).replace('+', '%20')
            res = requests.get(url, headers=random.choice(SEC_HEADERS_LIST), timeout=10)
            
            # 2. Process the filings list
            hits = res.json().get('hits', {}).get('hits', [])
            
            # In test mode, limit the number of hits to process
            if TEST_MODE and len(hits) > 10:
                logger.info(f"TEST MODE: Limiting from {len(hits)} to 10 hits")
                hits = hits[:10]
                
            filings = []
            
            for filing in hits:
                try:
                    url_raw = filing['_id']
                    source = filing['_source']
                    accession_id = source['adsh']
                    filing_type = source['file_type']
                    file_date = source['file_date']
                    cik = source['ciks'][0]
                    # exclude all amendments
                    form = source['form']
                    if form.endswith('/A'):
                        continue
                    filings.append({
                        'doc_id': url_raw.split(':')[-1],
                        'link': process_url(cik, url_raw),
                        'cik': cik,
                        'type': form,
                        'accession_id': accession_id,
                        'file_date': file_date,
                        'document_type': form,
                        'is_exhibit': 0,
                    })
                    
                    # In test mode, only include exhibits for a small subset
                    if include_exhibits and (not TEST_MODE or random.random() < 0.3):
                        try:
                            filing_obj = edgar.get_by_accession_number(accession_id)
                            filing_url_segments = filing_obj.filing_url.split('/')
                            
                            # In test mode, limit number of attachments
                            attachments = filing_obj.attachments
                            if TEST_MODE and len(attachments) > 3:
                                attachments = attachments[:3]
                                
                            for attachment in attachments:
                                if 'EX' not in attachment.document_type or not attachment.is_html():
                                    continue
                                doc_id = attachment.document
                                filing_url_segments[-1] = doc_id
                                filings.append({
                                    'doc_id': doc_id,
                                    'link': '/'.join(filing_url_segments),
                                    'cik': cik,
                                    'type': form,
                                    'accession_id': accession_id,
                                    'file_date': file_date,
                                    'document_type': attachment.document_type,
                                    'is_exhibit': 1,
                                })
                        except Exception as e:
                            logger.error(f"Error processing exhibits: {e}")
                except Exception as e:
                    logger.error(f"Error processing filing: {e}")
                    continue
                    
            return filings
        except Exception as e:
            logger.error(f"Error searching SEC filings for CIK {cik}: {e}")
            return []


def search_filings_sequence(ciks, start_date, end_date):
    """
    Search for SEC filings in sequence, trying different document types.
    Uses a cascading approach to find the most relevant filing types for M&A deals.
    """
    if not ciks or ciks.strip() == '':
        return []
        
    try:
        # First try DEFM14A and S-4 (key M&A filing types)
        docs = SECSearcher.search_filings_api_multiple_cik(
            ciks=ciks,
            start_date=start_date,
            end_date=end_date,
            file_types='DEFM14A,S-4',
            include_exhibits=False,
        )
        if len(docs) > 0:
            return docs

        # Then try SC TO-T and SC 14D9 (tender offers) with exhibits
        docs = SECSearcher.search_filings_api_multiple_cik(
            ciks=ciks,
            start_date=start_date,
            end_date=end_date,
            file_types='SC TO-T,SC 14D9',
            include_exhibits=True,
        )
        if len(docs) > 0:
            return docs

        # Then try preliminary proxy statements
        docs = SECSearcher.search_filings_api_multiple_cik(
            ciks=ciks,
            start_date=start_date,
            end_date=end_date,
            file_types='PREM14A,DEFM14C,PREM14C',
            include_exhibits=False,
        )
        if len(docs) > 0:
            return docs

        # Finally try 8-K (current report) filings with Item 1.01 (material agreements)
        docs = SECSearcher.search_filings_api_multiple_cik(
            ciks=ciks,
            query='Item 1.01',
            start_date=start_date,
            end_date=end_date,
            file_types='8-K',
            include_exhibits=True,
        )
        return docs
    except Exception as e:
        logger.error(f"Error in search_filings_sequence: {e}")
        return []


def create_dummy_table(conn, source_table, target_table):
    """Create a dummy table by replicating the structure of another table."""
    try:
        cursor = conn.cursor()
        
        # Check if the target table exists
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


def truncate_table(conn, table_name):
    """Truncate the specified table."""
    try:
        cursor = conn.cursor()
        
        # Truncate the table
        logger.info(f"Truncating {table_name} table...")
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()
        logger.info(f"{table_name} table truncated successfully")
    except Exception as e:
        logger.error(f"Error truncating {table_name} table: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def update_database(conn, sec_documents_df, target_table):
    """Update the database with new SEC document data."""
    try:
        cursor = conn.cursor()
        
        if sec_documents_df.empty:
            logger.warning("No documents to update in the database")
            return
            
        logger.info(f"Inserting {len(sec_documents_df)} documents into {target_table}...")
        
        # Replace NaN values with None for proper NULL handling in MySQL
        sec_documents_df = sec_documents_df.replace({pd.NA: None})
        
        # Ensure proper NULL handling for string columns (empty strings become NULL)
        string_columns = ['doc_id', 'link', 'cik', 'type', 'accession_id', 'document_type', 'source']
        for col in string_columns:
            if col in sec_documents_df.columns:
                sec_documents_df[col] = sec_documents_df[col].replace('', None)
        
        # Convert date fields to proper format
        if 'file_date' in sec_documents_df.columns:
            sec_documents_df['file_date'] = pd.to_datetime(sec_documents_df['file_date']).dt.strftime('%Y-%m-%d')
        
        # Convert DataFrame to list of tuples for bulk insert
        records = []
        for _, row in sec_documents_df.iterrows():
            record = (
                row['deal_id'], 
                row['doc_id'], 
                row['link'], 
                row['cik'], 
                row['type'], 
                row['accession_id'], 
                row['file_date'], 
                row['document_type'], 
                row['is_exhibit'],
                row['source'],
                datetime.now(),  # create_time
                datetime.now(),  # update_time
                1  # flag
            )
            records.append(record)
        
        # Insert records in batches to avoid memory issues
        batch_size = 50 if TEST_MODE else 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            # Insert with ignore to handle duplicates
            insert_query = f"""
            INSERT IGNORE INTO {target_table} 
            (deal_id, doc_id, link, cik, type, accession_id, file_date, document_type, is_exhibit, source, create_time, update_time, flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, batch)
            conn.commit()
            logger.info(f"Inserted batch {i//batch_size + 1}/{(len(records)+batch_size-1)//batch_size}")
        
        logger.info("Database update completed successfully")
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()


def process_sec_documents(test_mode=False):
    """Main function to process SEC documents for deals."""
    global TEST_MODE
    TEST_MODE = test_mode
    
    # Set up log message prefix based on mode
    mode_prefix = "[TEST MODE]" if TEST_MODE else ""
    
    try:
        logger.info(f"{mode_prefix} Starting SEC documents processing...")
        
        # Connect to database
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Determine table names based on test mode
        company_table = "company_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        deals_table = "deals_summary_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        sec_documents_table = "sec_documents_2" + (TEST_TABLE_SUFFIX if TEST_MODE else "")
        
        # Create test tables if in test mode
        if TEST_MODE:
            logger.info(f"{mode_prefix} Creating test tables...")
            # Create test tables for original tables first
            for source, target in [
                ("company_2", "company_2_test"),
                ("deals_summary_2", "deals_summary_2_test"),
                ("sec_documents", "sec_documents_test")
            ]:
                create_dummy_table(conn, source, target)
        
        # Create target table if it doesn't exist
        create_dummy_table(conn, "sec_documents", sec_documents_table)
        
        # Read company data to get CIKs
        logger.info(f"{mode_prefix} Reading company data to extract CIKs...")
        query = f"SELECT * FROM {company_table}"
        company = pd.read_sql(query, conn)
        
        # Create a mapping of PermID to CIKs
        company = company[['permid', 'ciks', 'ultimate_parent_id']]
        permid_cik_mapping = company.set_index('permid')['ciks'].to_dict()
        company['ultimate_parent_ciks'] = company['ultimate_parent_id'].map(permid_cik_mapping)
        
        # Combine CIKs from company and its ultimate parent
        def combine_ciks(row):
            ciks = []
            if pd.notna(row['ciks']) and row['ciks']:
                ciks.extend(row['ciks'].split(','))
            if pd.notna(row['ultimate_parent_ciks']) and row['ultimate_parent_ciks']:
                ciks.extend(row['ultimate_parent_ciks'].split(','))
            return list(set(ciks))
        
        company['combined_ciks'] = company.apply(combine_ciks, axis=1)
        permid_ciks_mapping = company.set_index('permid')['combined_ciks'].to_dict()
        
        # Read deals data for processing
        logger.info(f"{mode_prefix} Reading deals data...")
        query = f"SELECT * FROM {deals_table}"
        deals = pd.read_sql(query, conn)
        
        # In test mode, limit the number of deals to process
        if TEST_MODE and len(deals) > 10:
            logger.info(f"{mode_prefix} Limiting from {len(deals)} to 10 deals")
            deals = deals.head(10)
        
        # Process deals to get CIKs and date ranges
        logger.info(f"{mode_prefix} Processing deals data to extract relevant information...")
        deals = deals[['deal_id', 'date_announced', 'date_effective',
                   'target_permid', 'target_parent_permid', 'target_ultimate_parent_permid',
                   'acquiror_permid', 'acquiror_parent_permid', 'acquiror_ultimate_parent_permid']]
        
        deals['date_announced'] = pd.to_datetime(deals['date_announced']).fillna(date(2010, 1, 1))
        deals['date_effective'] = pd.to_datetime(deals['date_effective']).fillna(date.today())
        
        # Gather all PermIDs from a deal into a list
        columns_permid = ['target_permid', 'target_parent_permid', 'target_ultimate_parent_permid',
                      'acquiror_permid', 'acquiror_parent_permid', 'acquiror_ultimate_parent_permid']
                      
        def combine_permids(row):
            permids = []
            for column in columns_permid:
                if pd.notna(row[column]):
                    permids.append(int(row[column]))
            return list(set(permids))
        
        deals['combined_permids'] = deals.apply(combine_permids, axis=1)
        
        # Map PermIDs to their CIKs
        def get_ciks(permids):
            ciks = []
            for permid in permids:
                res = permid_ciks_mapping.get(permid, None)
                if res and len(res) > 0:
                    ciks.extend(res)
            return ','.join(list(set(ciks)))
        
        deals['ciks'] = deals['combined_permids'].apply(get_ciks)
        
        # Add start and end dates for document searches
        deals['start_date'] = deals['date_announced'].dt.strftime("%Y-%m-%d")
        
        # In test mode, use a shorter window for end_date
        if TEST_MODE:
            # Add 1 month to effective date for end date
            deals['end_date'] = deals['date_effective'].apply(lambda d: (d + relativedelta(months=+1)).strftime("%Y-%m-%d"))
        else:
            # Add 3 months to effective date for end date to catch follow-up filings
            deals['end_date'] = deals['date_effective'].apply(lambda d: (d + relativedelta(months=+3)).strftime("%Y-%m-%d"))
        
        # Keep only necessary columns
        deals = deals[['deal_id', 'start_date', 'end_date', 'ciks']]
        
        # Process each deal to get SEC documents
        logger.info(f"{mode_prefix} Processing {len(deals)} deals to fetch SEC documents...")
        out = []
        
        for _, row in tqdm(deals.iterrows(), total=deals.shape[0], desc="Searching SEC filings"):
            try:
                # Search for SEC filings for this deal
                documents = search_filings_sequence(row['ciks'], row['start_date'], row['end_date'])
                if documents:
                    # Convert to DataFrame and add deal_id
                    tmp = pd.DataFrame(documents)
                    tmp['deal_id'] = row['deal_id']
                    # Add to results
                    out.extend(tmp.to_dict(orient='records'))
                    
                    # Log progress periodically
                    if len(out) % 500 == 0:
                        logger.info(f"{mode_prefix} Found {len(out)} documents so far...")
                        
            except Exception as e:
                logger.error(f"{mode_prefix} Error processing deal {row['deal_id']}: {e}")
                continue
        
        # Process all collected documents
        logger.info(f"{mode_prefix} Processing {len(out)} documents...")
        out_df = pd.DataFrame(out)
        
        if not out_df.empty:
            # Remove duplicates and documents that don't have proper links
            out_df = out_df.drop_duplicates(subset=['deal_id', 'accession_id', 'document_type'])
            out_df = out_df[out_df['link'].str.endswith('htm')]
            out_df['source'] = 'cik'  # Mark the source as CIK search
            
            # Save to CSV for backup
            logger.info(f"{mode_prefix} Saving documents to CSV for backup...")
            csv_filename = "sec_documents_test.csv" if TEST_MODE else "sec_documents.csv"
            out_df.to_csv(csv_filename, index=False)
            
            # Truncate the target table
            truncate_table(conn, sec_documents_table)
            
            # Update the database
            update_database(conn, out_df, sec_documents_table)
            
        else:
            logger.warning(f"{mode_prefix} No documents found to process")
        
        # Close database connection
        cursor.close()
        conn.close()
        logger.info(f"{mode_prefix} SEC documents processing completed")
        
    except Exception as e:
        logger.error(f"{mode_prefix} Error in process_sec_documents: {e}")
        raise


def main(test_mode=False):
    """Main function to run the SEC documents update process."""
    try:
        # Set up log message prefix based on mode
        mode_prefix = "[TEST MODE]" if test_mode else ""
        
        logger.info(f"{mode_prefix} Starting SEC documents update process...")
        start_time = time.time()
        
        # Process SEC documents
        process_sec_documents(test_mode)
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"{mode_prefix} SEC documents update completed in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in SEC documents update process: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="SEC Documents Data Update Script")
    parser.add_argument("--test", action="store_true", help="Run in test mode with minimal data")
    args = parser.parse_args()
    
    main(test_mode=args.test)
