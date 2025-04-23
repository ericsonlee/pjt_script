import os
from dotenv import load_dotenv
import mysql.connector
import logging
import time

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

def connect_to_database(max_retries=3, retry_delay=5):
    """Establish connection to the MySQL database using environment variables with retry mechanism."""
    for attempt in range(max_retries):
        try:
            logger.info("Connecting to database...")
            conn = mysql.connector.connect(
                host=os.getenv('DB_HOST'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                port=int(os.getenv('DB_PORT', 3306)),
                ssl_disabled=False,
                ssl_verify_cert=False,
                allow_local_infile=True
            )
            logger.info("Database connection successful")
            return conn
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed (attempt {attempt+1}/{max_retries}): {err}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum connection attempts reached. Exiting.")
                raise

def initialize_refinitiv():
    """Initialize Refinitiv API connection using environment variables."""
    import refinitiv.data as rd
    try:
        logger.info("Initializing Refinitiv API connection...")
        rd.get_config()["http.request-timeout"] = 120
        rd.open_session(app_key=os.getenv('REFINITIV_APP_KEY'))      
        rd.session.get_default().config.set_param("apis.data.datagrid.underlying-platform", "rdp")
        logger.info("Refinitiv API connection successful")
    except Exception as e:
        logger.error(f"Failed to initialize Refinitiv API: {e}")
        raise
