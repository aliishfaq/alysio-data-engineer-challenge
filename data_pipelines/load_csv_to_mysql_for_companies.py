import mysql.connector
from mysql.connector import Error
import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors
from pandera.typing import Series
from pandera import Column, Check
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# ################################################################################
# #                           Database Configurations
# ################################################################################
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),  # Default to localhost
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME"),
}

logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("load_csv_to_mysql_for_companies.log"),  # Main log file
        logging.StreamHandler()  # Also log to console
    ]
)

# Dedicated logger for failed rows
fail_logger = logging.getLogger('fail_logger')
fail_logger.setLevel(logging.ERROR)
fail_handler = logging.FileHandler('failed_rows.log')  # Log for failed rows
fail_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
fail_logger.addHandler(fail_handler)
# ################################################################################
# #                           Batch Functions
# ################################################################################

def insert_batch_record(cursor):
    """Insert a new record into the batch table and return its ID."""
    start_time = datetime.now()
    insert_query = """
    INSERT INTO batch (start_time, status)
    VALUES (%s, %s)
    """
    cursor.execute(insert_query, (start_time, 'IN_PROGRESS'))
    return cursor.lastrowid

def update_batch_record(cursor, batch_id):
    """Update the batch record with end time and status."""
    end_time = datetime.now()
    update_query = """
    UPDATE batch
    SET end_time = %s, status = %s
    WHERE id = %s
    """
    cursor.execute(update_query, (end_time, 'COMPLETED', batch_id))
    
# ################################################################################
# #                           Execute Procedures
# ################################################################################

def truncate_staging_tables(cursor):
    """Truncate all staging tables"""
    query = """
    Truncate Table stg_companies
    """
    cursor.execute(query)
    return

def run_validations(cursor):
    """Run Validations"""
    query = """
    CALL run_validations()
    """
    cursor.execute(query)
    return

def upsert_companies(cursor,batch_id):
    """Upsert Companies Data"""
    query = """
    CALL UpsertCompanies(%s)
    """
    cursor.execute(query,(batch_id,))
    return

# ################################################################################
# #                           Processing Functions
# ################################################################################
def load_companies(cursor, file_path):
    """Process and load companies.csv."""
    companies_headers = [
        'id', 
        'name', 
        'domain', 
        'industry', 
        'size', 
        'country', 
        'created_date', 
        'is_customer', 
        'annual_revenue'
    ]
    datetime_columns =[
        'created_date'
    ]
    print(f"Processing: {file_path}")
    load_csv_to_db(cursor, file_path, companies_headers, datetime_columns, 'stg_companies')

# ################################################################################
# #                           Loading Functions
# ################################################################################    

# Define the schema using DataFrameSchema
company_schema = pa.DataFrameSchema({
    "id": pa.Column(pa.String),
    "name": pa.Column(pa.String),
    "domain": pa.Column(pa.String),
    "industry": pa.Column(pa.String),
    "size": pa.Column(pa.String),
    "country": pa.Column(pa.String),
    "created_date": pa.Column(pa.String),
    "is_customer": pa.Column(pa.Bool),
    "annual_revenue": pa.Column(pa.Int),
})

def load_csv_to_db(cursor, file_path, headers, datetime_columns, table_name):
    """Load a CSV file into the specified database table in chunks and validate using pandera."""
    if not os.path.exists(file_path):
        logging.error("The file %s does not exist.", file_path)
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    chunk_size = 10000  # Set the size of chunks to read at once

    try:
        # Initialize a reader to process the CSV file in chunks
        chunk_iter = pd.read_csv(file_path, chunksize=chunk_size)

        for chunk_df in chunk_iter:
            logging.info("Processing chunk with %d rows...", len(chunk_df))

            # Ensure the DataFrame columns match the provided headers
            if list(chunk_df.columns) != headers:
                logging.warning("The columns in the chunk don't match the expected headers.")
                logging.warning("Expected headers: %s", headers)
                logging.warning("Found headers: %s", list(chunk_df.columns))

            # If extra columns exist, log them
            extra_columns = set(chunk_df.columns) - set(headers)
            if extra_columns:
                logging.warning("Extra columns found in the chunk: %s", extra_columns)

            # Align the DataFrame to the expected headers (if any columns are missing, they will be NaN)
            chunk_df = chunk_df[headers]
            logging.info("Aligned chunk DataFrame columns to the expected headers.")

            # Validate the chunk DataFrame using pandera
            try:
                # Use pandera to validate the chunk
                company_schema.validate(chunk_df)
                logging.info("Chunk data is valid according to pandera schema.")
            except SchemaErrors as e:
                logging.error("Validation failed for chunk: %s", e)
                continue  # Skip this chunk if validation fails

            # Convert datetime columns to the desired format
            for col in datetime_columns:
                chunk_df[col] = pd.to_datetime(chunk_df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                logging.info("Formatted datetime column: %s", col)

            # Create placeholders for the insert query
            placeholders = ', '.join(['%s'] * len(chunk_df.columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"

            # Collect all rows for batch insert
            rows_to_insert = [tuple(row) for _, row in chunk_df.iterrows()]

            try:
                # Use executemany for batch insert
                cursor.executemany(insert_query, rows_to_insert)
                logging.info("Batch insert successful: %d rows inserted into %s.", len(rows_to_insert), table_name)
            except Error as e:
                logging.error("Error during batch insert for chunk: %s", e)

            # Log failed rows to a separate log file
            for idx, row in chunk_df.iterrows():
                try:
                    cursor.execute(insert_query, tuple(row))
                except Error as e:
                    fail_logger.error("Failed to insert row %d: %s. Error: %s", idx, row.to_dict(), e)

    except Exception as e:
        logging.critical("Error loading CSV file %s: %s", file_path, e)


# ################################################################################
# #                           Main Function
# ################################################################################
def Companies_csv_to_DB():
    directory_path = 'data/salesforce'
    batch_id = None
    
    try:
        with mysql.connector.connect(**DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                batch_id = insert_batch_record(cursor)
                truncate_staging_tables(cursor)
                connection.commit()
                logging.info("Batch record created with ID: %d", batch_id)
    
                file_name = "companies.csv"
                file_path = os.path.join(directory_path, file_name)
                logging.info("Processing file: %s", file_name)
                load_companies(cursor, file_path)
    
                run_validations(cursor)
                logging.info("Validations completed.")
    
                upsert_companies(cursor, batch_id)
                logging.info("Procedure executed: upsert_companies.")
                
                update_batch_record(cursor, batch_id)
                connection.commit()
                logging.info("Batch with ID %d loaded successfully.", batch_id)
    
    except mysql.connector.Error as err:
        logging.error("Database error occurred: %s", err)
        if 'connection' in locals() and connection.is_connected():
            connection.rollback()
            logging.warning("Transaction rolled back due to error.")
            
    except Exception as ex:
        logging.critical("An unexpected error occurred: %s", ex)
        if 'batch_id' in locals():
            with mysql.connector.connect(**DB_CONFIG) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE batch
                        SET status = %s, exceptions = %s
                        WHERE id = %s
                    """, ('FAILED', f"Error: {err}", batch_id))
                    connection.commit()

if __name__ == '__main__':
    main()
