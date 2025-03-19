"""
ETL Pipeline for CSV Processing
------------------------------
A foundation-level data engineering project that demonstrates:
1. Data extraction from CSV files
2. Data transformation with validation, cleaning and enrichment
3. Data loading to a target CSV and SQLite database
4. Proper logging and error handling
"""

import os
import csv
import sqlite3
import logging
import datetime
import pandas as pd
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_pipeline")

class ETLPipeline:
    """Simple ETL pipeline for processing CSV data"""
    
    def __init__(self, source_path: str, target_path: str, db_path: str = "etl_output.db"):
        """
        Initialize the ETL pipeline
        
        Args:
            source_path: Path to source CSV file
            target_path: Path to target CSV file
            db_path: Path to SQLite database
        """
        self.source_path = source_path
        self.target_path = target_path
        self.db_path = db_path
        self.metrics = {
            "rows_processed": 0,
            "rows_filtered": 0,
            "rows_with_errors": 0,
            "start_time": None,
            "end_time": None
        }
        logger.info(f"ETL Pipeline initialized with source: {source_path}, target: {target_path}")
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from source CSV file
        
        Returns:
            DataFrame containing source data
        """
        try:
            logger.info(f"Extracting data from {self.source_path}")
            # Using pandas to simplify reading CSV
            df = pd.read_csv(self.source_path)
            logger.info(f"Extracted {len(df)} rows from source")
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the extracted data
        
        Args:
            data: DataFrame containing source data
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        # Track original row count
        original_count = len(data)
        
        # 1. Data cleaning - Remove rows with missing values in critical columns
        # For this example, we'll consider all columns as critical
        data_cleaned = data.dropna()
        rows_filtered = original_count - len(data_cleaned)
        self.metrics["rows_filtered"] += rows_filtered
        
        if rows_filtered > 0:
            logger.info(f"Removed {rows_filtered} rows with missing values")
        
        # 2. Data validation - Ensure types are correct
        # For this example, we'll convert appropriate string columns to numeric
        numeric_columns = data_cleaned.select_dtypes(include=['object']).columns
        
        for col in numeric_columns:
            # Try to convert columns with numeric-looking values to numeric
            try:
                # Check if column has numeric values by attempting conversion
                pd.to_numeric(data_cleaned[col], errors='raise')
                # If successful, convert the column
                data_cleaned[col] = pd.to_numeric(data_cleaned[col])
                logger.info(f"Converted column {col} to numeric type")
            except:
                # If conversion fails, this column is likely not numeric
                pass
        
        # 3. Data enrichment - Add metadata columns
        data_cleaned['etl_timestamp'] = datetime.datetime.now().isoformat()
        data_cleaned['source_file'] = os.path.basename(self.source_path)
        
        # 4. Additional transformations (example)
        # For demonstration, let's add a new column that joins two existing text columns
        # This is just an example - in reality, you'd apply business-specific transformations
        text_columns = data_cleaned.select_dtypes(include=['object']).columns[:2]
        if len(text_columns) >= 2:
            col1, col2 = text_columns[0], text_columns[1]
            data_cleaned['combined_field'] = data_cleaned[col1].astype(str) + " - " + data_cleaned[col2].astype(str)
            logger.info(f"Created new column 'combined_field' from {col1} and {col2}")
        
        self.metrics["rows_processed"] = len(data_cleaned)
        logger.info(f"Transformation complete. {len(data_cleaned)} rows processed.")
        
        return data_cleaned
    
    def load_to_csv(self, data: pd.DataFrame) -> None:
        """
        Load transformed data to target CSV file
        
        Args:
            data: DataFrame to load
        """
        try:
            logger.info(f"Loading data to CSV: {self.target_path}")
            data.to_csv(self.target_path, index=False)
            logger.info(f"Successfully loaded {len(data)} rows to {self.target_path}")
        except Exception as e:
            logger.error(f"Error loading data to CSV: {str(e)}")
            raise
    
    def load_to_db(self, data: pd.DataFrame, table_name: str = "processed_data") -> None:
        """
        Load transformed data to SQLite database
        
        Args:
            data: DataFrame to load
            table_name: Name of the target table
        """
        try:
            logger.info(f"Loading data to SQLite DB: {self.db_path}, table: {table_name}")
            
            # Create connection to SQLite DB
            conn = sqlite3.connect(self.db_path)
            
            # Write DataFrame to SQL table
            data.to_sql(table_name, conn, if_exists='replace', index=False)
            
            # Verify the data was loaded correctly
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            conn.close()
            
            logger.info(f"Successfully loaded {count} rows to DB table {table_name}")
        except Exception as e:
            logger.error(f"Error loading data to database: {str(e)}")
            raise
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full ETL pipeline
        
        Returns:
            Dictionary containing pipeline metrics
        """
        self.metrics["start_time"] = datetime.datetime.now().isoformat()
        logger.info("Starting ETL pipeline execution")
        
        try:
            # Extract
            data = self.extract()
            
            # Transform
            transformed_data = self.transform(data)
            
            # Load
            self.load_to_csv(transformed_data)
            self.load_to_db(transformed_data)
            
            self.metrics["end_time"] = datetime.datetime.now().isoformat()
            logger.info("ETL pipeline completed successfully")
            
            # Calculate execution time
            start = datetime.datetime.fromisoformat(self.metrics["start_time"])
            end = datetime.datetime.fromisoformat(self.metrics["end_time"])
            execution_time = (end - start).total_seconds()
            self.metrics["execution_time_seconds"] = execution_time
            
            logger.info(f"Pipeline metrics: {self.metrics}")
            return self.metrics
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.metrics["end_time"] = datetime.datetime.now().isoformat()
            self.metrics["status"] = "failed"
            self.metrics["error"] = str(e)
            return self.metrics 