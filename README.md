# ETL Pipeline for CSV Processing

A foundation-level data engineering project that demonstrates:
1. Data extraction from CSV files
2. Data transformation with validation, cleaning and enrichment
3. Data loading to a target CSV and SQLite database
4. Proper logging and error handling

## Features

- Extract data from CSV source files
- Transform data with cleaning, validation, and enrichment
- Load processed data to CSV and SQLite database
- Track pipeline metrics and execution statistics
- Comprehensive logging for monitoring and debugging

## Requirements

- Python 3.7+
- pandas
- sqlite3 (included in Python standard library)

## Installation

1. Clone this repository or copy the files to your local machine
2. Install the required dependencies:

```bash
pip install pandas
```

## Usage

### Basic Usage

Run the example script:

```bash
python main.py
```

This will:
1. Generate a sample CSV file
2. Process it through the ETL pipeline
3. Output processed data to a new CSV and SQLite database
4. Display metrics about the operation

### Using Your Own Data

To use your own data, modify the main.py file or create a new script:

```python
from etl_pipeline import ETLPipeline

# Initialize the pipeline with your source and target files
pipeline = ETLPipeline(
    source_path="your_source_data.csv", 
    target_path="your_output_data.csv",
    db_path="your_database.db"
)

# Run the pipeline
results = pipeline.run()
print(f"Pipeline completed with results: {results}")
```

## Pipeline Details

### Extract

- Reads data from a CSV file using pandas

### Transform

- Cleans data by removing rows with missing values
- Validates and converts data types when appropriate
- Enriches data with metadata (timestamp, source file)
- Creates additional derived fields

### Load

- Writes transformed data to CSV
- Loads data into SQLite database for querying

## Output Files

- `processed_data.csv`: The transformed data in CSV format
- `etl_output.db`: SQLite database with processed data
- `etl_pipeline.log`: Detailed log of the pipeline execution

## Metrics

The pipeline tracks various metrics during execution:

- `rows_processed`: Number of rows successfully processed
- `rows_filtered`: Number of rows removed during cleaning
- `rows_with_errors`: Number of rows that caused errors
- `start_time`: Pipeline start timestamp
- `end_time`: Pipeline end timestamp
- `execution_time_seconds`: Total execution time in seconds 