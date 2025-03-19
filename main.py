"""
ETL Pipeline Example Usage
-------------------------
This file demonstrates how to use the ETL pipeline with sample data
"""

import os
import csv
from etl_pipeline import ETLPipeline

# Example usage
if __name__ == "__main__":
    # Generate a sample source CSV for testing
    def generate_sample_data(filename: str, num_rows: int = 100) -> None:
        """Generate a sample CSV file for testing"""
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write header
            writer.writerow(['id', 'name', 'age', 'email', 'purchase_amount'])
            
            # Write data rows
            for i in range(1, num_rows + 1):
                # Add some missing values for testing
                if i % 10 == 0:  # Every 10th row has a missing value
                    writer.writerow([i, f'Person {i}', '', f'person{i}@example.com', round(50 + i * 1.5, 2)])
                else:
                    writer.writerow([i, f'Person {i}', 20 + (i % 40), f'person{i}@example.com', round(50 + i * 1.5, 2)])
        
        print(f"Generated sample data file: {filename} with {num_rows} rows")
    
    # Generate sample data
    sample_file = "sample_data.csv"
    output_file = "processed_data.csv"
    generate_sample_data(sample_file)
    
    # Create and run the ETL pipeline
    pipeline = ETLPipeline(sample_file, output_file)
    results = pipeline.run()
    
    print(f"Pipeline completed with results: {results}") 