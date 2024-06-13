import asyncio
import pandas as pd
from PGDBConnector import PGDBConnector

class PGDataProcessor:
    """Class to execute SQL queries and save results as CSV files."""
    def __init__(self):
        self.db = PGDBConnector()

    def read_query(self, file_path):
        """Read SQL query from a file."""
        with open(file_path, 'r') as file:
            return file.read()

    def save_to_csv(self, data, file_path):
        """Save data to a CSV file."""
        df = pd.DataFrame([dict(row) for row in data])
        df.to_csv(file_path, index=False)

    async def execute_and_save(self, query_file, output_file):
        """Execute a SQL query and save the result as a CSV file."""
        query = self.read_query(query_file)
        data = await self.db.fetch(query)
        self.save_to_csv(data, output_file)
        print(f"Data saved to {output_file}")
        return data

    async def basic_process_data(self, data):
        """Process the data fetched from the database."""
        return data

    def standard_process_data_sub(self, data):
        """Standard Process the data fetched from the database."""
        return data

    def standard_process_data(self, data):
        """Standard Process the data fetched from the database."""
        data = self.standard_process_data_sub(data)
        return data

    async def download_data_from_sql(self, query_file, raw_file_path):
        """Function to execute queries and save results."""
        await self.db.init_pool()

        try:
            data = await self.execute_and_save(query_file, raw_file_path)
            data = await self.basic_process_data(data)
        finally:
            await self.db.close_pool()

        return data

    async def download_and_process(self, query_file, raw_file_path, processed_file_path):
        """Main function to execute queries, save results, and process data."""
        data = await self.download_data_from_sql(query_file, raw_file_path)
        processed_data = self.standard_process_data(data)
        processed_df = pd.DataFrame(processed_data)
        processed_df.to_csv(processed_file_path, index=False)

    def main(self):
        """Orchestrator"""
        query_file = 'queries/query1.sql'
        raw_file_path = 'output/query1.csv'
        processed_file_path = 'processed/query1.csv'
        asyncio.run(self.download_and_process(query_file, raw_file_path, processed_file_path))

if __name__ == "__main__":
    executor = PGDataProcessor()
    executor.main()
