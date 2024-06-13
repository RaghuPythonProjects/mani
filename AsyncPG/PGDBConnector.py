import asyncio
import asyncpg
from dotenv import load_dotenv
import os
import logging
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PGDBConnector:
    """
    Database class for interacting with a PostgreSQL database using asyncpg.

    This class handles creating a connection pool, executing queries, and closing the pool.
    """

    def __init__(self):
        """
        Initialize the Database class by loading database connection details from environment variables.
        """
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.database = os.getenv("DB_NAME")
        self.pool = None

    async def init_pool(self):
        """
        Initialize the connection pool for the database.

        Creates a connection pool with specified parameters for managing multiple connections.
        """
        try:
            self.pool = await asyncpg.create_pool(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                min_size=1,
                max_size=10
            )
            logger.info("Database pool created successfully")
        except Exception as e:
            logger.error(f"Error creating database pool: {e}")
            raise

    async def fetch(self, query, *args):
        """
        Execute a SELECT query and return all results.

        Parameters:
        query (str): The SQL query to execute.
        *args: Arguments to pass to the query.

        Returns:
        List[Record]: A list of records returned by the query.
        """
        async with self.pool.acquire() as connection:
            try:
                return await connection.fetch(query, *args)
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                raise

    async def fetchrow(self, query, *args):
        """
        Execute a SELECT query and return a single row.

        Parameters:
        query (str): The SQL query to execute.
        *args: Arguments to pass to the query.

        Returns:
        Record: A single record returned by the query.
        """
        async with self.pool.acquire() as connection:
            try:
                return await connection.fetchrow(query, *args)
            except Exception as e:
                logger.error(f"Error fetching row: {e}")
                raise

    async def execute(self, query, *args):
        """
        Execute a SQL command (INSERT, UPDATE, DELETE).

        Parameters:
        query (str): The SQL command to execute.
        *args: Arguments to pass to the command.

        Returns:
        str: The result of the command execution.
        """
        async with self.pool.acquire() as connection:
            try:
                return await connection.execute(query, *args)
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise

    async def close_pool(self):
        """
        Close the connection pool.

        This method should be called when the application is shutting down to ensure
        that all connections are properly closed.
        """
        try:
            await self.pool.close()
            logger.info("Database pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing database pool: {e}")
            raise

    async def read_query_from_file(self, file_path):
        """
        Read SQL query from a file.

        Parameters:
        file_path (str): The path to the SQL file.

        Returns:
        str: The SQL query as a string.
        """
        try:
            with open(file_path, 'r') as file:
                query = file.read()
            logger.info(f"Query read from {file_path}")
            return query
        except Exception as e:
            logger.error(f"Error reading query from file {file_path}: {e}")
            raise

    async def save_to_csv(self, data, output_file):
        """
        Save query results to a CSV file.

        Parameters:
        data (List[Record]): The data to save.
        output_file (str): The path to the output CSV file.
        """
        try:
            df = pd.DataFrame([dict(record) for record in data])
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data to CSV {output_file}: {e}")
            raise


async def main():
    """
    Example main function to demonstrate the usage of the Database class.

    This function initializes the connection pool, executes queries from files,
    saves the results to CSV files, and closes the connection pool.
    """
    db = PGDBConnector()
    await db.init_pool()

    # List of SQL files and corresponding output CSV files
    queries = [
        ("queries/query1.sql", "output/query1.csv"),
        ("queries/query2.sql", "output/query2.csv")
    ]

    for query_file, output_file in queries:
        try:
            query = await db.read_query_from_file(query_file)
            data = await db.fetch(query)
            await db.save_to_csv(data, output_file)
        except Exception as e:
            logger.error(f"Error processing {query_file}: {e}")

    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
