import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

load_dotenv()

class DatabaseConnection:
    _connection_pool = None

    @classmethod
    def initialize(cls):
        cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

    @classmethod
    def get_connection(cls):
        return cls._connection_pool.getconn()

    @classmethod
    def return_connection(cls, connection):
        cls._connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        cls._connection_pool.closeall()

# Initialize on import
DatabaseConnection.initialize()