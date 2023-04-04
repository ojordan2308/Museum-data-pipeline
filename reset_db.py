from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from main import get_db_connection

if __name__ == "__main__":
    config = dotenv_values()
    conn = get_db_connection(config)
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        query = """DELETE FROM exhibition_rating;
                   DELETE FROM exhibition_help;"""
        curs.execute(query)
