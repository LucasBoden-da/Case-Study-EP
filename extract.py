import pandas as pd 
from sqlalchemy import create_engine 
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load database and AWS credentials from environment variables
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DATABASE')

def extract_burritos_tacos_count():
    """
        Calculates the total number of burritos and tacos for each city, province, and country, 
        along with the burrito-to-taco ratio. 

        The result is saved as a CSV file named `burritos_tacos_count_per_city<florida_timestamp>.csv`.

        Returns:
        - str: The file path of the generated CSV if successful.
        - None: If an error occurs.
    """

    conn_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    try:
        engine = create_engine(conn_string)
    except Exception as e:
        logging.error(f"Couldn't connect to Database: {e}")
        return None
    
    sql_query = """WITH burritos_tacos_count_per_city AS (
    SELECT 
        l.city,
        l.province,
        l.country,
        COUNT(case when LOWER(mop.menus_name) LIKE '%taco%' then 1 end) AS total_tacos,
        COUNt(case when LOWER(mop.menus_name) LIKE '%burrito%' then 1 end) AS total_burritos
    FROM locations l
    LEFT JOIN menu_options_prices mop ON l.id = mop.id
    GROUP BY l.city, l.province, l.country
)
SELECT 
    city, 
    province,
    country,
    total_tacos, 
    total_burritos, 
    IIF(total_tacos = 0, NULL, ROUND((CAST(total_burritos AS FLOAT) / total_tacos),2)) AS burrito_to_taco_ratio
FROM burritos_tacos_count_per_city;"""

    try:
        df = pd.read_sql(sql_query, con=engine)
        if df.empty:
            logging.warning("No data returned from query.")
            return None
        # Ensure that the timestamp is generated only once per script execution
        florida_tz = ZoneInfo("America/New_York")
        timestamp = datetime.now(florida_tz).strftime("%Y%m%d_%H%M%S")

        # Save the extracted data as a CSV file
        file_path = f"burritos_tacos_count_per_city_{timestamp}.csv"
        df.to_csv(file_path, index=False)

        logging.info(f"File succesfully created and saved locally: {file_path}")
        return file_path

    except SQLAlchemyError as e:
        logging.error(f"Error while executing the SQL query: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return None
