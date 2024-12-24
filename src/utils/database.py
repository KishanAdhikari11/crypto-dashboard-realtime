import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.transform.transform_data import transform_data
from sqlalchemy import create_engine


def store_data_in_postgres(df,table_name,db_url):
    """
    To store data in postgresdatabase
    input:
     df : pandas.DataFrame
        Data to be stored.
    table_name : str
        Name of the target table in PostgreSQL.
    db_url : str
        Database connection URL.
    """
    try:
        engine=create_engine(db_url)
        df.to_sql(table_name,engine,if_exists='replace',index=False)
        print(f"Data stored in table: {table_name}")
    except Exception as e:
        print(f"Error storing data: {e}")
    
if __name__ =="__main__":
    input_csv="data/crypto_data.csv"
    db_url="postgresql+psycopg2://postgres:postgres@localhost:5432/crypto"
    table_name="cryptocurrency_data"
    transform_data=transform_data(input_csv)
    store_data_in_postgres(transform_data,table_name,db_url)