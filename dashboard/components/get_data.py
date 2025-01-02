from sqlalchemy import create_engine
import pandas as pd
def test_data_from_table(db_url,table_name):
    try:
        engine=create_engine(db_url)
        df=pd.read_sql(f"SELECT * from {table_name} limit 10",engine)
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

def get_data_from_table(db_url,table_name,filter_conditions=None):
    try:
        engine=create_engine(db_url)
        query=f"SELECT * from {table_name}"
        if filter_conditions:
            query+=f"{filter_conditions}"
        df=pd.read_sql(query,engine)
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()
    
def create_query(db_url,query):
    try:
        engine=create_engine(db_url)
        df=pd.read_sql(query,engine)
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()
    
