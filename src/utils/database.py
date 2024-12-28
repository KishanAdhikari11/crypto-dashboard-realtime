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
        df.to_sql(table_name,engine,if_exists='append',index=False)
        print(f"Data appended in table: {table_name}")
    except Exception as e:
        print(f"Error storing data: {e}")
    
