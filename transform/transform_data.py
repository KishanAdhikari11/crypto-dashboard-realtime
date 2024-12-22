import pandas as pd
def transform_data(input_csv):
    """ 
    Transforms and preprocesses cryptocurrency data by sorting it and handling missing values.

    input: str
        The path to the input CSV file containing cryptocurrency data.
    Returns:
        pandas.Dataframe"""
    data=pd.read_csv(input_csv)
    data=data.dropna()
    data=data.sort_values(by='id')
    return data


    