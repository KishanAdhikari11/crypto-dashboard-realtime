import streamlit as st
import pandas as pd
import time
import plotly.express as px
from components.get_data import get_data_from_table, test_data_from_table,create_query
db_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/crypto"
table_name = "cryptocurrency_data"

st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="📊",
    layout="wide"
)

@st.cache_data
def test_db_connection():
    sample_data = test_data_from_table(db_url, table_name)
    if sample_data is not None:
        st.success("Database connection successful.")
        st.write(sample_data)
        return sample_data
    else:
        st.error("Failed to connect to the database.")
        return None

@st.cache_data
def fetch_data():
    filter_condition=" Where rank <=100"
    df=get_data_from_table(db_url, table_name,filter_condition)
    return df

def get_crypto_names():
    query = f"SELECT DISTINCT name from {table_name} where rank <=100"
    df = create_query(db_url, query)
    return df['name'].tolist()

def make_dropdown():
    unique_names =get_crypto_names()
    option = st.selectbox("select a crypto", unique_names)
    st.write("Selected Crypto:", option)
    return option

def realtime_chart(selected_crypto):
    df=create_query(db_url,f"select name,price,last_updated from {table_name} where name='{selected_crypto}'")
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    df = df.sort_values(by='last_updated', ascending=True)
    fig = px.line(
        df,
        x='last_updated',
        y='price',
        title=f'{selected_crypto} Price Over Time',
        template='plotly_dark'  
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        hovermode='x unified',
        height=600,
    
    )
    st.plotly_chart(fig, use_container_width=True)


def market_cap_pie_chart():
    query = """
    SELECT 
        name, 
        MAX(market_cap) AS max_market_cap
    FROM 
        cryptocurrency_data
    GROUP BY 
        name
    ORDER BY 
        max_market_cap DESC
    LIMIT 10;
    """
    top_10 = create_query(db_url,query)
    fig = px.pie(
        top_10, 
        names='name', 
        values='max_market_cap', 
        title='Top 10 Cryptocurrencies by Market Cap',
        template='plotly_dark'
    )
    st.plotly_chart(fig, use_container_width=True)

# most fluctuating crypto in last 24 h
def most_fluctuating():
    query="""select distinct name,symbol,rank,price,percent_change_24h,last_updated from(select name,symbol,rank,price,percent_change_24h,last_updated from cryptocurrency_data order by last_updated desc limit 2000)latest_crypto order by percent_change_24h desc limit 10"""
    top_10=create_query(db_url,query)
    if top_10 is not None and not top_10.empty:
        fig=px.bar(top_10,x='name',y='percent_change_24h',title="most fluctuating crypto in 24h",template='plotly_dark')
        st.plotly_chart(fig,use_container_width=True)
    else:
         st.error("No data available for the query")
 

# top 10 crypto with percentage change from ath


# top dominace crypto




def app():
    st.title("Realtime Cryptocurrency Dashboard")
    col1, col2 = st.columns([1, 3]) 
    with col1:
        
        selected=make_dropdown()
    with col2:
        test_db_connection() 
        realtime_chart(selected)
        market_cap_pie_chart()
        most_fluctuating()
        
                
  

if __name__ == "__main__":
    app()
