import streamlit as st
import pandas as pd
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
    filter_condition="rank <=100"
    df=get_data_from_table(db_url, table_name,filter_condition)
    return df

def get_crypto_names():
    query = f"SELECT DISTINCT name from {table_name} where rank <=100"
    df = create_query(db_url, query)
    return df['name'].tolist()

def make_dropdown():
    query = f"SELECT DISTINCT name from {table_name} where rank <=100"
    df = create_query(db_url, query)
    unique_names = (df['name'].tolist())
    option = st.selectbox("select a crypto", unique_names)
    st.write("Selected Crypto:", option)
    return option

def realtime_chart():
    selected_crypto=make_dropdown()
    df=create_query(db_url,f"select * from {table_name} where name={set}")
    fig = px.line(
        df,
        x='timestamp',
        y='price',
        title=f'{selected_crypto} Price Over Time',
        template='plotly_dark'  
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        hovermode='x unified',
        height=600,
        width=None
    )

    fig.update_traces(
        line=dict(width=2),
        hovertemplate='<b>Price:</b> $%{y:.2f}<br>' +
                      '<b>Time:</b> %{x}<extra></extra>'
    )
    
    st.plotly_chart(fig, use_container_width=True)


def app():
    st.title("Realtime Cryptocurrency Dashboard")
    col1, col2 = st.columns([1, 3]) 
    with col1:
        make_dropdown()
    with col2:
        test_db_connection() 
        realtime_chart()
  

if __name__ == "__main__":
    app()
