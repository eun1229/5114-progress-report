import streamlit as st
import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.title("Connection test")

@st.cache_resource
def get_conn():
    with open(st.secrets["SF_PRIVATE_KEY_PATH"], "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        user=st.secrets["SF_USER"],
        account=st.secrets["SF_ACCOUNT"],
        private_key=private_key_bytes,
        warehouse=st.secrets["SF_WAREHOUSE"],
        database=st.secrets["SF_DATABASE"],
        schema="FINAL_PROJECT_MART"
    )

try:
    conn = get_conn()
    st.success("Connected!")
    df = pd.read_sql("SELECT COUNT(*) AS cnt FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY", conn)
    st.write(f"Rows in occupancy table: {df['CNT'][0]}")
except Exception as e:
    st.error(f"Error: {e}")