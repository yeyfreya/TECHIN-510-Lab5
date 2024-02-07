import streamlit as st
import pandas.io.sql as sqlio
import altair as alt
import folium
from streamlit_folium import st_folium

from db import conn_str

st.title("Seattle Events")

df = sqlio.read_sql_query("SELECT * FROM events", conn_str)
st.altair_chart(
    alt.Chart(df).mark_bar().encode(x="count()", y=alt.Y("category").sort('-x')).interactive(),
    use_container_width=True,
)

category = st.selectbox("Select a category", df['category'].unique())

m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)
folium.Marker([47.6062, -122.3321], popup='Seattle').add_to(m)
st_folium(m, width=1200, height=600)

df = df[df['category'] == category]
st.write(df)