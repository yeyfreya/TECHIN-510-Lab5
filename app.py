import streamlit as st
import pandas.io.sql as sqlio
import altair as alt
import folium
from streamlit_folium import st_folium
import pandas as pd
import psycopg2

from db import conn_str  # Ensure this correctly points to your database connection string

st.title("Seattle Events")

# Connect to the database and query data
conn = psycopg2.connect(conn_str)
query = "SELECT *, EXTRACT(MONTH FROM date) AS month, EXTRACT(DOW FROM date) AS weekday FROM events"
df = pd.read_sql_query(query, conn)
conn.close()

# Convert date to datetime and add necessary columns for month name and weekday name
df['date'] = pd.to_datetime(df['date'])
df['month_name'] = df['date'].dt.month_name()
df['weekday_name'] = df['date'].dt.day_name()

# 1. Chart for the most common event categories
st.subheader("Most Common Event Categories")
category_chart = alt.Chart(df).mark_bar().encode(
    x='count():Q',
    y=alt.Y('category:N', sort='-x'),
).properties(width=700)
st.altair_chart(category_chart, use_container_width=True)

# 2. Chart for the month with the most events
st.subheader("Months with Most Events")
month_chart = alt.Chart(df).mark_bar().encode(
    x='count():Q',
    y=alt.Y('month_name:N', sort='-x'),
).properties(width=700)
st.altair_chart(month_chart, use_container_width=True)

# 3. Chart for the day of the week with the most events
st.subheader("Days with Most Events")
weekday_chart = alt.Chart(df).mark_bar().encode(
    x='count():Q',
    y=alt.Y('weekday_name:N', sort='-x'),
).properties(width=700)
st.altair_chart(weekday_chart, use_container_width=True)

# Controls for filtering
st.subheader("Filters")

# Dropdown to filter by category
selected_category = st.selectbox("Select a Category", ['All'] + list(df['category'].unique()))

# Date range selector
selected_date_range = st.date_input("Select Date Range", [])

# Filter by location
selected_location = st.selectbox("Select a Location", ['All'] + list(df['location'].unique()))

# Apply filters to DataFrame
if selected_category != 'All':
    df = df[df['category'] == selected_category]

if selected_date_range:
    df = df[(df['date'] >= selected_date_range[0]) & (df['date'] <= selected_date_range[1])]

if selected_location != 'All':
    df = df[df['location'] == selected_location]

# Display filtered DataFrame
st.write(df)

# Map for event locations
st.subheader("Event Locations on Map")
m = folium.Map(location=[47.6062, -122.3321], zoom_start=10)
for idx, row in df.iterrows():
    if row['latitude'] and row['longitude']:
        folium.Marker(
            [row['latitude'], row['longitude']],
            popup=f"{row['title']}: {row['venue']}",
        ).add_to(m)
st_folium(m, width=700, height=500) 
