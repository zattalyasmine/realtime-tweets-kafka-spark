# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import os
from streamlit_autorefresh import st_autorefresh

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Tweet Sentiment Dashboard",
    page_icon="🐦",
    layout="wide"
)

st.title("📊 Dashboard Analyse de Tweets en Temps Réel")
st.markdown("**Spark Structured Streaming + Kafka + SQLite**")

# 🔁 Auto-refresh toutes les 5 secondes
st_autorefresh(interval=5000, key="refresh")

DB_PATH = "tweets.db"

# ─────────────────────────────────────────────
# LOAD DATA (OPTIMISÉ)
# ─────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)

    try:
        query = """
        SELECT id, text, sentiment, timestamp
        FROM tweets
        ORDER BY timestamp DESC
        LIMIT 100000
        """
        df = pd.read_sql(query, conn)
    except:
        df = pd.DataFrame()

    conn.close()
    return df


df = load_data()

# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────
if df.empty:
    st.warning("⏳ En attente des données... Lance spark_consumer.py")
    st.stop()

# KPI
total = len(df)
positifs = (df['sentiment'] == 'POSITIVE').sum()
negatifs = (df['sentiment'] == 'NEGATIVE').sum()
neutres  = (df['sentiment'] == 'NEUTRAL').sum()

st.subheader("📌 Indicateurs Clés")
c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Tweets", total)
c2.metric("Positifs", positifs, f"{round(positifs/total*100)}%")
c3.metric("Négatifs", negatifs, f"{round(negatifs/total*100)}%")
c4.metric("Neutres", neutres)

st.markdown("---")

# DATA PREP
colors = {
    'POSITIVE': '#2ecc71',
    'NEGATIVE': '#e74c3c',
    'NEUTRAL':  '#95a5a6'
}

counts = df['sentiment'].value_counts().reset_index()
counts.columns = ['Sentiment', 'Count']

col1, col2 = st.columns(2)

# ─────────────────────────────────────────────
# PIE CHART
# ─────────────────────────────────────────────
with col1:
    st.subheader("📊 Répartition des Sentiments")
    fig_pie = px.pie(
        counts,
        values='Count',
        names='Sentiment',
        color='Sentiment',
        color_discrete_map=colors,
        hole=0.4
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ─────────────────────────────────────────────
# BAR CHART
# ─────────────────────────────────────────────
with col2:
    st.subheader("📊 Tweets par Sentiment")
    fig_bar = px.bar(
        counts,
        x='Sentiment',
        y='Count',
        color='Sentiment',
        color_discrete_map=colors,
        text='Count'
    )
    fig_bar.update_traces(textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────
# TIME SERIES
# ─────────────────────────────────────────────
st.subheader("📈 Évolution dans le Temps")

df['timestamp'] = pd.to_datetime(df['timestamp'])

df_time = df.groupby(
    [pd.Grouper(key='timestamp', freq='1s'), 'sentiment']
).size().reset_index(name='count')

fig_line = px.line(
    df_time,
    x='timestamp',
    y='count',
    color='sentiment',
    color_discrete_map=colors,
    markers=True
)

st.plotly_chart(fig_line, use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────
# LAST TWEETS
# ─────────────────────────────────────────────
st.subheader("📝 Derniers Tweets Analysés")

st.dataframe(
    df.sort_values("timestamp", ascending=False)
      .head(10),
    use_container_width=True
)