import os

import clickhouse_connect
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="Flash Sale Performance", layout="wide")
st.title("Flash Sale Performance")
st.caption("Live analytics from the OneShop purchases CDC stream")

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DATABASE", "oneshop"),
)

st.header("Hourly sales volume (last 24 hours)")
hourly_sales = client.query_df(
    """
    SELECT
        toStartOfHour(created_at) AS hour,
        sum(quantity) AS total_sales
    FROM oneshop.purchases FINAL
    WHERE created_at >= now64(6, 'UTC') - INTERVAL 24 HOUR
      AND created_at <= now64(6, 'UTC')
    GROUP BY hour
    ORDER BY hour
    """
)
if hourly_sales.empty:
    st.info("No purchase events have arrived in ClickHouse yet.")
else:
    figure = px.bar(
        hourly_sales,
        x="hour",
        y="total_sales",
        title="Units sold per hour",
        labels={"hour": "Hour (UTC)", "total_sales": "Units sold"},
    )
    st.plotly_chart(figure, use_container_width=True)

st.header("Top 10 items by revenue")
top_items = client.query_df(
    """
    SELECT
        item_id,
        sum(quantity) AS total_quantity,
        sum(purchase_price) AS total_revenue
    FROM oneshop.purchases FINAL
    GROUP BY item_id
    ORDER BY total_revenue DESC
    LIMIT 10
    """
)
st.dataframe(top_items, use_container_width=True)
client.close()
