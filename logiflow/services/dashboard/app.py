"""
services/dashboard/app.py

Streamlit analytics dashboard over the warehouse.

Depends on: common.config (1). Reads whatever pipelines/etl.py (5) has
loaded -- no separate db_connector.py, no duplicated connection logic.
"""

import pandas as pd
import plotly.express as px
import streamlit as st
from common.config import settings
from sqlalchemy import create_engine

st.set_page_config(page_title="LogiFlow Analytics", page_icon="🚚", layout="wide")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    engine = create_engine(settings.database_url)
    query = """
        SELECT
            f.shipment_id, f.status, f.is_delayed, f.delay_minutes, f.cost_usd,
            f.weight_kg, f.distance_km, f.fuel_consumed_liters,
            f.weather_condition, f.temperature_celsius, f.wind_speed_kmh,
            f.traffic_congestion_ratio, f.traffic_condition,
            d.full_date, d.month, d.month_name, d.quarter, d.year, d.weekday, d.is_weekend,
            c.company_name, c.industry, c.segment,
            dr.full_name AS driver_name, dr.rating AS driver_rating,
            v.vehicle_type,
            r.origin_city, r.destination_city, r.region, r.route_type
        FROM fact_shipments f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_customer c ON f.customer_id = c.customer_id
        JOIN dim_driver dr ON f.driver_id = dr.driver_id
        JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
        JOIN dim_route r ON f.route_id = r.route_id
    """
    return pd.read_sql(query, engine)


with st.spinner("Loading warehouse data..."):
    df = load_data()

with st.sidebar:
    st.title("🚚 LogiFlow")
    st.caption("Logistics Analytics Platform")
    st.divider()
    years = st.multiselect("Year", sorted(df["year"].unique()), default=sorted(df["year"].unique()))
    statuses = st.multiselect("Status", df["status"].unique().tolist(), default=df["status"].unique().tolist())
    regions = st.multiselect("Region", df["region"].unique().tolist(), default=df["region"].unique().tolist())
    st.divider()
    st.caption(f"{len(df):,} total shipments loaded")

filtered = df[df["year"].isin(years) & df["status"].isin(statuses) & df["region"].isin(regions)]

if filtered.empty:
    st.warning("No data matches your filters.")
    st.stop()

st.title("🚚 LogiFlow — Logistics Analytics")
st.caption(f"Showing **{len(filtered):,}** shipments")
st.divider()

total = len(filtered)
on_time_pct = (filtered["status"] == "on_time").sum() / total * 100
avg_delay = filtered.loc[filtered["is_delayed"], "delay_minutes"].mean()
total_cost = filtered["cost_usd"].sum()
failed_pct = (filtered["status"] == "failed").sum() / total * 100

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Shipments", f"{total:,}")
k2.metric("On-Time Rate", f"{on_time_pct:.1f}%")
k3.metric("Avg Delay (delayed)", f"{avg_delay:.0f}m")
k4.metric("Total Revenue", f"${total_cost:,.0f}")
k5.metric("Failure Rate", f"{failed_pct:.1f}%")
st.divider()

st.subheader("Delivery Performance")
col1, col2, col3 = st.columns(3)
with col1:
    counts = filtered["status"].value_counts().reset_index()
    counts.columns = ["status", "count"]
    fig = px.pie(counts, values="count", names="status", hole=0.55,
                 color="status", color_discrete_map={"on_time": "#10b981", "delayed": "#f59e0b", "failed": "#ef4444"},
                 title="Shipment Status")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    monthly = filtered.groupby(["year", "month", "month_name"]).agg(
        total=("shipment_id", "count"), on_time=("status", lambda x: (x == "on_time").sum())
    ).reset_index()
    monthly["on_time_pct"] = monthly["on_time"] / monthly["total"] * 100
    monthly["period"] = monthly["month_name"].str[:3] + " " + monthly["year"].astype(str)
    fig = px.line(monthly.sort_values(["year", "month"]), x="period", y="on_time_pct",
                  markers=True, title="Monthly On-Time Rate (%)")
    st.plotly_chart(fig, use_container_width=True)
with col3:
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    wk = filtered.groupby("weekday")["is_delayed"].mean().reset_index()
    wk["is_delayed"] = wk["is_delayed"] * 100
    wk["weekday"] = pd.Categorical(wk["weekday"], categories=weekday_order, ordered=True)
    fig = px.bar(wk.sort_values("weekday"), x="weekday", y="is_delayed",
                 title="Delay Rate by Weekday (%)", color="is_delayed", color_continuous_scale="RdYlGn_r")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
st.divider()

st.subheader("Cost & Revenue")
col1, col2 = st.columns(2)
with col1:
    seg = filtered.groupby("segment")["cost_usd"].sum().reset_index()
    fig = px.bar(seg.sort_values("cost_usd"), x="cost_usd", y="segment", orientation="h",
                 title="Revenue by Segment", color="cost_usd", color_continuous_scale="Purples")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
with col2:
    monthly_rev = filtered.groupby(["year", "month", "month_name"])["cost_usd"].sum().reset_index()
    monthly_rev["period"] = monthly_rev["month_name"].str[:3] + " " + monthly_rev["year"].astype(str)
    fig = px.area(monthly_rev.sort_values(["year", "month"]), x="period", y="cost_usd",
                  title="Monthly Revenue Trend")
    st.plotly_chart(fig, use_container_width=True)
st.divider()

st.subheader("Driver & Vehicle Performance")
col1, col2 = st.columns(2)
with col1:
    dp = filtered.groupby("driver_name").agg(
        total=("shipment_id", "count"), on_time=("status", lambda x: (x == "on_time").sum()),
        avg_rating=("driver_rating", "mean")
    ).reset_index()
    dp["on_time_pct"] = dp["on_time"] / dp["total"] * 100
    top = dp[dp["total"] >= 5].nlargest(10, "on_time_pct") if (dp["total"] >= 5).any() else dp
    fig = px.bar(top.sort_values("on_time_pct"), x="on_time_pct", y="driver_name", orientation="h",
                 title="Top Drivers — On-Time Rate", color="on_time_pct", color_continuous_scale="Greens")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
with col2:
    vp = filtered.groupby("vehicle_type").agg(
        count=("shipment_id", "count"), delay_rate=("is_delayed", "mean")
    ).reset_index()
    vp["delay_rate"] = vp["delay_rate"] * 100
    fig = px.bar(vp, x="vehicle_type", y="delay_rate", title="Delay Rate by Vehicle Type (%)",
                 color="delay_rate", color_continuous_scale="RdYlGn_r")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
st.divider()

st.subheader("Weather & Traffic Impact")
col1, col2 = st.columns(2)
with col1:
    wp = filtered.groupby("weather_condition").agg(
        count=("shipment_id", "count"), delay_rate=("is_delayed", "mean")
    ).reset_index()
    wp["delay_rate"] = wp["delay_rate"] * 100
    fig = px.bar(wp.sort_values("delay_rate", ascending=False), x="weather_condition", y="delay_rate",
                 title="Delay Rate by Weather (%)", color="delay_rate", color_continuous_scale="RdYlBu_r")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
with col2:
    tp = filtered.groupby("traffic_condition").agg(
        count=("shipment_id", "count"), delay_rate=("is_delayed", "mean")
    ).reset_index()
    tp["delay_rate"] = tp["delay_rate"] * 100
    order = ["LOW", "MEDIUM", "HIGH"]
    tp["traffic_condition"] = pd.Categorical(tp["traffic_condition"], categories=order, ordered=True)
    fig = px.bar(tp.sort_values("traffic_condition"), x="traffic_condition", y="delay_rate",
                 title="Delay Rate by Traffic Congestion (%)", color="delay_rate", color_continuous_scale="RdYlGn_r")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
st.divider()

st.subheader("Raw Data Explorer")
cols_to_show = ["shipment_id", "full_date", "status", "delay_minutes", "company_name",
                 "driver_name", "vehicle_type", "origin_city", "destination_city",
                 "distance_km", "cost_usd", "weather_condition", "traffic_condition"]
cols_to_show = [c for c in cols_to_show if c in filtered.columns]
st.dataframe(filtered[cols_to_show].sort_values("full_date", ascending=False), use_container_width=True, height=350)
st.download_button("Download filtered data as CSV", data=filtered[cols_to_show].to_csv(index=False),
                    file_name="logiflow_filtered.csv", mime="text/csv")

st.divider()
st.caption("🚚 LogiFlow Analytics")