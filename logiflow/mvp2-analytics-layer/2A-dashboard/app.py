import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db_connector import load_shipments

# ─── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="LogiFlow Analytics",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    .kpi-card {
        background: #1e1e2e;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        border-left: 4px solid #7c3aed;
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #a78bfa; }
    .kpi-label { font-size: 0.85rem; color: #94a3b8; margin-top: 4px; }
    .kpi-delta { font-size: 0.8rem; margin-top: 6px; }
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #e2e8f0;
        margin-bottom: 12px;
        padding-bottom: 6px;
        border-bottom: 1px solid #334155;
    }
</style>
""", unsafe_allow_html=True)

# ─── Load Data ────────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_data():
    return load_shipments()

with st.spinner("Loading data from warehouse..."):
    df = get_data()

# ─── Sidebar Filters ──────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/truck.png", width=48)
    st.title("LogiFlow")
    st.caption("Logistics Analytics Platform")
    st.divider()

    st.subheader("🔍 Filters")

    years = st.multiselect(
        "Year",
        options=sorted(df["year"].unique()),
        default=sorted(df["year"].unique())
    )

    statuses = st.multiselect(
        "Status",
        options=df["status"].unique().tolist(),
        default=df["status"].unique().tolist()
    )

    regions = st.multiselect(
        "Region",
        options=df["region"].unique().tolist(),
        default=df["region"].unique().tolist()
    )

    segments = st.multiselect(
        "Customer Segment",
        options=df["segment"].unique().tolist(),
        default=df["segment"].unique().tolist()
    )

    st.divider()
    st.caption(f"📦 {len(df):,} total shipments loaded")

# ─── Apply Filters ────────────────────────────────────────────
filtered = df[
    df["year"].isin(years) &
    df["status"].isin(statuses) &
    df["region"].isin(regions) &
    df["segment"].isin(segments)
]

if filtered.empty:
    st.warning("No data matches your filters. Please adjust the sidebar.")
    st.stop()

# ─── Header ───────────────────────────────────────────────────
st.title("🚚 LogiFlow — Logistics Analytics")
st.caption(f"Showing **{len(filtered):,}** shipments · Last refreshed from PostgreSQL")
st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 1 — KPI CARDS
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">📊 Key Performance Indicators</p>', unsafe_allow_html=True)

total       = len(filtered)
on_time_pct = (filtered["status"] == "on_time").sum() / total * 100
avg_delay   = filtered.loc[filtered["is_delayed"], "delay_minutes"].mean()
total_cost  = filtered["cost_usd"].sum()
avg_cost    = filtered["cost_usd"].mean()
failed_pct  = (filtered["status"] == "failed").sum() / total * 100
avg_rating  = filtered["driver_rating"].mean()
avg_fuel    = filtered["fuel_consumed_liters"].mean()

k1, k2, k3, k4, k5, k6 = st.columns(6)

def kpi(col, value, label, color="#7c3aed"):
    col.markdown(f"""
    <div class="kpi-card" style="border-left-color:{color}">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>""", unsafe_allow_html=True)

kpi(k1, f"{total:,}",         "Total Shipments",    "#7c3aed")
kpi(k2, f"{on_time_pct:.1f}%","On-Time Rate",       "#10b981")
kpi(k3, f"{avg_delay:.0f}m",  "Avg Delay (delayed)","#f59e0b")
kpi(k4, f"${total_cost:,.0f}","Total Revenue (USD)", "#3b82f6")
kpi(k5, f"{failed_pct:.1f}%", "Failure Rate",       "#ef4444")
kpi(k6, f"⭐ {avg_rating:.2f}","Avg Driver Rating",  "#8b5cf6")

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 2 — DELIVERY PERFORMANCE
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">🎯 Delivery Performance</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

# Donut — Status breakdown
with col1:
    status_counts = filtered["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    color_map = {"on_time": "#10b981", "delayed": "#f59e0b", "failed": "#ef4444"}
    fig = px.pie(
        status_counts, values="count", names="status",
        hole=0.55,
        color="status", color_discrete_map=color_map,
        title="Shipment Status"
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)

# Bar — Monthly on-time rate trend
with col2:
    monthly = filtered.groupby(["year", "month", "month_name"]).agg(
        total=("shipment_id", "count"),
        on_time=("status", lambda x: (x == "on_time").sum())
    ).reset_index()
    monthly["on_time_pct"] = monthly["on_time"] / monthly["total"] * 100
    monthly["period"] = monthly["month_name"].str[:3] + " " + monthly["year"].astype(str)
    fig = px.line(
        monthly.sort_values(["year", "month"]),
        x="period", y="on_time_pct",
        markers=True,
        title="Monthly On-Time Rate (%)",
        labels={"on_time_pct": "On-Time %", "period": ""}
    )
    fig.update_traces(line_color="#10b981", line_width=2.5)
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Bar — Delay by weekday
with col3:
    weekday_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    weekday = filtered.groupby("weekday")["is_delayed"].mean().reset_index()
    weekday["is_delayed"] = weekday["is_delayed"] * 100
    weekday["weekday"] = pd.Categorical(weekday["weekday"], categories=weekday_order, ordered=True)
    weekday = weekday.sort_values("weekday")
    fig = px.bar(
        weekday, x="weekday", y="is_delayed",
        title="Delay Rate by Weekday (%)",
        labels={"is_delayed": "Delay Rate %", "weekday": ""},
        color="is_delayed",
        color_continuous_scale="RdYlGn_r"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 3 — COST & REVENUE ANALYSIS
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">💰 Cost & Revenue Analysis</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

# Cost by segment
with col1:
    seg_cost = filtered.groupby("segment")["cost_usd"].sum().reset_index()
    fig = px.bar(
        seg_cost.sort_values("cost_usd", ascending=True),
        x="cost_usd", y="segment", orientation="h",
        title="Total Revenue by Segment",
        labels={"cost_usd": "USD", "segment": ""},
        color="cost_usd", color_continuous_scale="Purples"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Cost vs Distance scatter
with col2:
    sample = filtered.sample(min(1000, len(filtered)), random_state=42)
    fig = px.scatter(
        sample, x="distance_km", y="cost_usd",
        color="route_type",
        title="Cost vs Distance by Route Type",
        labels={"distance_km": "Distance (km)", "cost_usd": "Cost (USD)"},
        opacity=0.6
    )
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Monthly revenue trend
with col3:
    monthly_rev = filtered.groupby(["year", "month", "month_name"])["cost_usd"].sum().reset_index()
    monthly_rev["period"] = monthly_rev["month_name"].str[:3] + " " + monthly_rev["year"].astype(str)
    fig = px.area(
        monthly_rev.sort_values(["year", "month"]),
        x="period", y="cost_usd",
        title="Monthly Revenue Trend",
        labels={"cost_usd": "Revenue (USD)", "period": ""}
    )
    fig.update_traces(line_color="#3b82f6", fillcolor="rgba(59,130,246,0.15)")
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 4 — ROUTE & GEOGRAPHY
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">🗺️ Route & Geography</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

# Top routes by volume
with col1:
    filtered["route_label"] = filtered["origin_city"] + " → " + filtered["destination_city"]
    top_routes = filtered["route_label"].value_counts().head(10).reset_index()
    top_routes.columns = ["route", "count"]
    fig = px.bar(
        top_routes.sort_values("count"),
        x="count", y="route", orientation="h",
        title="Top 10 Routes by Volume",
        labels={"count": "Shipments", "route": ""},
        color="count", color_continuous_scale="Blues"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Delay rate by region
with col2:
    region_perf = filtered.groupby("region").agg(
        delay_rate=("is_delayed", "mean"),
        total=("shipment_id", "count")
    ).reset_index()
    region_perf["delay_rate"] = region_perf["delay_rate"] * 100
    fig = px.bar(
        region_perf.sort_values("delay_rate", ascending=False),
        x="region", y="delay_rate",
        title="Delay Rate by Region (%)",
        labels={"delay_rate": "Delay Rate %", "region": ""},
        color="delay_rate", color_continuous_scale="RdYlGn_r"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Route type breakdown
with col3:
    route_type_stats = filtered.groupby("route_type").agg(
        count=("shipment_id", "count"),
        avg_cost=("cost_usd", "mean"),
        delay_rate=("is_delayed", "mean")
    ).reset_index()
    route_type_stats["delay_rate"] = route_type_stats["delay_rate"] * 100
    fig = px.scatter(
        route_type_stats,
        x="avg_cost", y="delay_rate",
        size="count", color="route_type",
        title="Route Type: Cost vs Delay Rate",
        labels={"avg_cost": "Avg Cost (USD)", "delay_rate": "Delay Rate %"}
    )
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 5 — DRIVER & VEHICLE PERFORMANCE
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">🧑‍✈️ Driver & Vehicle Performance</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

# Top 10 drivers by on-time rate
with col1:
    driver_perf = filtered.groupby("driver_name").agg(
        total=("shipment_id", "count"),
        on_time=("status", lambda x: (x == "on_time").sum()),
        avg_rating=("driver_rating", "mean")
    ).reset_index()
    driver_perf["on_time_pct"] = driver_perf["on_time"] / driver_perf["total"] * 100
    top_drivers = driver_perf[driver_perf["total"] >= 5].nlargest(10, "on_time_pct")
    fig = px.bar(
        top_drivers.sort_values("on_time_pct"),
        x="on_time_pct", y="driver_name", orientation="h",
        title="Top 10 Drivers — On-Time Rate",
        labels={"on_time_pct": "On-Time %", "driver_name": ""},
        color="on_time_pct", color_continuous_scale="Greens"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Rating vs delay rate
with col2:
    driver_perf["delay_rate"] = 100 - driver_perf["on_time_pct"]
    fig = px.scatter(
        driver_perf[driver_perf["total"] >= 5],
        x="avg_rating", y="delay_rate",
        size="total", hover_name="driver_name",
        title="Driver Rating vs Delay Rate",
        labels={"avg_rating": "Avg Rating", "delay_rate": "Delay Rate %"},
        color="delay_rate", color_continuous_scale="RdYlGn_r"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Vehicle type performance
with col3:
    veh_perf = filtered.groupby("vehicle_type").agg(
        count=("shipment_id", "count"),
        delay_rate=("is_delayed", "mean"),
        avg_fuel=("fuel_consumed_liters", "mean")
    ).reset_index()
    veh_perf["delay_rate"] = veh_perf["delay_rate"] * 100
    fig = px.bar(
        veh_perf,
        x="vehicle_type", y=["delay_rate", "avg_fuel"],
        barmode="group",
        title="Vehicle Type: Delay Rate vs Avg Fuel (L)",
        labels={"value": "", "vehicle_type": "", "variable": "Metric"}
    )
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 6 — WEATHER IMPACT
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">🌦️ Weather Impact on Deliveries</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

# Delay rate by weather condition
with col1:
    weather_perf = filtered.groupby("weather_condition").agg(
        count=("shipment_id", "count"),
        delay_rate=("is_delayed", "mean"),
        avg_delay_min=("delay_minutes", "mean")
    ).reset_index()
    weather_perf["delay_rate"] = weather_perf["delay_rate"] * 100
    fig = px.bar(
        weather_perf.sort_values("delay_rate", ascending=False),
        x="weather_condition", y="delay_rate",
        title="Delay Rate by Weather Condition (%)",
        labels={"delay_rate": "Delay Rate %", "weather_condition": ""},
        color="delay_rate", color_continuous_scale="RdYlBu_r"
    )
    fig.update_layout(coloraxis_showscale=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Temperature vs delay minutes
with col2:
    sample_w = filtered.sample(min(800, len(filtered)), random_state=1)
    fig = px.scatter(
        sample_w, x="temperature_celsius", y="delay_minutes",
        color="weather_condition",
        title="Temperature vs Delay Minutes",
        labels={"temperature_celsius": "Temp (°C)", "delay_minutes": "Delay (min)"},
        opacity=0.6
    )
    fig.update_layout(margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

# Wind speed vs delay
with col3:
    fig = px.box(
        filtered[filtered["wind_speed_kmh"].notna()],
        x="status", y="wind_speed_kmh",
        color="status",
        color_discrete_map={"on_time": "#10b981", "delayed": "#f59e0b", "failed": "#ef4444"},
        title="Wind Speed Distribution by Status",
        labels={"wind_speed_kmh": "Wind Speed (km/h)", "status": ""}
    )
    fig.update_layout(showlegend=False, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════
# SECTION 7 — RAW DATA TABLE
# ══════════════════════════════════════════════════════════════
st.markdown('<p class="section-title">📋 Raw Data Explorer</p>', unsafe_allow_html=True)

cols_to_show = [
    "shipment_id", "full_date", "status", "delay_minutes",
    "company_name", "segment", "driver_name", "driver_rating",
    "vehicle_type", "route_label", "region", "route_type",
    "distance_km", "cost_usd", "weight_kg", "weather_condition"
]

# Only keep cols that exist in filtered
cols_to_show = [c for c in cols_to_show if c in filtered.columns]

st.dataframe(
    filtered[cols_to_show].sort_values("full_date", ascending=False),
    use_container_width=True,
    height=350
)

# Download button
csv = filtered[cols_to_show].to_csv(index=False)
st.download_button(
    "⬇️ Download filtered data as CSV",
    data=csv,
    file_name="logiflow_filtered.csv",
    mime="text/csv"
)

# ─── Footer ───────────────────────────────────────────────────
st.divider()
st.caption("🚚 LogiFlow Analytics · Built by Chmicha · INSEA Data Science")