# 📊 MVP 2A — Analytics Dashboard

Interactive logistics analytics dashboard built with Streamlit + Plotly,
connected directly to the PostgreSQL data warehouse.

## 🚀 How to Run

```bash
# From project root
cd mvp2-analytics-layer/2A-dashboard
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501

## 📸 Dashboard Preview

### KPI Cards
![KPI Cards](assets/screenshots/01_kpi_cards.png)

### Delivery Performance
![Delivery Performance](assets/screenshots/02_delivery_performance.png)

### Cost & Revenue Analysis
![Cost Revenue](assets/screenshots/03_cost_revenue.png)

### Route & Geography
![Routes](assets/screenshots/04_routes_geography.png)

### Driver & Vehicle Performance
![Drivers](assets/screenshots/05_driver_vehicle.png)

### Weather Impact
![Weather](assets/screenshots/06_weather_impact.png)

### Data Explorer
![Data Table](assets/screenshots/07_data_explorer.png)

## 📋 Features

- **6 KPI cards** — total shipments, on-time rate, avg delay, revenue, failure rate, driver rating
- **Delivery performance** — status breakdown, monthly trend, weekday analysis
- **Cost & revenue** — by segment, cost vs distance, monthly trend
- **Route & geography** — top routes, delay by region, route type comparison
- **Driver & vehicle** — top performers, rating vs delay correlation
- **Weather impact** — delay by condition, temperature & wind analysis
- **Data explorer** — filterable table with CSV download

## 🔧 Tech Stack

- **Streamlit** — dashboard framework
- **Plotly** — interactive charts
- **SQLAlchemy + psycopg2** — PostgreSQL connection
- **Pandas** — data manipulation