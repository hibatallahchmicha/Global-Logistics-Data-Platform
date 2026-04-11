import pandas as pd
from db_connector import load_shipments

@st.cache_data(ttl=300)
def get_data():
    return load_shipments()

def apply_filters(df, years, statuses, regions, segments):
    mask = (
        df["year"].isin(years) &
        df["status"].isin(statuses) &
        df["region"].isin(regions) &
        df["segment"].isin(segments)
    )
    return df[mask]