#!/usr/bin/env python3
"""

Flow:
1. Extract: Download CSV files from MinIO
2. Transform: Clean data, prepare for star schema
3. Load: Insert into PostgreSQL (dimensions first, then fact table)
"""

import os
import io
import pandas as pd
import psycopg2
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()



# 1 CONFIGURATION


# MinIO settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "logiflow-raw")

# PostgreSQL settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "logiflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "logiflow_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "logiflow_pass")

# Files to process from MinIO
FILES_TO_EXTRACT = {
    "raw/customers.csv": "customers",
    "raw/drivers.csv": "drivers",
    "raw/vehicles.csv": "vehicles",
    "raw/shipments.csv": "shipments"
}



# 2 EXTRACT — Download CSV files from MinIO


def extract_from_minio():
    """
    Connect to MinIO and download all CSV files into pandas DataFrames.
    
    Logic:
    - Connect to MinIO (like
    - For each file, download it as bytes
    - Convert bytes → pandas DataFrame
    - Return dictionary: {"customers": df, "drivers": df, ...}
    """
    print(" STEP 1: EXTRACTING DATA FROM MINIO")
    
    # Connect to MinIO
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Local development, no HTTPS
        )
        print(f" Connected to MinIO at {MINIO_ENDPOINT}")
    except Exception as e:
        print(f" Failed to connect to MinIO: {e}")
        return None
    
    # Download each CSV file
    dataframes = {}
    
    for file_path, table_name in FILES_TO_EXTRACT.items():
        try:
            print(f"\n Downloading: {file_path}")
            
            # Get object from MinIO
            response = minio_client.get_object(BUCKET_NAME, file_path)
            
            # Read bytes into DataFrame
            csv_bytes = response.read()
            df = pd.read_csv(io.BytesIO(csv_bytes))
            
            dataframes[table_name] = df
            print(f"    Loaded {len(df):,} rows")
            
            response.close()
            response.release_conn()
            
        except Exception as e:
            print(f"    Error loading {file_path}: {e}")
            return None
    
    print(f"\ Successfully extracted {len(dataframes)} files from MinIO")
    return dataframes


# 3 TRANSFORM — Prepare data for star schema

def transform_data(dataframes):
    """
    Clean and prepare data for loading into star schema.
    
    Logic:
    - Remove duplicates
    - Handle missing values
    - Create dim_routes from shipment data
    - Prepare fact_shipments (will need dimension IDs later)
    """
    print(" STEP 2: TRANSFORMING DATA")
    
    transformed = {}

    # Dimension 1: Customers 
    print("\n Processing dim_customers...")
    customers = dataframes["customers"].copy()
    customers = customers.drop_duplicates(subset=["customer_id"])
    customers = customers.dropna(subset=["customer_id", "company_name"])
    transformed["dim_customer"] = customers
    print(f"    {len(customers):,} unique customers ready")
    
    #  Dimension 2: Drivers 
    print("\n Processing dim_drivers...")
    drivers = dataframes["drivers"].copy()
    drivers = drivers.drop_duplicates(subset=["driver_id"])
    drivers = drivers.dropna(subset=["driver_id", "full_name"])
    transformed["dim_driver"] = drivers
    print(f"    {len(drivers):,} unique drivers ready")
    
    #  Dimension 3: Vehicles 
    print("\n Processing dim_vehicles...")
    vehicles = dataframes["vehicles"].copy()
    vehicles = vehicles.drop_duplicates(subset=["vehicle_id"])
    vehicles = vehicles.dropna(subset=["vehicle_id", "vehicle_type"])
    transformed["dim_vehicle"] = vehicles
    print(f"    {len(vehicles):,} unique vehicles ready")
    
    #  Dimension 4: Routes 
    print("\n Processing dim_routes...")
    # Routes are derived from unique origin-destination pairs in shipments
    shipments = dataframes["shipments"].copy()
    routes = shipments[[
        "route_origin_city", "route_origin_country", 
        "route_dest_city", "route_dest_country", 
        "distance_km", "region", "route_type"
    ]].copy()
    routes = routes.rename(columns={
        "route_origin_city": "origin_city", 
        "route_origin_country": "origin_country",
        "route_dest_city": "destination_city",
        "route_dest_country": "destination_country"
    })
    routes = routes.drop_duplicates(subset=["origin_city", "destination_city"])
    routes = routes.dropna(subset=["origin_city", "destination_city"])
    
    # Add route_id (will be assigned by database, this is placeholder)
    routes = routes.reset_index(drop=True)
    routes.insert(0, "route_id", range(1, len(routes) + 1))
    
    transformed["dim_route"] = routes
    print(f"    {len(routes):,} unique routes ready")
    
    #  Fact Table: Shipments 
    print("\n Processing fact_shipments...")
    fact_shipments = shipments.copy()
    fact_shipments = fact_shipments.dropna(subset=["shipment_id"])
    
    transformed["fact_shipments"] = fact_shipments
    print(f"    {len(fact_shipments):,} shipments ready")
    
    print("\n Transformation complete")
    return transformed


# 4 LOAD — Insert data into PostgreSQL

def load_to_postgres(transformed_data):
    """
    Load data into PostgreSQL star schema.
    
    Logic:
    - Connect to PostgreSQL
    - Load dimensions FIRST (customers, drivers, vehicles, routes)
    - Then load fact table (needs foreign keys from dimensions)
    - Use batch inserts for performance
    """
    print(" STEP 3: LOADING DATA INTO POSTGRESQL")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        print(f" Connected to PostgreSQL database: {POSTGRES_DB}")
    except Exception as e:
        print(f" Failed to connect to PostgreSQL: {e}")
        return False
    
    try:
        #  Load Dimension 1: Customers 
        print("\n Loading dim_customers...")
        customers = transformed_data["dim_customer"]
        
        insert_query = """
            INSERT INTO dim_customer (customer_id, company_name, industry, country)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """
        
        records = customers[["customer_id", "company_name", "industry", "country"]].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"    Inserted {cursor.rowcount} customers")
        
        # Load Dimension 2: Drivers 
        print("\n Loading dim_drivers...")
        drivers = transformed_data["dim_driver"]
        
        insert_query = """
            INSERT INTO dim_driver (driver_id, full_name, experience_years, rating)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING;
        """
        
        records = drivers[["driver_id", "full_name", "experience_years", "rating"]].values.tolist()
        cursor.executemany(insert_query, records)
        print(f"    Inserted {cursor.rowcount} drivers")
        
        # Load Dimension 3: Vehicles 
        print("\n Loading dim_vehicles...")
        vehicles = transformed_data["dim_vehicle"]
        
        insert_query = """
            INSERT INTO dim_vehicle (vehicle_id, vehicle_type, capacity_kg, plate_number)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vehicle_id) DO NOTHING;
        """
        
        records = vehicles[["vehicle_id", "vehicle_type", "capacity_kg", "plate_number"]].values.tolist()
        cursor.executemany(insert_query, records)
        print(f" Inserted {cursor.rowcount} vehicles")
        
        #  Load Dimension 4: Routes 
        print("\n  Loading dim_route...")
        routes = transformed_data["dim_route"]
        
        # First, clear existing routes (since we're regenerating route_ids)
        cursor.execute("DELETE FROM dim_route;")
        
        insert_query = """
            INSERT INTO dim_route (origin_city, origin_country, destination_city, destination_country, distance_km, region, route_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING route_id, origin_city, destination_city;
        """
        
        # Build a mapping: (origin_city, destination_city) → route_id
        route_mapping = {}
        
        for _, row in routes.iterrows():
            cursor.execute(insert_query, (
                row["origin_city"], row["origin_country"],
                row["destination_city"], row["destination_country"],
                row["distance_km"], row["region"], row["route_type"]
            ))
            route_id, origin_city, destination_city = cursor.fetchone()
            route_mapping[(origin_city, destination_city)] = route_id
        
        print(f" Inserted {len(route_mapping)} routes")
        
        #  Load Fact Table: Shipments
        print("\n Loading fact_shipments...")
        fact_shipments = transformed_data["fact_shipments"]
        
        # Map shipments to their route_id
        def get_route_id(row):
            return route_mapping.get((row["route_origin_city"], row["route_dest_city"]), None)
        
        fact_shipments["route_id"] = fact_shipments.apply(get_route_id, axis=1)
        
        # Remove shipments without valid routes
        fact_shipments = fact_shipments.dropna(subset=["route_id"])
        
        insert_query = """
            INSERT INTO fact_shipments (
                shipment_id, customer_id, driver_id, vehicle_id, route_id,
                planned_duration_hrs, actual_duration_hrs, delay_minutes,
                distance_km, weight_kg, cost_usd, fuel_consumed_liters,
                weather_condition, temperature_celsius, wind_speed_kmh,
                status, is_delayed,
                scheduled_pickup, actual_pickup, scheduled_delivery, actual_delivery
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (shipment_id) DO NOTHING;
        """
        
        records = fact_shipments[[
            "shipment_id", "customer_id", "driver_id", "vehicle_id", "route_id",
            "planned_duration_hrs", "actual_duration_hrs", "delay_minutes",
            "distance_km", "weight_kg", "cost_usd", "fuel_consumed_liters",
            "weather_condition", "temperature_celsius", "wind_speed_kmh",
            "status", "is_delayed",
            "scheduled_pickup", "actual_pickup", "scheduled_delivery", "actual_delivery"
        ]].values.tolist()
        
        cursor.executemany(insert_query, records)
        print(f"    Inserted {cursor.rowcount} shipments")
        
        # Commit all changes
        conn.commit()
        print("\n All data committed to database")
        
        return True
        
    except Exception as e:
        print(f"\n Error during loading: {e}")
        conn.rollback()
        return False
        
    finally:
        cursor.close()
        conn.close()


# 5 VERIFY — Check data was loaded correctly

def verify_load():
    """
    Connect to PostgreSQL and verify row counts in each table.
    """
    print(" STEP 4: VERIFYING DATA LOAD")
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        
        tables = [
            "dim_customer",
            "dim_driver", 
            "dim_vehicle",
            "dim_route",
            "fact_shipments"
        ]
        
        print("\n Row counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"   {table:20} → {count:,} rows")
        
        cursor.close()
        conn.close()
        
        print("\n Verification complete!")
        
    except Exception as e:
        print(f" Verification failed: {e}")


#  MAIN EXECUTION

def main():
    """
    Run the complete ETL pipeline.
    """
    print("\n" + "╔" + "═"*68 + "╗")
    print("║" + " "*20 + "LOGIFLOW ETL PIPELINE" + " "*27 + "║")
    print("╚" + "═"*68 + "╝")
    print(f"\n Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Extract
    dataframes = extract_from_minio()
    if not dataframes:
        print("\n ETL pipeline failed at EXTRACT step")
        return
    
    # Step 2: Transform
    transformed_data = transform_data(dataframes)
    if not transformed_data:
        print("\n ETL pipeline failed at TRANSFORM step")
        return
    
    # Step 3: Load
    success = load_to_postgres(transformed_data)
    if not success:
        print("\n ETL pipeline failed at LOAD step")
        return
    
    # Step 4: Verify
    verify_load()
    
    print(" ETL PIPELINE COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()