#!/usr/bin/env python3
"""
ETL Pipeline for LogiFlow Data Warehouse
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
load_dotenv("/mnt/c/Users/HP PRO/Documents/global logistic project/logiflow/.env")


# 1. CONFIGURATION

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
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
if not POSTGRES_PASSWORD:
    raise ValueError("POSTGRES_PASSWORD environment variable must be set")

# Files to process from MinIO
FILES_TO_EXTRACT = {
    "raw/customers.csv": "customers",
    "raw/drivers.csv": "drivers",
    "raw/vehicles.csv": "vehicles",
    "raw/shipments.csv": "shipments"
}


# 2. EXTRACT — Download CSV files from MinIO

def extract_from_minio():
    """
    Connect to MinIO and download all CSV files into pandas DataFrames.
    
    Logic:
    - Connect to MinIO (like connecting to AWS S3)
    - For each file, download it as bytes
    - Convert bytes → pandas DataFrame
    - Return dictionary: {"customers": df, "drivers": df, ...}
    """
    print("\n" + "="*70)
    print("STEP 1: EXTRACTING DATA FROM MINIO")
    print("="*70)
    
    # Connect to MinIO
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Local development, no HTTPS
        )
        print(f"[OK] Connected to MinIO at {MINIO_ENDPOINT}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to MinIO: {e}")
        return None
    
    # Download each CSV file
    dataframes = {}
    
    for file_path, table_name in FILES_TO_EXTRACT.items():
        try:
            print(f"\nDownloading: {file_path}")
            
            # Get object from MinIO
            response = minio_client.get_object(BUCKET_NAME, file_path)
            
            # Read bytes into DataFrame
            csv_bytes = response.read()
            df = pd.read_csv(io.BytesIO(csv_bytes))
            
            dataframes[table_name] = df
            print(f"   [OK] Loaded {len(df):,} rows")
            
            response.close()
            response.release_conn()
            
        except Exception as e:
            print(f"   [ERROR] Error loading {file_path}: {e}")
            return None
    
    print(f"\n[OK] Successfully extracted {len(dataframes)} files from MinIO")
    return dataframes


# 3. TRANSFORM — Prepare data for star schema

def transform_data(dataframes):
    """
    Clean and prepare data for loading into star schema.
    
    Logic:
    - Remove duplicates
    - Handle missing values
    - Create dim_routes from shipment data
    - Prepare fact_shipments (will need dimension IDs later)
    """
    print("\n" + "="*70)
    print("STEP 2: TRANSFORMING DATA")
    print("="*70)
    
    transformed = {}
    
    # ─── Dimension 1: Customers ───
    print("\nProcessing dim_customers...")
    customers = dataframes["customers"].copy()
    customers = customers.drop_duplicates(subset=["customer_id"])
    customers = customers.dropna(subset=["customer_id", "company_name"])
    transformed["dim_customers"] = customers
    print(f"   [OK] {len(customers):,} unique customers ready")
    
    # ─── Dimension 2: Drivers ───
    print("\nProcessing dim_drivers...")
    drivers = dataframes["drivers"].copy()
    drivers = drivers.drop_duplicates(subset=["driver_id"])
    drivers = drivers.dropna(subset=["driver_id", "full_name"])
    transformed["dim_drivers"] = drivers
    print(f"   [OK] {len(drivers):,} unique drivers ready")
    
    # ─── Dimension 3: Vehicles ───
    print("\nProcessing dim_vehicles...")
    vehicles = dataframes["vehicles"].copy()
    vehicles = vehicles.drop_duplicates(subset=["vehicle_id"])
    vehicles = vehicles.dropna(subset=["vehicle_id", "vehicle_type"])
    transformed["dim_vehicles"] = vehicles
    print(f"   [OK] {len(vehicles):,} unique vehicles ready")
    
    # ─── Dimension 4: Routes ───
    print("\nProcessing dim_routes...")
    # Routes are derived from unique origin-destination pairs in shipments
    shipments = dataframes["shipments"].copy()
    routes = shipments[["route_origin_city", "route_origin_country", "route_dest_city", "route_dest_country", "distance_km", "region", "route_type"]].copy()
    routes = routes.drop_duplicates(subset=["route_origin_city", "route_origin_country", "route_dest_city", "route_dest_country"])
    routes = routes.dropna(subset=["route_origin_city", "route_dest_city"])
    
    # Rename columns to match schema
    routes = routes.rename(columns={
        "route_origin_city": "origin_city",
        "route_origin_country": "origin_country",
        "route_dest_city": "destination_city",
        "route_dest_country": "destination_country"
    })
    
    routes = routes.reset_index(drop=True)
    
    transformed["dim_routes"] = routes
    print(f"   [OK] {len(routes):,} unique routes ready")
    
    # ─── Fact Table: Shipments ───
    print("\nProcessing fact_shipments...")
    fact_shipments = shipments.copy()
    fact_shipments = fact_shipments.dropna(subset=["shipment_id"])
    
    # Convert date strings to datetime objects
    fact_shipments["scheduled_pickup"] = pd.to_datetime(fact_shipments.get("scheduled_pickup", None))
    fact_shipments["actual_pickup"] = pd.to_datetime(fact_shipments["actual_pickup"])
    fact_shipments["scheduled_delivery"] = pd.to_datetime(fact_shipments.get("scheduled_delivery", None))
    fact_shipments["actual_delivery"] = pd.to_datetime(fact_shipments["actual_delivery"])
    
    # Extract unique dates for dimension table
    unique_dates = pd.concat([
        fact_shipments["actual_delivery"].dt.normalize(),
        fact_shipments["actual_pickup"].dt.normalize(),
        fact_shipments["scheduled_pickup"].dt.normalize().dropna(),
        fact_shipments["scheduled_delivery"].dt.normalize().dropna()
    ]).unique()
    
    # Create dim_dates dataframe
    dim_dates = pd.DataFrame({"full_date": unique_dates})
    dim_dates = dim_dates.dropna().drop_duplicates().sort_values("full_date").reset_index(drop=True)
    
    # Add date attributes
    dim_dates["day"] = dim_dates["full_date"].dt.day
    dim_dates["month"] = dim_dates["full_date"].dt.month
    dim_dates["month_name"] = dim_dates["full_date"].dt.strftime("%B")
    dim_dates["quarter"] = dim_dates["full_date"].dt.quarter
    dim_dates["year"] = dim_dates["full_date"].dt.year
    dim_dates["weekday"] = dim_dates["full_date"].dt.strftime("%A")
    dim_dates["is_weekend"] = dim_dates["full_date"].dt.dayofweek >= 5
    
    transformed["dim_dates"] = dim_dates
    
    # Initialize date_id to None (will be filled after dimension load)
    fact_shipments["date_id"] = None
    # Store the date in a temporary column to map to date_id later
    fact_shipments["_temp_date"] = fact_shipments["actual_delivery"].dt.normalize()
    
    # Add raw_file (source file reference)
    fact_shipments["raw_file"] = "raw/shipments.csv"
    
    # Ensure required columns exist with defaults
    if "status" not in fact_shipments.columns:
        fact_shipments["status"] = "on_time"
    if "is_delayed" not in fact_shipments.columns:
        fact_shipments["is_delayed"] = False
    
    # Calculate delay in days from delay_minutes (for reference)
    fact_shipments["delay_days"] = fact_shipments["delay_minutes"] / (24 * 60)
    
    transformed["fact_shipments"] = fact_shipments
    print(f"   [OK] {len(fact_shipments):,} shipments ready")
    
    print("\n[OK] Transformation complete")
    return transformed


# 4. LOAD — Insert data into PostgreSQL

def initialize_database(cursor):
    """
    Create all required tables from schema.sql if they don't exist.
    """
    print("\nInitializing database schema...")
    
    schema_file_path = os.path.join(os.path.dirname(__file__), "../warehouse/schema.sql")
    
    try:
        with open(schema_file_path, 'r') as f:
            schema_sql = f.read()
        
        cursor.execute(schema_sql)
        print("   [OK] Database schema initialized")
        return True
        
    except FileNotFoundError:
        print(f"   [WARNING] Schema file not found at {schema_file_path}")
        print("   Creating tables with minimal schema...")
        
        # Minimal schema as fallback
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_id SERIAL PRIMARY KEY,
                company_name VARCHAR(150),
                industry VARCHAR(80),
                country VARCHAR(60),
                city VARCHAR(80),
                segment VARCHAR(20),
                contract_type VARCHAR(20)
            );
            
            CREATE TABLE IF NOT EXISTS dim_driver (
                driver_id SERIAL PRIMARY KEY,
                full_name VARCHAR(100),
                license_type VARCHAR(10),
                experience_years INT,
                rating NUMERIC(3,2),
                country VARCHAR(60),
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS dim_vehicle (
                vehicle_id SERIAL PRIMARY KEY,
                plate_number VARCHAR(20) UNIQUE,
                vehicle_type VARCHAR(30),
                capacity_kg INT,
                manufacture_year INT,
                mileage_km INT,
                last_service_date DATE,
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS dim_route (
                route_id SERIAL PRIMARY KEY,
                origin_city VARCHAR(80),
                origin_country VARCHAR(60),
                destination_city VARCHAR(80),
                destination_country VARCHAR(60),
                distance_km INT,
                region VARCHAR(40),
                route_type VARCHAR(20)
            );
            
            CREATE TABLE IF NOT EXISTS fact_shipments (
                shipment_id SERIAL PRIMARY KEY,
                customer_id INT REFERENCES dim_customer(customer_id),
                driver_id INT REFERENCES dim_driver(driver_id),
                vehicle_id INT REFERENCES dim_vehicle(vehicle_id),
                route_id INT REFERENCES dim_route(route_id),
                planned_duration_hrs NUMERIC(6,2),
                actual_duration_hrs NUMERIC(6,2),
                delay_minutes INT,
                distance_km INT,
                weight_kg NUMERIC(8,2),
                cost_usd NUMERIC(10,2),
                fuel_consumed_liters NUMERIC(8,2),
                weather_condition VARCHAR(30),
                temperature_celsius NUMERIC(5,2),
                wind_speed_kmh NUMERIC(5,2)
            );
        """)
        print("   [OK] Minimal schema created")
        return True
        
    except Exception as e:
        print(f"   [ERROR] Error initializing schema: {e}")
        return False


def clear_tables(cursor):
    """
    Clear all tables in the correct order to avoid foreign key violations.
    
    Logic:
    - Delete from fact_shipments FIRST (it references all dimensions)
    - Then delete from dimension tables (no dependencies between them)
    - This respects foreign key constraints
    
    Why this order?
    - fact_shipments has foreign keys → dim_customers, dim_drivers, dim_vehicles, dim_routes
    - You can't delete a dimension row if a fact row still references it
    - So: delete fact rows first, then dimension rows
    """
    print("\nClearing existing data...")
    
    # Order matters! Delete fact table first
    tables_to_clear = [
        "fact_shipments",    # FIRST: has foreign keys to all dimensions
        "dim_date",          # Then dimensions (no dependencies between them)
        "dim_route",
        "dim_customer",
        "dim_driver",
        "dim_vehicle"
    ]
    
    for table in tables_to_clear:
        cursor.execute(f"DELETE FROM {table};")
        print(f"   [OK] Cleared {table}")
    
    print("   [OK] All tables cleared successfully\n")


def load_to_postgres(transformed_data):
    """
    Load data into PostgreSQL star schema.
    
    Logic:
    - Connect to PostgreSQL
    - Load dimensions FIRST (customers, drivers, vehicles, routes)
    - Then load fact table (needs foreign keys from dimensions)
    - Use batch inserts for performance
    """
    print("\n" + "="*70)
    print("STEP 3: LOADING DATA INTO POSTGRESQL")
    print("="*70)
    
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
        print(f"[OK] Connected to PostgreSQL database: {POSTGRES_DB}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        return False
    
    try:
        # Initialize database schema
        if not initialize_database(cursor):
            return False
        
        conn.commit()
        
        # Clear existing data (in correct order: fact first, then dimensions)
        clear_tables(cursor)
        
        # ─── Load Dimension 0: Dates ───
        print("\nLoading dim_date...")
        dim_dates = transformed_data["dim_dates"]
        
        # Build a mapping: full_date → date_id
        date_mapping = {}
        
        insert_query = """
            INSERT INTO dim_date (full_date, day, month, month_name, quarter, year, weekday, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING date_id, full_date;
        """
        
        for _, row in dim_dates.iterrows():
            cursor.execute(insert_query, (
                row["full_date"], int(row["day"]), int(row["month"]), row["month_name"], 
                int(row["quarter"]), int(row["year"]), row["weekday"], bool(row["is_weekend"])
            ))
            result = cursor.fetchone()
            if result:
                date_id, full_date = result
                date_mapping[pd.Timestamp(full_date)] = date_id
        
        print(f"   [OK] Inserted {len(date_mapping)} dates")
        
        # ─── Load Dimension 1: Customers ───
        print("\nLoading dim_customer...")
        customers = transformed_data["dim_customers"]
        
        insert_query = """
            INSERT INTO dim_customer (customer_id, company_name, industry, country, city, segment, contract_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """
        
        records = customers[["customer_id", "company_name", "industry", "country", "city", "segment", "contract_type"]].fillna('').values.tolist()
        cursor.executemany(insert_query, records)
        print(f"   [OK] Inserted {cursor.rowcount} customers")
        
        # Reset sequence for customer_id
        cursor.execute("SELECT MAX(customer_id) FROM dim_customer;")
        max_id = cursor.fetchone()[0] or 0
        cursor.execute(f"SELECT setval('dim_customer_customer_id_seq', {max_id} + 1);")
        
        # ─── Load Dimension 2: Drivers ───
        print("\nLoading dim_driver...")
        drivers = transformed_data["dim_drivers"]
        
        insert_query = """
            INSERT INTO dim_driver (driver_id, full_name, license_type, experience_years, rating, country, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING;
        """
        
        records = drivers[["driver_id", "full_name", "license_type", "experience_years", "rating", "country", "is_active"]].fillna('').values.tolist()
        cursor.executemany(insert_query, records)
        print(f"   [OK] Inserted {cursor.rowcount} drivers")
        
        # Reset sequence for driver_id
        cursor.execute("SELECT MAX(driver_id) FROM dim_driver;")
        max_id = cursor.fetchone()[0] or 0
        cursor.execute(f"SELECT setval('dim_driver_driver_id_seq', {max_id} + 1);")
        
        # ─── Load Dimension 3: Vehicles ───
        print("\nLoading dim_vehicle...")
        vehicles = transformed_data["dim_vehicles"]
        
        insert_query = """
            INSERT INTO dim_vehicle (vehicle_id, plate_number, vehicle_type, capacity_kg, manufacture_year, mileage_km, last_service_date, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vehicle_id) DO NOTHING;
        """
        
        records = vehicles[["vehicle_id", "plate_number", "vehicle_type", "capacity_kg", "manufacture_year", "mileage_km", "last_service_date", "is_active"]].fillna('').values.tolist()
        cursor.executemany(insert_query, records)
        print(f"   [OK] Inserted {cursor.rowcount} vehicles")
        
        # Reset sequence for vehicle_id
        cursor.execute("SELECT MAX(vehicle_id) FROM dim_vehicle;")
        max_id = cursor.fetchone()[0] or 0
        cursor.execute(f"SELECT setval('dim_vehicle_vehicle_id_seq', {max_id} + 1);")
        
        # ─── Load Dimension 4: Routes ───
        print("\nLoading dim_route...")
        routes = transformed_data["dim_routes"]
        
        insert_query = """
            INSERT INTO dim_route (origin_city, origin_country, destination_city, destination_country, distance_km, region, route_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING route_id, origin_city, origin_country, destination_city, destination_country;
        """
        
        # Build a mapping: (origin, destination) → route_id
        route_mapping = {}
        
        for _, row in routes.iterrows():
            cursor.execute(insert_query, (
                row["origin_city"], row["origin_country"],
                row["destination_city"], row["destination_country"],
                row["distance_km"], row.get("region", "Unknown"), row.get("route_type", "Road")
            ))
            result = cursor.fetchone()
            if result:
                route_id, o_city, o_country, d_city, d_country = result
                route_mapping[(o_city, o_country, d_city, d_country)] = route_id
        
        print(f"   [OK] Inserted {len(route_mapping)} routes")
        
        # ─── Load Fact Table: Shipments ───
        print("\nLoading fact_shipments...")
        fact_shipments = transformed_data["fact_shipments"]
        
        # Map shipments to their route_id
        def get_route_id(row):
            return route_mapping.get((row["route_origin_city"], row["route_origin_country"], row["route_dest_city"], row["route_dest_country"]), None)
        
        fact_shipments["route_id"] = fact_shipments.apply(get_route_id, axis=1)
        
        # Map shipments to their date_id using the date_mapping
        def get_date_id(row):
            temp_date = pd.Timestamp(row["_temp_date"])
            return date_mapping.get(temp_date, None)
        
        fact_shipments["date_id"] = fact_shipments.apply(get_date_id, axis=1)
        
        # Remove shipments without valid routes or dates
        fact_shipments = fact_shipments.dropna(subset=["route_id", "date_id"])
        
        insert_query = """
            INSERT INTO fact_shipments (
                customer_id, driver_id, vehicle_id, route_id, date_id, raw_file,
                planned_duration_hrs, actual_duration_hrs, delay_minutes, distance_km, weight_kg, cost_usd,
                fuel_consumed_liters, weather_condition, temperature_celsius, wind_speed_kmh,
                status, is_delayed, scheduled_pickup, actual_pickup, scheduled_delivery, actual_delivery
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        
        # Prepare records - cast date_id to int, fill numeric NaNs with 0, but keep None for foreign keys
        records_list = []
        for _, row in fact_shipments.iterrows():
            record = [
                int(row["customer_id"]) if pd.notna(row["customer_id"]) else None,
                int(row["driver_id"]) if pd.notna(row["driver_id"]) else None,
                int(row["vehicle_id"]) if pd.notna(row["vehicle_id"]) else None,
                int(row["route_id"]) if pd.notna(row["route_id"]) else None,
                int(row["date_id"]) if pd.notna(row["date_id"]) else None,
                row["raw_file"] or "",
                float(row["planned_duration_hrs"]) if pd.notna(row["planned_duration_hrs"]) else 0,
                float(row["actual_duration_hrs"]) if pd.notna(row["actual_duration_hrs"]) else 0,
                int(row["delay_minutes"]) if pd.notna(row["delay_minutes"]) else 0,
                int(row["distance_km"]) if pd.notna(row["distance_km"]) else 0,
                float(row["weight_kg"]) if pd.notna(row["weight_kg"]) else 0,
                float(row["cost_usd"]) if pd.notna(row["cost_usd"]) else 0,
                float(row["fuel_consumed_liters"]) if pd.notna(row["fuel_consumed_liters"]) else 0,
                row["weather_condition"] or "Unknown",
                float(row["temperature_celsius"]) if pd.notna(row["temperature_celsius"]) else 0,
                float(row["wind_speed_kmh"]) if pd.notna(row["wind_speed_kmh"]) else 0,
                row["status"] or "unknown",
                bool(row["is_delayed"]) if pd.notna(row["is_delayed"]) else False,
                row["scheduled_pickup"],
                row["actual_pickup"],
                row["scheduled_delivery"],
                row["actual_delivery"]
            ]
            records_list.append(record)
        
        cursor.executemany(insert_query, records_list)
        print(f"   [OK] Inserted {cursor.rowcount} shipments")
        
        # Commit all changes
        conn.commit()
        print("\n[OK] All data committed to database")
        
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Error during loading: {e}")
        conn.rollback()
        return False
        
    finally:
        cursor.close()
        conn.close()


# 5. VERIFY — Check data was loaded correctly

def verify_load():
    """
    Connect to PostgreSQL and verify row counts in each table.
    """
    print("\n" + "="*70)
    print("STEP 4: VERIFYING DATA LOAD")
    print("="*70)
    
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
        
        print("\nRow counts:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"   {table:20} -> {count:,} rows")
        
        cursor.close()
        conn.close()
        
        print("\n[OK] Verification complete!")
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")


# MAIN EXECUTION

def main():
    """
    Run the complete ETL pipeline.
    """
    print("\n" + "="*70)
    print("LOGIFLOW ETL PIPELINE")
    print("="*70)
    print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Extract
    dataframes = extract_from_minio()
    if not dataframes:
        print("\n[ERROR] ETL pipeline failed at EXTRACT step")
        return
    
    # Step 2: Transform
    transformed_data = transform_data(dataframes)
    if not transformed_data:
        print("\n[ERROR] ETL pipeline failed at TRANSFORM step")
        return
    
    # Step 3: Load
    success = load_to_postgres(transformed_data)
    if not success:
        print("\n[ERROR] ETL pipeline failed at LOAD step")
        return
    
    # Step 4: Verify
    verify_load()
    
    print("\n" + "="*70)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*70)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    main()


def run():
    """Entry point called by the scheduler"""
    print("Starting ETL pipeline...")
    # call your existing main logic here
    # e.g.: extract() → transform() → load()

if __name__ == "__main__":
    run()

