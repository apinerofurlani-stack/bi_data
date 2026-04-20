import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from datetime import datetime
import time
import json
import logging

#CONF
#Tableau 
TABLEAU_SERVER_URL = "https://prod-useast-a.online.tableau.com"
SITE_ID = "mi_sitio"
TOKEN_NAME = "mi_token_tableau"
TOKEN_VALUE = "XXXXXXXXXXXXXXXXXXXX"
API_VERSION = "3.10"

#PostgreSQL
DB_CONFIG = {
    "host": "SERVERPOSTGRES",
    "port": "5432",
    "dbname": "BI_DATA",
    "user": "USERPOSTGRES",
    "password": "XXXXXXXXXX!"
}
SCHEMA_NAME = "bi_data"

#AUTEN
def get_tableau_token():
    url = f"{TABLEAU_SERVER_URL}/api/{API_VERSION}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": TOKEN_NAME,
            "personalAccessTokenValue": TOKEN_VALUE,
            "site": {"contentUrl": SITE_ID}
        }
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data["credentials"]["token"], data["credentials"]["site"]["id"]
    else:
        raise Exception(f"Authentication failed: {response.text}")

#ETL
def get_workbooks_metadata(token, site_id):
    #Endpoint to fetch workbooks
    url = f"{TABLEAU_SERVER_URL}/api/{API_VERSION}/sites/{site_id}/workbooks"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    
    all_data = []
    page_number = 1
    
       
    while True:
        # Tableau-style pagination
        paged_url = f"{url}?pageSize=100&pageNumber={page_number}"
        response = requests.get(paged_url, headers=headers)
        data = response.json()
        
        workbooks = data.get("workbooks", {}).get("workbook", [])
        if not workbooks:
            break
            
        for wb in workbooks:
            # Data Transformation & Modeling
            updated_at = datetime.strptime(wb['updatedAt'], '%Y-%m-%dT%H:%M:%SZ')
            
            #Business logic for the derived field 'status'
            #Flag as 'stale' if the last update was more than 90 days ago
            is_stale = (datetime.utcnow() - updated_at).days > 90
            
            all_data.append({
                "asset_id": wb["id"],
                "asset_name": wb["name"],
                "owner": wb.get("owner", {}).get("id"),
                "last_updated": updated_at,
                "last_viewed": updated_at, #Placeholder: Using update date as a proxy
                "views_last_30d": 0,       # Requiere Metadata API avanzada
                "last_refresh": updated_at,
                "refresh_status": "Success",
                "status": "Obsolete" if is_stale else "Active",
                "last_synced_at": datetime.utcnow()
            })
            
        page_number += 1
        
    return pd.DataFrame(all_data)

#LOAD
def map_pandas_to_postgres(df):
    type_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT',
        'bool': 'BOOLEAN'
    }
    return {col: type_mapping.get(str(df[col].dtype), 'TEXT') for col in df.columns}

def create_and_insert_tableau(df, table_name):
    if df.empty: return
    
    column_types = map_pandas_to_postgres(df)
    
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            #Schema creation
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}";')
            
            #Schema Desing
            cols_def = ", ".join([f'"{c}" {t}' for c, t in column_types.items()])
            
            #Table creation and data insertion
            cur.execute(f'CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}."{table_name}" ({cols_def}, PRIMARY KEY (asset_id));')
            
            #Insert with ON CONFLICT to prevent duplicates
            col_names = ', '.join([f'"{c}"' for c in df.columns])
            insert_query = f'''
                INSERT INTO {SCHEMA_NAME}."{table_name}" ({col_names})
                VALUES %s
                ON CONFLICT (asset_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    last_updated = EXCLUDED.last_updated,
                    last_synced_at = EXCLUDED.last_synced_at;
            '''
            
            values = [tuple(row) for row in df.to_numpy()]
            execute_values(cur, insert_query, values)
            conn.commit()
            print(f"📥 {len(values)} Records successfully updated in {table_name}.")

#EXECUTION
if __name__ == "__main__":
    try:
        start_time = time.time()
        
        #AUT
        token, site_id = get_tableau_token()
        
        #ET
        df_metadata = get_workbooks_metadata(token, site_id)
        
        #L
        create_and_insert_tableau(df_metadata, "dim_tableau_assets")
        
        print(f"\n Run completed in {time.time() - start_time:.2f}s")
        
    except Exception as e:
        print(f" Fatal Error: {e}")