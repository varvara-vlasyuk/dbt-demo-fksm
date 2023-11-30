import yaml
import os
from databricks import sql

# Constants
DBT_PROFILE_PATH = os.path.expanduser('~/.dbt/profiles.yml')
TARGET_LOCATION = 'models/sources'
SOURCES = {
    "silver" : "tpch",
}

# Function to read DBT profile
def read_dbt_profile(profile_name):
    with open(DBT_PROFILE_PATH, 'r') as file:
        profiles = yaml.safe_load(file)
        return profiles[profile_name]['outputs']['dev']  # Adjust based on your profile structure

# Function to execute SQL query in Databricks
def execute_sql_query(query, connection_info):
    with sql.connect(server_hostname=connection_info["host"],
                     http_path=connection_info["http_path"],
                     access_token=connection_info["token"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result

# Function to generate DBT source file
def generate_dbt_source_file(tables, connection_info):
    # catalog = connection_info['catalog']
    for catalog, schema in SOURCES.items():
        # schema = connection_info['schema']
        # schema = 'tpch'
        source_name = f"{catalog}_{schema}"
        dbt_sources = {
            "version": 2,
            "sources": [
                {
                    "name": source_name,
                    "description": "",
                    "catalog": catalog,
                    "schema": schema,
                    "tables": [{"name": table.table_name} for table in tables]
                }
            ]
        }

        with open(os.path.join(TARGET_LOCATION, f'sources_{catalog}.yml'), 'w') as file:
            yaml.dump(dbt_sources, file, indent=True, default_flow_style=False, sort_keys=False)

# Main execution
if __name__ == "__main__":
    try:
        dbt_profile = read_dbt_profile('my_dbt_demo')  # Replace with your actual profile name
        # SQL query to fetch data from information schema
        sql_query = "SELECT table_name FROM information_schema.tables where table_schema = 'tpch'"
        query_result = execute_sql_query(sql_query, dbt_profile)
        generate_dbt_source_file(query_result, dbt_profile)
        print("DBT source file generated successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
