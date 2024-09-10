import os
import csv
from datetime import datetime
import time
import requests
import json

# Get Elasticsearch host and port from environment variables or use Docker network IP as default
es_host = os.environ.get('ES_HOST', '172.25.0.2')
es_port = os.environ.get('ES_PORT', '9200')
es_url = f'http://{es_host}:{es_port}'

# Define the index name
index_name = 'weather_data'

# Define the mapping
weather_schema = {
    "mappings": {
        "properties": {
            "time": {"type": "date"},
            "temperature_2m": {"type": "float"},
            "relative_humidity_2m": {"type": "float"},
            "rain": {"type": "float"},
            "snowfall": {"type": "float"},
            "weather_code": {"type": "integer"},
            "surface_pressure": {"type": "float"},
            "cloud_cover": {"type": "float"},
            "cloud_cover_low": {"type": "float"},
            "cloud_cover_high": {"type": "float"},
            "wind_direction_10m": {"type": "float"},
            "wind_direction_100m": {"type": "float"},
            "soil_temperature_28_to_100cm": {"type": "float"}
        }
    }
}

print(f"Attempting to connect to Elasticsearch at {es_url}...")

# Function to check Elasticsearch status
def check_elasticsearch_status():
    try:
        response = requests.get(es_url)
        if response.status_code == 200:
            return True, "Elasticsearch is up and running"
        else:
            return False, f"Elasticsearch returned status code {response.status_code}"
    except requests.RequestException as e:
        return False, f"Error connecting to Elasticsearch: {str(e)}"

# Wait for Elasticsearch to be ready
max_retries = 30
for i in range(max_retries):
    status_ok, status_message = check_elasticsearch_status()
    if status_ok:
        print("Successfully connected to Elasticsearch")
        break
    else:
        print(f"Attempt {i+1}/{max_retries}: {status_message}")
        if i < max_retries - 1:
            print("Retrying in 5 seconds...")
            time.sleep(5)
else:
    print("Failed to connect to Elasticsearch after maximum retries")
    exit(1)

# Function to create index
def create_index():
    try:
        # Check if index exists
        response = requests.head(f"{es_url}/{index_name}")
        if response.status_code == 200:
            print(f"Index '{index_name}' already exists. Deleting it...")
            requests.delete(f"{es_url}/{index_name}")
            print(f"Index '{index_name}' deleted.")

        print(f"Creating index '{index_name}'...")
        response = requests.put(
            f"{es_url}/{index_name}",
            headers={"Content-Type": "application/json"},
            data=json.dumps(weather_schema)
        )
        response.raise_for_status()
        print(f"Index '{index_name}' created successfully.")

        # Verify the index was created
        response = requests.head(f"{es_url}/{index_name}")
        if response.status_code == 200:
            print(f"Verified: Index '{index_name}' exists.")

            # Get and print the mapping to verify it's correct
            mapping_response = requests.get(f"{es_url}/{index_name}/_mapping")
            mapping_response.raise_for_status()
            print(f"Mapping for index '{index_name}':")
            print(json.dumps(mapping_response.json(), indent=2))
        else:
            print(f"Error: Index '{index_name}' was not found after creation attempt.")
    except requests.RequestException as e:
        print(f"Error creating index: {e}")
        exit(1)

# Function to index a single document
def index_document(doc):
    try:
        response = requests.post(
            f"{es_url}/{index_name}/_doc",
            headers={"Content-Type": "application/json"},
            data=json.dumps(doc)
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error indexing document: {e}")

# Function to read CSV and index data
def index_csv_data(file_path):
    with open(file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for i, row in enumerate(csvreader):
            # Convert empty strings to None
            for key, value in row.items():
                if value == '':
                    row[key] = None
                elif key != 'time':
                    # Convert numerical values to float or int
                    row[key] = float(value) if '.' in value else int(value)
            
            # Convert time string to ISO format
            row['time'] = datetime.strptime(row['time'], "%Y-%m-%dT%H:%M").isoformat()

            # Index the document
            index_document(row)
            
            if (i + 1) % 100 == 0:
                print(f"Indexed {i + 1} documents")

    print("Finished indexing all documents")

# Create the index
create_index()

# Index the CSV data
csv_file_path = 'open_metro.csv'  # Update this to the actual path of your CSV file
index_csv_data(csv_file_path)

print("Script execution completed.")