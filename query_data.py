import requests
import json
from datetime import datetime

# Elasticsearch connection details
es_host = "172.25.0.2"  # Update this if your Elasticsearch host is different
es_port = "9200"  # Update this if your Elasticsearch port is different
index_name = "weather_data"

es_url = f"http://{es_host}:{es_port}"

def get_document_count():
    try:
        response = requests.get(f"{es_url}/{index_name}/_count")
        response.raise_for_status()
        return response.json()['count']
    except requests.RequestException as e:
        print(f"Error getting document count: {e}")
        return None

def search_documents(size=10):
    query = {
        "query": {
            "match_all": {}
        },
        "size": size
    }
    
    try:
        response = requests.get(
            f"{es_url}/{index_name}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(query)
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error searching documents: {e}")
        return None

def print_document(doc):
    source = doc['_source']
    print(f"ID: {doc['_id']}")
    print("Document content:")
    for key, value in source.items():
        print(f"  {key}: {value}")
    print("-" * 40)

# Main execution
if __name__ == "__main__":
    # Get total document count
    doc_count = get_document_count()
    if doc_count is not None:
        print(f"Total documents in the index: {doc_count}")
    
    # Search for documents
    results = search_documents()
    if results:
        hits = results['hits']['hits']
        print(f"\nDisplaying {len(hits)} sample documents:")
        for doc in hits:
            print_document(doc)
    
    print("Data verification completed.")