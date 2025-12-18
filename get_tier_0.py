import pandas as pd
import requests
from io import StringIO
import json
import os
import csv
from dotenv import load_dotenv
import ssl
from requests.adapters import HTTPAdapter

# Load environment variables from .env file
load_dotenv()

# --- Configuration Loaded from Environment ---
# Fetches the Splunk URL and Token from your .env file
SPLUNK_URL = os.getenv("SPLUNK_URL")
SPLUNK_TOKEN = os.getenv("SPLUNK_TOKEN")
SPLUNK_CA = os.getenv("SPLUNK_CA")


class CustomHTTPAdapter(HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)
    
    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

def get_tier_zero_names():
    """
    Fetch Tier Zero names from the SpecterOps TierZeroTable CSV
    
    Returns:
        str: Comma-separated quoted names where Is Tier Zero = YES
    """
    # Create the SSL context with VERIFY_X509_STRICT disabled
    context = ssl.create_default_context(cafile='C:/agent/nscacert_combined.pem')
    context.verify_flags &= ~ssl.VERIFY_X509_STRICT

    # Create session and mount the custom adapter
    session = requests.Session()
    session.mount('https://', CustomHTTPAdapter(ssl_context=context))

    # Fetch CSV content from URL
    response = session.get('https://raw.githubusercontent.com/SpecterOps/TierZeroTable/refs/heads/main/TierZeroTable.csv')
    csv_content = response.text
    
    # Load CSV with custom delimiter ;
    df = pd.read_csv(StringIO(csv_content), delimiter=';')
    
    # Filter rows where 'Is Tier Zero' is YES (case insensitive)
    tier_zero_names = df[df['Is Tier Zero'].str.upper() == 'YES']['Name']
    
    # Format the result string with comma-separated quoted names
    result_string = ','.join([f'"{name}"' for name in tier_zero_names])
    
    return result_string

def execute_splunk_search(search_query: str):
    """
    Connects to Splunk and executes any provided SPL query.

    Args:
        search_query (str): The full Splunk Processing Language (SPL) query to execute.

    Returns:
        list: A list of dictionaries, where each dictionary is a result row.
              Returns an empty list if an error occurs.
    """
    if not SPLUNK_URL or not SPLUNK_TOKEN:
        print("Error: SPLUNK_URL and SPLUNK_TOKEN must be set in the .env file.")
        return []

    # API endpoint for running a search and exporting results immediately
    export_endpoint = f"{SPLUNK_URL}/services/search/jobs/export"

    headers = {
        "Authorization": f"Bearer {SPLUNK_TOKEN}"
    }
    payload = {
        "search": search_query, # The function now uses the query passed as an argument
        "output_mode": "json"
    }

    try:
        # Use verify=False for self-signed certs (not for production)
        # For production, use verify=True and ensure your system trusts the cert
        response = requests.post(export_endpoint, headers=headers, data=payload, verify=SPLUNK_CA)
        response.raise_for_status()

        # Parse the streaming JSON response
        results = [json.loads(line) for line in response.iter_lines() if line]
        return results

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return []

def get_splunk_report(report_name, splunk_app="search", splunk_user="admin"):
    """
    Connects to a Splunk instance and retrieves the results of a scheduled report.

    Args:
        report_name (str): The name of the scheduled report (saved search).
        splunk_app (str, optional): The Splunk app context where the report is saved. Defaults to "search".
        splunk_user (str, optional): The Splunk username who owns the report. Defaults to "admin".

    Returns:
        list: A list of dictionaries, where each dictionary is a result from the report.
              Returns an empty list if an error occurs.
    """
    if not SPLUNK_URL or not SPLUNK_TOKEN:
        print("Error: SPLUNK_URL and SPLUNK_TOKEN must be set in the .env file.")
        return []

    # The endpoint for exporting search results
    export_endpoint = f"{SPLUNK_URL}/services/search/jobs/export"

    # The search query using 'loadjob' to get results from a saved search
    search_query = f'| loadjob savedsearch="{splunk_user}:{splunk_app}:{report_name}"'

    # Set up headers and payload for the API request
    headers = {
        "Authorization": f"Bearer {SPLUNK_TOKEN}"
    }
    payload = {
        "search": search_query,
        "output_mode": "json"
    }

    try:
        # It's recommended to use a valid SSL cert in production.
        # For development with self-signed certs, verify=False can be used.
        response = requests.post(export_endpoint, headers=headers, data=payload, verify=SPLUNK_CA)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        # The response is a stream of JSON objects, one per line.
        results = [json.loads(line) for line in response.iter_lines() if line]
        return results

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except Exception as err:
        print(f"An error occurred: {err}")

    return []

def load_spl_query(file_path, **kwargs):
    with open(file_path, 'r') as file:
        query = file.read().strip().replace('\n', ' ')
    return query.format(**kwargs)

if __name__ == "__main__":
    # Get Tier 0 Names to Search
    tier_0_names = get_tier_zero_names()
    # print(tier_0_names)

    # Construct SPL query
    query_with_filter = load_spl_query('search.spl', tier_0_names=tier_0_names)

    print(f"Executing Splunk query: {query_with_filter}")

    # Run the query
    filtered_results = execute_splunk_search(query_with_filter)

    if filtered_results:
        print(f"Successfully retrieved {len(filtered_results)} results matching the query.")

        with open('tier_0_members.json', 'w') as f:
            json.dump(filtered_results, f, indent=4)
        
        result_data = [item['result'] for item in filtered_results if 'result' in item]
    
        if result_data:
            # Get column headers from first result
            keys = result_data[0].keys()
            
            # Write to CSV
            with open('tier_0_members.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=keys)
                writer.writeheader()
                writer.writerows(result_data)
            
            print(f"Successfully wrote {len(result_data)} records to tier_0_results.csv")
        else:
            print("No result data found to export csv.")

    else:
        print("Failed to retrieve results or no results matched the query.")
