"""
Tableau Packaged Data Source SQL Extractor
Extracts initial SQL queries from .tdsx files
Supports both local files and direct download from Tableau Server/Cloud
"""

import zipfile
import xml.etree.ElementTree as ET
import os
import sys
import tempfile
import re
from pathlib import Path
from urllib.parse import urlparse
import requests


def download_from_tableau_server(url, access_token=None):
    """
    Download a .tdsx file from Tableau Server/Cloud
    
    Supports URLs in format:
    - https://tableau-server.com/#/site/sitename/datasources/12345
    - Direct REST API URL
    
    Args:
        url: Tableau Server/Cloud URL or REST API endpoint
        access_token: Optional access token for authentication
    
    Returns:
        Path to downloaded temporary file, or None if failed
    """
    print(f"Attempting to download from: {url}")
    
    # Parse the URL to extract components
    parsed = urlparse(url)
    
    # Check if it's a direct REST API call or a web UI URL
    if '/api/' in url:
        api_url = url
    else:
        # Try to extract site and datasource ID from web URL
        # Format: /#/site/sitename/datasources/datasource-id
        match = re.search(r'/site/([^/]+)/datasources/([a-zA-Z0-9\-]+)', url)
        
        if not match:
            print("Error: Could not parse Tableau URL. Expected format:")
            print("  https://your-server.com/#/site/sitename/datasources/datasource-id")
            return None
        
        site_name = match.group(1)
        datasource_id = match.group(2)
        
        # Construct the REST API URL
        # Note: This uses API version 3.17, adjust as needed
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # For Tableau Cloud, we need to resolve site name to site ID
        # This is simplified - in production you'd call the sites API first
        api_url = f"{base_url}/api/3.17/sites/{site_name}/datasources/{datasource_id}/content"
        print(f"Constructed API URL: {api_url}")
    
    # Set up headers
    headers = {
        'Accept': 'application/octet-stream'
    }
    
    # Add authentication if provided
    if access_token:
        headers['Authorization'] = f'Bearer {access_token}'
        headers['X-Tableau-Auth'] = access_token
    else:
        print("\nNote: No access token provided. This may fail if authentication is required.")
        print("Usage with token: python tableau_sql_extractor.py <url> --token <your_token>")
    
    try:
        # Make the request
        print("Downloading...")
        response = requests.get(api_url, headers=headers, stream=True)
        response.raise_for_status()
        
        # Save to temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.tdsx')
        
        # Write in chunks
        for chunk in response.iter_content(chunk_size=8192):
            temp_file.write(chunk)
        
        temp_file.close()
        print(f"Downloaded successfully to temporary file: {temp_file.name}")
        return temp_file.name
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if e.response.status_code == 401:
            print("Authentication failed. Please provide a valid access token using --token")
        elif e.response.status_code == 404:
            print("Data source not found. Check the URL and your permissions.")
        return None
    except Exception as e:
        print(f"Error downloading from Tableau Server: {e}")
        return None


def extract_sql_from_tdsx(tdsx_path, output_dir=None):
    """
    Extract initial SQL queries from a Tableau packaged data source (.tdsx)
    
    Args:
        tdsx_path: Path to the .tdsx file
        output_dir: Optional directory to save extracted SQL files
    
    Returns:
        Dictionary with connection names as keys and SQL queries as values
    """
    sql_queries = {}
    
    if not os.path.exists(tdsx_path):
        print(f"Error: File not found: {tdsx_path}")
        return sql_queries
    
    try:
        # Open the .tdsx file (it's a zip archive)
        with zipfile.ZipFile(tdsx_path, 'r') as zip_ref:
            # Find the .tds file inside (usually named Data/Datasources/*.tds)
            tds_files = [f for f in zip_ref.namelist() if f.endswith('.tds')]
            
            if not tds_files:
                print("No .tds file found in the packaged data source")
                return sql_queries
            
            # Process each .tds file found
            for tds_file in tds_files:
                print(f"\nProcessing: {tds_file}")
                
                # Read the XML content
                with zip_ref.open(tds_file) as xml_file:
                    xml_content = xml_file.read()
                
                # Parse the XML
                root = ET.fromstring(xml_content)
                
                # Find all connection elements with initial SQL
                # Tableau uses 'relation' elements with 'type="text"' for custom SQL
                connections = root.findall('.//connection')
                
                for idx, connection in enumerate(connections, 1):
                    # Get connection name/class for identification
                    conn_name = connection.get('class', f'connection_{idx}')
                    
                    # Look for relation elements (custom SQL)
                    relations = connection.findall('.//relation[@type="text"]')
                    
                    for rel_idx, relation in enumerate(relations, 1):
                        sql_text = relation.text
                        
                        if sql_text and sql_text.strip():
                            key = f"{conn_name}_query_{rel_idx}" if len(relations) > 1 else conn_name
                            sql_queries[key] = sql_text.strip()
                            print(f"  Found SQL in: {key}")
                
                # Also check for named-connection elements with initial-sql
                named_connections = root.findall('.//named-connection')
                for named_conn in named_connections:
                    name = named_conn.get('name', 'unnamed')
                    connection_elem = named_conn.find('.//connection')
                    
                    if connection_elem is not None:
                        # Check for one-time-sql attribute
                        initial_sql = connection_elem.get('one-time-sql')
                        if initial_sql and initial_sql.strip():
                            sql_queries[f"{name}_initial_sql"] = initial_sql.strip()
                            print(f"  Found initial SQL in: {name}")
    
    except zipfile.BadZipFile:
        print(f"Error: {tdsx_path} is not a valid zip file")
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    # Save to files if output directory specified
    if output_dir and sql_queries:
        os.makedirs(output_dir, exist_ok=True)
        
        for name, sql in sql_queries.items():
            # Create safe filename
            safe_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in name)
            output_path = os.path.join(output_dir, f"{safe_name}.sql")
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(sql)
            
            print(f"Saved: {output_path}")
    
    return sql_queries


def cli():
    """Command line interface entry point"""
    # Check for help flag
    if len(sys.argv) < 2 or sys.argv[1] in ['--help', '-h', 'help']:
        print("Tableau SQL Extractor - Extract SQL queries from .tdsx files")
        print("\nUsage: tableau-sql <path_or_url> [output_directory] [--token <access_token>]")
        print("\nExamples:")
        print("  Local file:")
        print("    tableau-sql mydata.tdsx")
        print("    tableau-sql mydata.tdsx ./extracted_sql")
        print("\n  Tableau Server/Cloud URL:")
        print("    tableau-sql 'https://tableau.com/#/site/mysite/datasources/abc123'")
        print("    tableau-sql 'https://tableau.com/#/site/mysite/datasources/abc123' --token YOUR_TOKEN")
        print("\n  Direct REST API URL:")
        print("    tableau-sql 'https://tableau.com/api/3.17/sites/site-id/datasources/ds-id/content' --token YOUR_TOKEN")
        print("\nOptions:")
        print("  --token <token>    Access token for Tableau Server/Cloud authentication")
        print("  --help, -h         Show this help message")
        sys.exit(0 if len(sys.argv) > 1 else 1)
    
    # Parse arguments
    path_or_url = sys.argv[1]
    output_dir = None
    access_token = None
    
    # Parse remaining arguments
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--token' and i + 1 < len(sys.argv):
            access_token = sys.argv[i + 1]
            i += 2
        elif not output_dir and not sys.argv[i].startswith('--'):
            output_dir = sys.argv[i]
            i += 1
        else:
            i += 1
    
    # Check if it's a URL or local file
    is_url = path_or_url.startswith('http://') or path_or_url.startswith('https://')
    temp_file = None
    
    try:
        if is_url:
            # Download from Tableau Server/Cloud
            tdsx_path = download_from_tableau_server(path_or_url, access_token)
            if not tdsx_path:
                sys.exit(1)
            temp_file = tdsx_path
        else:
            # Use local file
            tdsx_path = path_or_url
        
        print(f"\nExtracting SQL from: {tdsx_path}")
        
        sql_queries = extract_sql_from_tdsx(tdsx_path, output_dir)
        
        if sql_queries:
            print(f"\n{'='*60}")
            print(f"Found {len(sql_queries)} SQL query/queries:")
            print(f"{'='*60}\n")
            
            for name, sql in sql_queries.items():
                print(f"--- {name} ---")
                print(sql)
                print(f"\n{'-'*60}\n")
        else:
            print("\nNo SQL queries found in the data source.")
    
    finally:
        # Clean up temporary file if we downloaded one
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
                print(f"\nCleaned up temporary file: {temp_file}")
            except:
                pass


if __name__ == "__main__":
    cli()