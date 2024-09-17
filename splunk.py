import psutil
import time
import json
import os
import re
import pymongo
from pymongo import MongoClient
from urllib.parse import quote_plus

# MongoDB connection details
username = quote_plus('splunkadmin')
password = quote_plus('Admin@splunk')
host = 'cluster0.a4zwu.mongodb.net'
db_name = "splunk_db"
collection_name = "logs01"

# Construct the MongoDB URI with URL encoding
mongo_uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Create an index on the 'timestamp' field if it doesn't exist
collection.create_index([("system_stats.timestamp", pymongo.ASCENDING)])

# Function to collect system logs (e.g., syslog for Linux)
def collect_system_logs():
    try:
        if os.path.exists("/var/log/syslog"):
            with open("/var/log/syslog", "r") as log_file:
                logs = log_file.read()
                return logs
        else:
            return "No syslog available"
    except Exception as e:
        return f"Error collecting logs: {e}"

# Function to collect system stats (CPU, memory, disk, and network usage)
def collect_system_stats():
    data = {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory": psutil.virtual_memory()._asdict(),
        "disk_usage": psutil.disk_usage('/')._asdict(),
        "network_io": psutil.net_io_counters()._asdict(),
        "timestamp": time.time()
    }
    return data

# Function to determine log format and parse accordingly
def parse_logs(log_data):
    try:
        # Check if logs are JSON
        if is_json(log_data):
            return {"format": "JSON", "data": json.loads(log_data)}
        
        # Check if logs are in CSV format
        elif ',' in log_data:
            return {"format": "CSV", "data": parse_csv_log(log_data)}
        
        # Default to Syslog parsing
        else:
            parsed = parse_syslog(log_data)
            if parsed:
                return {"format": "Syslog", "data": parsed}
            else:
                return {"format": "Syslog", "data": "Unable to parse Syslog logs"}
    
    except Exception as e:
        return {"error": f"Error parsing logs: {e}"}

# Helper function to check if a string is valid JSON
def is_json(data):
    try:
        json.loads(data)
        return True
    except ValueError:
        return False

# Function to parse Syslog
def parse_syslog(log_entry):
    pattern = r'(?P<timestamp>\w+ \d+ \d+:\d+:\d+) (?P<hostname>\w+) (?P<process>\w+\[\d+\]): (?P<message>.+)'
    match = re.match(pattern, log_entry)
    return match.groupdict() if match else None

# Function to parse CSV logs
def parse_csv_log(log_data):
    import csv
    csv_reader = csv.DictReader(log_data.splitlines())
    return [row for row in csv_reader]

# Function to format the system stats and parsed logs as JSON
def format_data_as_json(system_stats, parsed_logs):
    data = {
        "system_stats": system_stats,
        "parsed_logs": parsed_logs
    }
    return data

# Function to store data in MongoDB
def store_data_in_mongo(data):
    try:
        collection.insert_one(data)
        print("Data stored successfully in MongoDB")
    except Exception as e:
        print(f"Error storing data in MongoDB: {e}")

if __name__ == "__main__":
    interval_seconds = 10  # Time interval for data collection

    while True:
        # Collect system logs and stats
        system_logs = collect_system_logs()
        system_stats = collect_system_stats()

        # Parse logs (handle JSON, CSV, Syslog formats)
        parsed_logs = parse_logs(system_logs)
        
        # Debug print to check parsed logs
        print("Parsed logs:")
        print(json.dumps(parsed_logs, indent=4))

        # Format the data as JSON
        formatted_data = format_data_as_json(system_stats, parsed_logs)

        # Print to console for debugging purposes
        print(json.dumps(formatted_data, indent=4))

        # Store the data in MongoDB
        store_data_in_mongo(formatted_data)

        # Wait for the next interval
        time.sleep(interval_seconds)
