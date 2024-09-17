from flask import Flask, request, jsonify
from pymongo import MongoClient
import json
import urllib.parse

app = Flask(__name__)

# MongoDB connection details
username = urllib.parse.quote_plus('splunkadmin')
password = urllib.parse.quote_plus('Admin@splunk')
host = 'cluster0.a4zwu.mongodb.net'
db_name = "splunk_db"
collection_name = "logs"

# Construct the MongoDB URI with URL encoding
mongo_uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Create an index on the 'timestamp' field
collection.create_index([("system_stats.timestamp", 1)])

# Route to accept logs from the agent
@app.route('/logs', methods=['POST'])
def accept_logs():
    try:
        # Get the incoming JSON data
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Invalid JSON format"}), 400

        # Insert the received data into MongoDB
        collection.insert_one(data)

        # Print the received data for debugging
        print("Received data:")
        print(json.dumps(data, indent=4))

        return jsonify({"message": "Data received and stored successfully"}), 200

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
