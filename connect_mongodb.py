from pymongo import MongoClient, errors
import os
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve the MongoDB password from environment
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=5000, retryWrites=True)

# Specify the database and collection
database = "mth8yq"
collection_name = "dataproject2files"
db = client[database]
collection = db[collection_name]

# Directory containing JSON files
directory = "data"

def import_json_files(directory):
    records_imported = 0
    records_orphaned = 0
    records_corrupted = 0

    for filename in os.listdir(directory):
        if filename.endswith('.json'):  # Ensure the file is a JSON file
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r') as file:
                    file_data = json.load(file)
                    if isinstance(file_data, list):
                        collection.insert_many(file_data)
                        records_imported += len(file_data)
                    else:
                        collection.insert_one(file_data)
                        records_imported += 1
                    logging.info(f"Imported {filename} successfully with {len(file_data) if isinstance(file_data, list) else 1} records.")
            except json.JSONDecodeError as e:
                records_corrupted += 1
                logging.error(f"Corrupted JSON file {filename}: {str(e)}")
            except Exception as e:
                records_orphaned += 1
                logging.error(f"Error when importing {filename}: {str(e)}")
            finally:
                logging.info(f"Processed {filename}.")

    return records_imported, records_orphaned, records_corrupted

def clear_collection():
    try:
        collection.drop()  # Drop the entire collection
        logging.info("Collection cleared successfully.")
    except errors.PyMongoError as e:
        logging.error(f"Could not clear collection: {e}")

# Main execution
if __name__ == "__main__":
    clear_collection()
    records_imported, records_orphaned, records_corrupted = import_json_files(directory)
    logging.info(f"Finished processing.")
    print(f"Records imported: {records_imported}")
    print(f"Records orphaned (complete but not imported): {records_orphaned}")
    print(f"Records corrupted: {records_corrupted}")
