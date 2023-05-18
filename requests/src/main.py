# Allows us to connect to the data source and pulls the information
from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import argparse
import sys
import os

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='311 Requests Data')
# In the parse, we have two arguments to add.
# The first one is a required argument for the program to run. If page_size is not passed in, don’t let the program to run
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
# The second one is an optional argument for the program to run. It means that with or without it your program should be able to work.
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
# Take the command line arguments passed in (sys.argv) and pass them through the parser.
# Then you will ned up with variables that contains page size and num pages.  
args = parser.parse_args(sys.argv[1:])
print(args)

# Let’s comment hardcoded values out and create environment variables
#This comes from the documentation:
#https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
#DATASET_ID="erm2-nwe9"
#APP_TOKEN="oi6y3R0tNwsUPP7VpCvBCUjCL"
#ES_HOST="https://search-cis9760-kwesi-sullivan-tdev5s2iggifwbns66xysirbg4.us-east-2.es.amazonaws.com"
#ES_USERNAME="ksulliv01"
#ES_PASSWORD="Cis9760*"
#INDEX_NAME="payroll"

DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]

if __name__ == '__main__': 
    #You can comment this out
    #get() sends a GET request to the specified url. In our case, it is OpenSearch Domain Endpoint URL (ES_HOST)
    #resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
    #print(resp.json())

    try:
        #Using requests.put(), we are creating an index (db) first.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                #We are specifying the columns and define what we want the data to be.
                #However, it is not guaranteed that the data will come us clean. 
                #We will might need to clean it in the next steps.
                #If the data you're pushing to the Elasticsearch is not compatible with these definitions, 
                #you'll either won't be able to push the data to Elasticsearch in the next steps 
                #and get en error due to that or the columns will not be usable in Elasticsearch 
                "mappings": {
                    "properties": {
                        "unique_key": {"type": "keyword"},
                        "created_date": {"type": "date"},
                        "complaint_type": {"type": "keyword"},
                        "descriptor": {"type": "keyword"},
                        "location_type": {"type": "keyword"},
                        "city": {"type": "keyword"},
                        "borough": {"type": "keyword"},
                        "incident_zip": {"type": "float"}, #This should normally be considered for keyword 
                        #but I need a numeric field for the next steps. 
                    }
                },
            }
        )
        resp.raise_for_status()
        print(resp.json())
        
    #If we send another put() request after creating an index (first put() request), the pogram will give an error and crash.
    #In order to avoid it, we use try and except here.
    #If the index is already created, it will raise an excepion and the program will not be crashed. 
    except Exception as e:
        print("Index already exists! Skipping")    
    
    # Remove the comments
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    #Change the hardcoded page size to args.page_size
    #rows = client.get(DATASET_ID, limit=10)
    rows = client.get(DATASET_ID, limit=args.page_size)
    #print(rows)
    es_rows=[]
    
    #You can comment this out
    # Print out a few specific columns instead of the entire output
    #for row in rows:
    #    print (row["unique_key"], row["created_date"])

    #Let's comment this out
    #for row in rows:
    #    try:
    #        print (row["unique_key"], row["created_date"], row["complaint_type"], row["city"])
    #    except Exception as e:
    #        print (f"Error!: {e}, skipping row: {row}")
    #        continue

    for row in rows:
        try:
            # Convert
            es_row = {}
            es_row["unique_key"] = row["unique_key"]
            es_row["created_date"] = row["created_date"]
            es_row["complaint_type"] = row["complaint_type"]
            es_row["location_type"] = row["location_type"]
            es_row["city"] = row["city"]
            es_row["borough"] = row["borough"]
            #The data that we will collect comes in string. That's why we might need to do a cleaning here. 
            es_row["incident_zip"] = float(row["incident_zip"]) 
            
            #print(es_row)
        
        #There might be still some bad data coming from the source
        #For instance, incident_zip might have N/A instead of numerice values.
        #In this case, the conversion will not work and the program will crash.
        #We do not want that. That's why we raise an exception here. 
        except Exception as e:
            print (f"Error!: {e}, skipping row: {row}")
            continue
        
        es_rows.append(es_row)
        print(es_rows)
    
        #Let's comment this out
        #try:
        #    # Upload to Elasticsearch by creating a document
        #    resp = requests.post(f"{ES_HOST}/{INDEX_NAME}/_doc",
        #            json=es_row,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),)
        #    resp.raise_for_status()

            # If it fails, skip that row and move on.
        #except Exception as e:
        #    print(f"Failed to insert in ES: {e}, skipping row: {row}")
        #    continue
        
        #print(resp.json())
    
    bulk_upload_data = ""
    for line in es_rows:
        print(f'Handling row {line["unique_key"]}')
        action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["unique_key"] + '"}}'
        data = json.dumps(line)
        bulk_upload_data += f"{action}\n"
        bulk_upload_data += f"{data}\n"
    #print (bulk_upload_data)
    
    try:
        # Upload to Elasticsearch by creating a document
        resp = requests.post(f"{ES_HOST}/_bulk",
            # We upload es_row to Elasticsearch
                    data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
        resp.raise_for_status()
        print ('Done')
            
        # If it fails, skip that row and move on.
    except Exception as e:
        print(f"Failed to insert in ES: {e}")
