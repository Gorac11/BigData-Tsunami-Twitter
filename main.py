from google.cloud import bigquery
from twython import Twython, TwythonError
import time
import io

def twitterbot():
    client = bigquery.Client()

    dataset=client.get_dataset("myDataset")
    datasetId=dataset.dataset_id
    table=dataset.table("tsunamiTable")
    tableId=table.table_id
    projectId=client.project

    fullDestination=format(projectId)+"."+format(datasetId)+"."+format(tableId)
    job_config = bigquery.QueryJobConfig(destination=fullDestination)       #using arguments

    dataset_ref = client.dataset('myDataset')
    table_ref = dataset_ref.table('tsunamiTable')
    insertTable= client.get_table(table_ref)
 

    query=""" 
        SELECT  timestamp, country, location_name
        FROM `bigquery-public-data.noaa_tsunami.historical_runups` pub
        WHERE NOT EXISTS
            (
            SELECT  null 
            FROM    `myDataset.tsunamiTable` priv
            WHERE   priv.timestamp = pub.timestamp
            )
        And pub.timestamp is not null
    """
    results = client.query(query)
    printedResults=""
    for row in results:
        country = row['country']
        location_name = row['location_name']
        timestamp = row['timestamp']
        if(printedResults!=""):
            printedResults+="\n"
        if country is None:
            printedResults+="New tsunami event located at "+str(timestamp)
        else:
            printedResults+="New tsunami event located at "+str(timestamp)+", "+country+", "
        printedResults+=location_name+". View our map at https://europe-west1-shieldo-1283c.cloudfunctions.net/function-map"
    
    query=""" 
        Insert into `myDataset.tsunamiTable`
        SELECT timestamp, country, location_name
        FROM `bigquery-public-data.noaa_tsunami.historical_runups` pub
        WHERE NOT EXISTS
            (
            SELECT  null 
            FROM    `myDataset.tsunamiTable` priv
            WHERE   priv.timestamp = pub.timestamp
            )
        And pub.timestamp is not null
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete.

    APP_KEY = 'SECRET'
    APP_SECRET = 'SECRET'
    OAUTH_TOKEN = 'SECRET'
    OAUTH_TOKEN_SECRET = 'SECRET'
    twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    stringIO=io.StringIO(printedResults)
    buff=stringIO.readlines()
    printedResults=""

    for line in buff[:]:
    	if len(line)<=280 and len(line)>0:
    		print ("Tweeting...")
    		try:
    			twitter.update_status(status=line)
    		except TwythonError as e:
    			print (e)
    		time.sleep(3)


def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    twitterbot()
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Hello World!'
