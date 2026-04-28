import requests 
import json
from time import sleep
from dotenv import load_dotenv
import os
import pandas as pd
from pyspark.sql import SparkSession
load_dotenv(override=True)


def url_filter(program = "tri",table = 'tri_chem_activity/', range = '1:20/', format = 'json', column = "", join = "", sort = ""):
    
    """
    Creates a URL for the EPA's DMP API based on the provided parameters.
    Keyword arguments:
    program -- **required** the EPA program to query (default "tri")
    table -- **required** the specific table within the program to query (defauly "tri_chem_activity/")
    range -- the range of records to retrieve, formatted as "start:end" (default "1:20/")
    column -- the column to filter
    join -- Combine the results of two tables format  [join_type]/[table]/[comparison]
    sort -- sort results in the field format /sort/[column]:[direction]/. Direction can either be "asc|desc"
    
    **Important**
    URL Format: 
    1. https://data.epa.gov/dmapservice/[table]/[column][operator][value]/[join]/[first]:[last]/[sort]/[format]
    if you get an status code of 400 or 500, check your URL format and paramters
    
    2. Don't forget to add / at the end of each parameters, otherwise you will not get an url:
    
    3. Go to this URL for more information
    https://www.epa.gov/enviro/web-services
    
    """
    
    # Base Url for the EPA DMP API
    base_url = 'https://data.epa.gov/dmapservice'
    if table and range and program:
        url = f"{base_url}/{program}.{table}{column}{join}{range}{sort}{format}"
        return url 
    else:
        print("Error: Missing required parameters. Please provide program, table, and range.")
        return None

def get_data_json(base_url, table = None, range = None):
    """
    Fetches data from the provided URL and returns it as a JSON object.
    Keyword arguments:
    base_url -- the URL to fetch data from
    """
    try:
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: Received status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Error has occurred: {e}")
        return None 

def batch_extraction(table = 'tri_chem_info/',start = 0, end = 5, increment = 5, loop_count = 0):
    """
    Extracts data in batches from the EPA DMP API to avoid hitting rate limits. 
    """
    for i in range(loop_count):
        string_range = f'{start}:{end}'
        print(string_range)
        base_url = url_filter(table = table, range=string_range)
        data = get_data_json(base_url)
        start = end
        end += increment
        json_data = json.dumps(data, indent = 4)
        yield data
        print(f"Batch {i+1} extracted successfully.")
        sleep(30) # Sleep for 30 seconds to avoid hitting API rate limits

def batch_extraction_spark(table = 'tri_chem_info/',start = 1, end = 5, batch = 5, loop_count = 0):
    spark  = SparkSession.builder.appName('BatchProcessing').getOrCreate()
    range = f'{start}:{end}'
    base_url = url_filter(table = table, range = range )
    data = get_data_json(base_url=base_url)
    
    df = pd.DataFrame(data)
    df = df.fillna('')
    
    df_spark = spark.createDataFrame(df)
    df_spark.show()
    
