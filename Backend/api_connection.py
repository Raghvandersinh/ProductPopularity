import requests 
import json
from time import sleep
from dotenv import load_dotenv
import os
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

def get_data(base_url):
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

base_url = url_filter()
print(base_url)
data = get_data(base_url)
print(json.dumps(data, indent=4))