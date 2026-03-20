import requests 
import json
from warcio.archiveiterator import ArchiveIterator as ai
import io
from time import sleep

url = "https://index.commoncrawl.org/collinfo.json"
headers = {"User-Agent": "Mozilla/5.0"}

for attempt in range(5):
    try:
        response = requests.get(url, headers=headers, timeout = 10)
        response.raise_for_status()
        catalog = response.json()
        all_ids = [c['id'] for c in catalog]
        print("Collection IDs:", all_ids)
        break
    except requests.exceptions.RequestException as e:
        print(f"Attempts: {attempt + 1} failed {e}")
        sleep(2)

# endpoint = f"https://index.commoncrawl.org/{latest_index}-index"

# params = {
#     "url": 'amazon.com',
#     "output": 'json',
#     "limit" : 5
# }

# res = requests.get(endpoint, params=params)
# print(res)

# records = [json.loads(line) for line in res.text.strip().split('\n')]

# s3_urls = f"https://data.commoncrawl.org/{records[0]['filename']}"
# offset = int(records[0]['offset'])
# length = int(records[0]['length'])

# headers = {"Range": f"bytes={offset}-{offset+length-1}"}
# warc_res = requests.get(s3_urls,headers=headers)

# print(warc_res.status_code)

# stream = io.BytesIO(warc_res.content)
# encoding =  records[0]['encoding']
# print(type(encoding))
# print(f"Encoding: {encoding}, Type: {type(encoding)}")

# for rec in ai(stream):
#     if rec.rec_type == 'response':
#         html = rec.content_stream().read().decode(encoding, errors='ignore')
#         print(html[:500])
        