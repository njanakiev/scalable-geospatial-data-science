import os
import zipfile
import requests
from bs4 import BeautifulSoup
from io import BytesIO

DST_FOLDERPATH = "data/raw"
GDELT_URL = "http://data.gdeltproject.org/events/"


def get_urls(url):
    r = requests.get(url)
    items = []
    for line in r.text.split('\n'):
        if line.startswith('<LI>'):
            a = BeautifulSoup(line, 'html.parser').find('a')
            items.append({
                'name': a.text,
                'url': url + a['href']
            })
    return items


def download_and_extract(url, dst_folderpath):
    r = requests.get(url)
    with zipfile.ZipFile(BytesIO(r.content)) as zfile:
        zfile.extractall(dst_folderpath)

        
if __name__ == '__main__':
    items = get_urls(GDELT_URL)
    
    # Download and unzip all files
    for item in items:
        if item['name'].endswith('zip') and not item['name'].startswith('GDELT'):
            print(item['name'], item['url'])
            download_and_extract(item['url'], DST_FOLDERPATH)
    
    # Rename all files to lower case
    for filename in os.listdir(DST_FOLDERPATH):
        filepath = os.path.join(DST_FOLDERPATH, filename)
        print("Rename", filepath)
        os.rename(filepath, filepath.lower())
