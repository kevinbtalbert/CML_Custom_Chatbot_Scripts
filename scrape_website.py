import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import os

visited = set()
to_visit = ['https://riversideca.gov/']
max_retries = 5
output_file = '/home/cdsw/data/downloads/urls_visited.txt'

def is_valid_url(url):
    parsed = urlparse(url)
    return parsed.netloc.endswith('riversideca.gov')

def scrape_page(url, retries=0):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            if retries < max_retries:
                print(f"Retrying {url} (attempt {retries + 1})")
                time.sleep(1)
                return scrape_page(url, retries + 1)
            else:
                print(f"Failed to retrieve {url} after {max_retries} attempts")
                return None
    except requests.RequestException as e:
        if retries < max_retries:
            print(f"Retrying {url} (attempt {retries + 1}) due to exception: {e}")
            time.sleep(1)
            return scrape_page(url, retries + 1)
        else:
            print(f"Failed to retrieve {url} after {max_retries} attempts due to exception: {e}")
            return None

def find_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        full_url = urljoin(base_url, href)
        if is_valid_url(full_url) and full_url not in visited:
            links.add(full_url)
    return links

# Ensure the output directory exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

while to_visit:
    current_url = to_visit.pop(0)
    if current_url in visited:
        continue

    print(f"Scraping: {current_url}")
    html = scrape_page(current_url)
    if html is None:
        continue

    visited.add(current_url)
    new_links = find_links(html, current_url)
    to_visit.extend(new_links)

    # Save the visited URL to the file
    with open(output_file, 'a') as f:
        f.write(current_url + '\n')

    # To prevent overwhelming the server
    time.sleep(1)

print(f"Visited {len(visited)} pages.")