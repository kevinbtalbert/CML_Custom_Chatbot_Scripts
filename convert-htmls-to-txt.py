import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urlparse
import re

visited_urls = set()
max_retries = 5
retry_delay_seconds = 2

def get_tld(url):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"

def create_directory_path_from_url(base_path, url):
    url = url.replace("http:/", "").replace("https:/", "")
    url_parts = url.strip('/').split('/')
    directory_path = os.path.join(base_path, *url_parts[:-1])
    file_name = f"{url_parts[-1]}.txt"
    file_path = os.path.join(directory_path, file_name)
    return directory_path, file_path

def log_error(url, error_message, base_path):
    error_log_path = os.path.join(base_path, 'html_errors.txt')
    with open(error_log_path, 'a', encoding='utf-8') as error_log_file:
        error_log_file.write(f"Failed to process {url}: {error_message}\n")

def extract_and_write_text(url, base_path, tld):
    if url in visited_urls or not url.startswith(tld):
        return
    visited_urls.add(url)
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                log_error(url, f"Request attempt {attempt} failed: {e}", base_path)
            time.sleep(retry_delay_seconds)
    else:
        log_error(url, f"Failed to fetch {url} after {max_retries} attempts.", base_path)
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    text_content = soup.get_text(separator=' ', strip=True)
    text_content = re.sub(r'\s+', ' ', text_content)

    if url.endswith('.html'):
        url = url[:-5]

    directory_path, file_path = create_directory_path_from_url(base_path, url)
    os.makedirs(directory_path, exist_ok=True)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(text_content)
    except IOError as e:
        log_error(url, f"File write error: {e}", base_path)

def main():
    base_path = "/home/cdsw/data"
    input_file_path = "/home/cdsw/USER_START_HERE/Build_Your_Own_Knowledge_Base_Tools/Python-based_sitemap_scrape/found_htmls.txt"
    
    with open(input_file_path, "r") as file:
        for line in file:
            url = line.strip()
            if url:
                tld = get_tld(url)
                extract_and_write_text(url, base_path, tld)

if __name__ == '__main__':
    main()
