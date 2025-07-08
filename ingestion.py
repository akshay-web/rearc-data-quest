import requests
import boto3
import hashlib
import json
from bs4 import BeautifulSoup
from pathlib import Path

source_url = "https://download.bls.gov/pub/time.series/pr/"
api_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
api_filename = "population_data.json"
bucket_name = "rearc-data-quest-1"
prefix = "bls/"

s3 = boto3.client("s3", region_name="us-east-2")

session = requests.Session()

session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }
)


def list_source_files():
    resp = session.get(source_url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    files = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Only include files in the current directory (not parent links)
        if href.startswith("/pub/time.series/pr/") and not href.endswith("/"):
            filename = href.split("/")[-1]
            files.append(filename)

    return files


def download_file(url, local_path):
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_api_data(download_dir):
    resp = session.get(api_url)
    resp.raise_for_status()
    data = resp.json()

    local_path = download_dir / api_filename

    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    return local_path


def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def file_exists_and_same(filename, local_md5):
    try:
        obj = s3.head_object(Bucket=bucket_name, Key=prefix + filename)
        s3_etag = obj["ETag"].strip('"')
        return s3_etag == local_md5
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_to_s3(file_path, s3_key):
    s3.upload_file(file_path, bucket_name, s3_key)


if __name__ == "__main__":
    files = list_source_files()
    download_dir = Path.cwd() / "tmp"
    download_dir.mkdir(exist_ok=True)

    # Download BLS datasets and upload to S3
    for filename in files:
        file_url = source_url + filename
        local_path = download_dir / filename

        download_file(file_url, local_path)
        local_md5 = compute_md5(local_path)

        if not file_exists_and_same(filename, local_md5):
            upload_to_s3(local_path, prefix + filename)

    # Download API data & upload to S3
    local_path = download_api_data(download_dir)
    local_md5 = compute_md5(local_path)
    if not file_exists_and_same(api_filename, local_md5):
        upload_to_s3(local_path, prefix + api_filename)
