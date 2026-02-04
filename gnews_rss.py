import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from utils import get_logger

logger=get_logger(__name__)

def get_hash_file_path(config: dict) -> str:
    data_path=config.get("data_path", "data/")
    output_folder=config.get("gnews", {}).get("output_folder", "rss_feeds")
    hash_file_path=os.path.join(data_path, output_folder, "rss_hashes.json")

    os.makedirs(os.path.dirname(hash_file_path), exist_ok=True)
    return hash_file_path

def load_hashes(config: dict) -> dict[str, str]:
    hash_file_path=get_hash_file_path(config)
    if os.path.exists(hash_file_path):
        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load hashes from {hash_file_path}: {e}")
            return {}
    return {}

def save_hashes(hashes: dict[str, str], config: dict) -> None:
    hash_file_path=get_hash_file_path(config)
    try:
        with open(hash_file_path, "w", encoding="utf-8") as f:
            json.dump(hashes, f, indent=2)
        logger.info(f"Hashes saved successfully to {hash_file_path}")
    except Exception as e:
        logger.error(f"Failed to save hashes to {hash_file_path}: {e}")

def calculate_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def get_domain(url: str) -> str:
    parsed=urlparse(url)
    domain=parsed.netloc.replace("www.", "")
    return domain

def get_output_path(url: str, config: dict) -> Path:

    data_path=config.get("data_path", "data/")
    output_folder=config.get("gnews", {}).get("output_folder", "rss_feeds")

    domain=get_domain(url)
    now=datetime.now()
    date_str=now.strftime("%Y%m%d")
    time_str=now.strftime("%H%M%S")
    filename=f"rss_{date_str}_{time_str}.xml"

    output_dir=Path(data_path) / output_folder / domain
    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir / filename

def fetch_rss(url: str) -> str | None:
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'
    }

    try:
        response=requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            logger.info(f"Fetched {url} using requests")
            return response.text
    except Exception as e:
        logger.error(f"Requests failed for {url}: {str(e)}")

    logger.error(f"Failed to fetch {url}")
    return None

def parse_and_format_rss(content: str) -> str | None:
    try:
        soup=BeautifulSoup(content, 'xml')

        rss=soup.find('rss')
        if not rss:
            logger.error("No RSS tag found in content")
            return None

        formatted_xml=soup.prettify()
        return formatted_xml
    except Exception as e:
        logger.error(f"Failed to parse RSS with xml parser: {e}")
        try:
            soup=BeautifulSoup(content, 'html.parser')
            rss=soup.find('rss')
            if rss:
                formatted_xml=soup.prettify()
                return formatted_xml
        except Exception as e2:
            logger.error(f"Failed to parse RSS with html parser: {e2}")
        return None

def process_rss_feed(url: str, hashes: dict[str, str], config: dict) -> bool:
    logger.info(f"Processing RSS feed: {url}")

    content=fetch_rss(url)
    if not content:
        return False

    rss_content=parse_and_format_rss(content)
    if not rss_content:
        return False

    content_hash=calculate_hash(rss_content)
    logger.info(f"Content hash: {content_hash}")

    previous_hash=hashes.get(url)
    if previous_hash == content_hash:
        logger.info(f"No changes detected for {url} - skipping")
        return False

    if previous_hash:
        logger.info(f"Hash changed for {url}")
    else:
        logger.info(f"First time processing {url}")

    output_path=get_output_path(url, config)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rss_content)
        logger.info(f"Saved RSS to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save RSS: {e}")
        return False

    hashes[url]=content_hash

    return True

def build_gnews_rss_url(**kwargs) -> str:
    # https://news.google.com/rss/search?q=crypto+site=tradingview.com&hl=en-IN&gl=IN&ceid=IN:en
    url="https://news.google.com/rss/search?"
    for key, value in kwargs.items():
        url += f"{key}={value}&"
    return url.rstrip("&")
