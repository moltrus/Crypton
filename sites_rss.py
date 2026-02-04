import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse
import requests
import trafilatura
from bs4 import BeautifulSoup
from newspaper import Article
from scrapling.fetchers import StealthyFetcher
from dotenv import load_dotenv
from utils import get_logger

load_dotenv()
logger=get_logger(__name__)

def get_hash_file_path(config: dict) -> str:

    data_path=config.get("data_path", "data/")
    output_folder=config.get("sites", {}).get("output_folder", "rss_feeds")
    hash_file_path=os.path.join(data_path, output_folder, "rss_hashes.json")

    os.makedirs(os.path.dirname(hash_file_path), exist_ok=True)
    return hash_file_path

def load_hashes(config: dict) -> Dict[str, str]:
    hash_file_path=get_hash_file_path(config)
    if os.path.exists(hash_file_path):
        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load hashes from {hash_file_path}: {e}")
            return {}
    return {}

def save_hashes(hashes: Dict[str, str], config: dict) -> None:
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
    output_folder=config.get("sites", {}).get("output_folder", "rss_feeds")

    domain=get_domain(url)
    now=datetime.now()
    date_str=now.strftime("%Y%m%d")
    time_str=now.strftime("%H%M%S")
    filename=f"rss_{date_str}_{time_str}.xml"

    output_dir=Path(data_path) / output_folder / domain
    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir / filename

def fetch_jina_api(url: str) -> Optional[str]:
    jina_url="https://r.jina.ai/"
    jina_api_key=os.getenv("JINA_API_KEY")
    headers={
        "Authorization": f"Bearer {jina_api_key}",
        "Content-Type": "application/json",
        "X-Engine": "browser"
    }
    data={
        "url": url
    }

    try:
        response=requests.post(jina_url, headers=headers, data=json.dumps(data), timeout=120)
        if response.status_code == 200:
            content=response.text
            if "Markdown Content:" in content:
                rss_content=content.split("Markdown Content:", 1)[1].strip()
                logger.info(f"Fetched {url} using Jina API")
                return rss_content
            else:
                logger.warning(f"Jina API response missing Markdown Content for {url}")
                return None
        else:
            logger.warning(f"Jina API failed for {url}: Status code {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Jina API failed for {url}: {str(e)}")
        return None

def fetch_rss(url: str, exclude_headers: list) -> Optional[str]:
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'
    }

    try:
        if url not in exclude_headers:
            response=requests.get(url, headers=headers, timeout=120)
        else:
            response=requests.get(url, timeout=120)
        if response.status_code == 200:
            logger.info(f"Fetched {url} using requests")
            return response.text
        else:
            logger.warning(f"Requests failed for {url}: Status code {response.status_code}")
    except Exception as e:
        logger.error(f"Requests failed for {url}: {str(e)}")

    try:
        article=Article(url)
        article.download()
        if article.html:
            logger.info(f"Fetched {url} using newspaper3k")
            return article.html
        else:
            logger.warning(f"Newspaper3k failed for {url}: No HTML content")
    except Exception as e:
        logger.error(f"Newspaper3k failed for {url}: {str(e)}")

    try:
        downloaded=trafilatura.fetch_url(url)
        if downloaded:
            logger.info(f"Fetched {url} using trafilatura")
            return downloaded
        else:
            logger.warning(f"Trafilatura failed for {url}: No content fetched")
    except Exception as e:
        logger.error(f"Trafilatura failed for {url}: {str(e)}")

    jina_content=fetch_jina_api(url)
    if jina_content:
        return jina_content

    try:
        StealthyFetcher.adaptive=True
        page=StealthyFetcher.fetch(url, headless=False, network_idle=False)
        logger.info(f"Fetched {url} using StealthyFetcher - Status: {page.status}")
        if page.status == 200:
            return str(page)
        else:
            logger.warning(f"StealthyFetcher failed for {url}: {page.status}")
    except Exception as e:
        logger.error(f"StealthyFetcher failed for {url}: {str(e)}")

    logger.error(f"All methods failed for {url}")
    return None

def extract_rss_content(html_content: str) -> Optional[str]:

    if "<?xml" in html_content or "<rss" in html_content:
        return html_content

    soup=BeautifulSoup(html_content, "html.parser")
    rss_tag=soup.find("rss")

    if rss_tag:
        return str(rss_tag)
    else:
        logger.warning("RSS tag not found in content")
        return None

def process_rss_feed(url: str, hashes: Dict[str, str], exclude_headers: list, config: dict) -> bool:

    logger.info(f"Processing RSS feed: {url}")

    content=fetch_rss(url, exclude_headers)
    if not content:
        return False

    rss_content=extract_rss_content(content)
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

