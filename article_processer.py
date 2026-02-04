import os
import re
import glob
from html import unescape
from datetime import datetime
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
import uuid as uuid_lib
from langdetect import detect, LangDetectException
import xml.etree.ElementTree as ET
from django.db import IntegrityError, transaction
from extractor import get_article_content
from utils import get_logger
from django_config import RSSFeedArticleModel, ArticleMetadataModel, FailedArticlesModel, ensure_schema
from resolve_url import get_redirected_url

logger=get_logger(__name__)

def clean_unicode_for_logging(text: str)->str:
    if not text:
        return ""
    return ''.join(
        char for char in text
        if ord(char)<65536 and (ord(char)<127 or ord(char)>159)
    )

def detect_language(text: str)->str:
    if not text or len(text.strip())<3:
        return "unknown"
    try:
        lang=detect(text[:500])
        return lang
    except LangDetectException:
        return "unknown"

def convert_list(list_content: str, ordered: bool=False)->str:
    items=re.findall(r'<li[^>]*>(.*?)</li>', list_content, flags=re.DOTALL|re.IGNORECASE)
    markdown_items=[]
    for index, item in enumerate(items):
        item=re.sub(r'<[^>]+>', '', item)
        item=unescape(item)
        item=' '.join(item.split())
        prefix=f"{index+1}. " if ordered else "- "
        markdown_items.append(f"{prefix}{item}")
    return '\n'+'\n'.join(markdown_items)+'\n\n'

def html_to_markdown(text: str)->str:
    if not text:
        return ""
    content=re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    content=re.sub(r'<h1[^>]*>(.*?)</h1>', r'# \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<h2[^>]*>(.*?)</h2>', r'## \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<h3[^>]*>(.*?)</h3>', r'### \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<h4[^>]*>(.*?)</h4>', r'#### \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<h5[^>]*>(.*?)</h5>', r'##### \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<h6[^>]*>(.*?)</h6>', r'###### \1', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n\n', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<i[^>]*>(.*?)</i>', r'*\1*', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<blockquote[^>]*>(.*?)</blockquote>', r'> \1\n', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<ul[^>]*>(.*?)</ul>', lambda m: convert_list(m.group(1), ordered=False), content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<ol[^>]*>(.*?)</ol>', lambda m: convert_list(m.group(1), ordered=True), content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<a[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', r'[\2](\1)', content, flags=re.DOTALL|re.IGNORECASE)
    content=re.sub(r'<img[^>]*src=["\']([^"\']*)["\'][^>]*alt=["\']([^"\']*)["\'][^>]*/?>', r'![\2](\1)', content, flags=re.IGNORECASE)
    content=re.sub(r'<img[^>]*alt=["\']([^"\']*)["\'][^>]*src=["\']([^"\']*)["\'][^>]*/?>', r'![\1](\2)', content, flags=re.IGNORECASE)
    content=re.sub(r'<br[^>]*/?>', '\n', content, flags=re.IGNORECASE)
    content=re.sub(r'<[^>]+>', '', content)
    content=unescape(content)
    content=re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    content=re.sub(r'[ \t]+', ' ', content)
    return content.strip()

def parse_pub_date(pub_date_str: Optional[str])->Optional[datetime]:
    if not pub_date_str:
        return None
    date_formats=[
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(pub_date_str.strip(), fmt)
        except ValueError:
            continue
    logger.error("Could not parse date: %s", pub_date_str)
    return None

def extract_categories(item: ET.Element, source_name: str)->str:
    categories=[]
    for cat in item.findall('category'):
        if cat.text:
            categories.append(cat.text.strip())
    if source_name in ['decrypttoday']:
        media_keywords=item.find('.//{http://search.yahoo.com/mrss/}keywords')
        if media_keywords is not None and media_keywords.text:
            categories.extend(kw.strip() for kw in media_keywords.text.split(','))
    return ', '.join(categories) if categories else ''

def is_valid_url(url: str)->bool:
    if not url or not isinstance(url, str):
        return False
    url=url.strip()
    return url.startswith('http://') or url.startswith('https://')

def extract_source_url(root: ET.Element)->str:
    atom_link=root.find('.//{http://www.w3.org/2005/Atom}link[@rel="self"][@type="application/rss+xml"]')
    if atom_link is not None:
        href=atom_link.get('href')
        if href and is_valid_url(href):
            return href.strip()
    atom_link=root.find('.//{http://www.w3.org/2005/Atom}link[@rel="self"]')
    if atom_link is not None:
        href=atom_link.get('href')
        if href and is_valid_url(href):
            return href.strip()
    channel=root.find('.//channel')
    if channel is not None:
        link=channel.find('link')
        if link is not None and link.text and is_valid_url(link.text):
            return link.text.strip()
    link=root.find('.//link')
    if link is not None and link.text and is_valid_url(link.text):
        return link.text.strip()
    return ""

def write_article_to_db(config: dict, article: Dict)->bool:
    from django_config import AppSettings
    from db_func import log_failed_article
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    rss_model=RSSFeedArticleModel.get_model()
    metadata_model=ArticleMetadataModel.get_model()
    article_uuid=None
    rss_article=None
    try:
        parsed_url=urlparse(article["link"])
        domain=parsed_url.netloc or ""
        article_uuid=article.get("_uuid") or str(uuid_lib.uuid4())
        text_for_lang=article.get("content") or article.get("description") or article.get("title")
        detected_language=detect_language(text_for_lang)
        try:
            rss_article=rss_model.objects.create(
                url=article["link"],
                uuid=article_uuid,
                source_url=article.get("source_url", ""),
                domain=domain,
            )
        except IntegrityError:
            clean_title=clean_unicode_for_logging(article.get("title", "Unknown")[:50])
            logger.info("Article already exists, skipping insert: %s", clean_title)
            return False
        try:
            with transaction.atomic():
                metadata_model.objects.create(
                    uuid=rss_article,
                    url=article["link"],
                    title=article["title"],
                    pub_date=article["pub_date"],
                    description=article["description"],
                    content=article["content"],
                    creator=article.get("creator", ""),
                    category=article.get("given_category", ""),
                    word_count=article.get("word_count", 0),
                    language=detected_language,
                )
        except Exception as metadata_exc:
            log_failed_article(config, article_uuid, type(metadata_exc).__name__, str(metadata_exc))
            raise
        clean_title=clean_unicode_for_logging(article["title"][:50])
        logger.info("Inserted article: %s", clean_title)
        return True
    except Exception as exc:
        clean_title=clean_unicode_for_logging(article.get("title", "Unknown")[:50])
        logger.error("Failed to insert article %s: %s", clean_title, exc)
        return False

def extract_unique_urls_from_xml(xml_file_path: str, source_name: str)->set:
    logger.debug("Extracting URLs from: %s", xml_file_path)
    if not os.path.exists(xml_file_path):
        logger.error("XML file does not exist: %s", xml_file_path)
        return set()
    if os.path.getsize(xml_file_path)==0:
        logger.error("XML file is empty: %s", xml_file_path)
        return set()
    try:
        with open(xml_file_path, 'r', encoding='utf-8') as handle:
            content=handle.read().strip()
            if not content.startswith('<?xml') and not content.startswith('<rss'):
                logger.error("File does not appear to be valid XML: %s", xml_file_path)
                return set()
        tree=ET.parse(xml_file_path)
        root=tree.getroot()
        urls=set()
        for item in root.findall('.//item'):
            link_elem=item.find('link')
            if link_elem is not None and link_elem.text:
                url=link_elem.text.strip()
                if url:
                    urls.add(url)
        logger.debug("Extracted %d unique URLs from %s", len(urls), xml_file_path)
        return urls
    except Exception as exc:
        logger.error("Error extracting URLs from %s: %s", xml_file_path, exc)
        return set()

def process_single_article(config: dict, item: ET.Element, source_name: str, existing_urls: set, source_url: str="")->Tuple[bool, str]:
    from db_func import log_failed_article
    article: Dict[str, Optional[str]]={}
    title_elem=item.find('title')
    article['title']=title_elem.text.strip() if title_elem is not None and title_elem.text else ''
    link_elem=item.find('link')
    article['link']=link_elem.text.strip() if link_elem is not None and link_elem.text else ''
    if not article['link'] or article['link'] in existing_urls:
        return False, "skipped"
    pub_date_elem=item.find('pubDate')
    article['pub_date']=parse_pub_date(pub_date_elem.text) if pub_date_elem is not None and pub_date_elem.text else None
    creator_elem=item.find('.//{http://purl.org/dc/elements/1.1/}creator')
    article['creator']=creator_elem.text.strip() if creator_elem is not None and creator_elem.text else ''
    description_elem=item.find('description')
    article['description']=html_to_markdown(description_elem.text) if description_elem is not None and description_elem.text else ''
    article['given_category']=extract_categories(item, source_name)
    article['source_url']=source_url
    article_uuid=str(uuid_lib.uuid4())
    content_elem=item.find('.//{http://purl.org/rss/1.0/modules/content/}encoded')
    if content_elem is not None and content_elem.text:
        article['content']=html_to_markdown(content_elem.text)
        article['content_source']='xml'
    else:
        fetch_url=article['link']
        if source_name=='news.google.com':
            try:
                resolved_url=get_redirected_url(article['link'])
                if resolved_url:
                    fetch_url=resolved_url
                    article['link']=resolved_url
                    logger.debug("Resolved gnews URL: %s", resolved_url)
                else:
                    error_msg="Failed to resolve gnews URL"
                    logger.error(error_msg+": %s", article['link'])
                    log_failed_article(config, article_uuid, article['link'], "url_resolution_failed", error_msg)
                    return False, "failed"
            except Exception as exc:
                error_msg=f"Exception resolving gnews URL: {str(exc)}"
                logger.error(error_msg)
                log_failed_article(config, article_uuid, article['link'], "url_resolution_error", error_msg)
                return False, "failed"
        try:
            logger.debug("Fetching content for: %s", fetch_url)
            content=get_article_content(fetch_url)
            if content:
                article['content']=content
                article['content_source']='web_fetched'
                logger.debug("Successfully fetched content (%d words)", len(content.split()))
            else:
                error_msg=f"Failed to fetch content from: {fetch_url}"
                article['content']=''
                article['content_source']='web_failed'
                logger.error(error_msg)
                log_failed_article(config, article_uuid, article['link'], "content_extraction_failed", error_msg)
                return False, "failed"
        except Exception as exc:
            error_msg=f"Error fetching content for {fetch_url}: {str(exc)}"
            logger.error(error_msg)
            log_failed_article(config, article_uuid, article['link'], "content_extraction_error", error_msg)
            article['content']=''
            article['content_source']='web_error'
            return False, "failed"
    article['source_name']=source_name
    article['extracted_category']=''
    article['http_status']=200
    article['word_count']=len(article['content'].split()) if article['content'] else 0
    article['_uuid']=article_uuid
    success=write_article_to_db(config, article)
    if success:
        existing_urls.add(article['link'])
    return success, "processed" if success else "failed"

def process_xml_file_efficiently(config: dict, xml_file_path: str, source_name: str, existing_urls: set)->Dict[str, int]:
    logger.info("Processing: %s", xml_file_path)
    if not os.path.exists(xml_file_path):
        logger.error("XML file does not exist: %s", xml_file_path)
        return {"processed": 0, "skipped": 0, "failed": 0}
    if os.path.getsize(xml_file_path)==0:
        logger.error("XML file is empty: %s", xml_file_path)
        return {"processed": 0, "skipped": 0, "failed": 0}
    try:
        with open(xml_file_path, 'r', encoding='utf-8') as handle:
            content=handle.read().strip()
            if not content.startswith('<?xml') and not content.startswith('<rss'):
                logger.error("File does not appear to be valid XML: %s", xml_file_path)
                return {"processed": 0, "skipped": 0, "failed": 0}
        tree=ET.parse(xml_file_path)
        root=tree.getroot()
        source_url=extract_source_url(root)
        if source_url:
            logger.debug("Extracted source_url: %s", source_url)
        items=root.findall('.//item')
        logger.info("Found %d items in %s", len(items), xml_file_path)
        stats={"processed": 0, "skipped": 0, "failed": 0}
        for index, item in enumerate(items, 1):
            success, status=process_single_article(config, item, source_name, existing_urls, source_url)
            stats[status]+=1
            if index%10==0:
                logger.info(
                    "Progress: %d/%d articles processed from %s",
                    index,
                    len(items),
                    os.path.basename(xml_file_path),
                )
        logger.info(
            "Completed %s: %d processed, %d skipped, %d failed",
            xml_file_path,
            stats['processed'],
            stats['skipped'],
            stats['failed'],
        )
        return stats
    except Exception as exc:
        logger.error("Error processing %s: %s", xml_file_path, exc)
        return {"processed": 0, "skipped": 0, "failed": 0}

def parse_xml_file(xml_file_path: str, source_name: str)->list:
    logger.info("Processing: %s", xml_file_path)
    if not os.path.exists(xml_file_path):
        logger.error("XML file does not exist: %s", xml_file_path)
        return []
    if os.path.getsize(xml_file_path)==0:
        logger.error("XML file is empty: %s", xml_file_path)
        return []
    try:
        with open(xml_file_path, 'r', encoding='utf-8') as handle:
            content=handle.read().strip()
            if not content.startswith('<?xml') and not content.startswith('<rss'):
                logger.error("File does not appear to be valid XML: %s", xml_file_path)
                return []
        tree=ET.parse(xml_file_path)
        root=tree.getroot()
        source_url=extract_source_url(root)
        articles=[]
        items=root.findall('.//item')
        logger.info("Found %d items in %s", len(items), xml_file_path)
        for item in items:
            article={}
            title_elem=item.find('title')
            article['title']=title_elem.text.strip() if title_elem is not None and title_elem.text else ''
            link_elem=item.find('link')
            article['link']=link_elem.text.strip() if link_elem is not None and link_elem.text else ''
            if not article['link']:
                continue
            pub_date_elem=item.find('pubDate')
            article['pub_date']=parse_pub_date(pub_date_elem.text) if pub_date_elem is not None and pub_date_elem.text else None
            creator_elem=item.find('.//{http://purl.org/dc/elements/1.1/}creator')
            article['creator']=creator_elem.text.strip() if creator_elem is not None and creator_elem.text else ''
            description_elem=item.find('description')
            article['description']=html_to_markdown(description_elem.text) if description_elem is not None and description_elem.text else ''
            article['given_category']=extract_categories(item, source_name)
            article['source_url']=source_url
            content_elem=item.find('.//{http://purl.org/rss/1.0/modules/content/}encoded')
            if content_elem is not None and content_elem.text:
                article['content']=html_to_markdown(content_elem.text)
                article['content_source']='xml'
            else:
                article['content']=''
                article['content_source']='web_needed'
            article['source_name']=source_name
            article['extracted_category']=''
            article['word_count']=len(article['content'].split()) if article['content'] else 0
            articles.append(article)
        return articles
    except Exception as exc:
        logger.error("Error parsing %s: %s", xml_file_path, exc)
        return []

def process_source_directory(config: dict, source_path: str, source_name: str)->None:
    from db_func import get_existing_urls
    xml_files=glob.glob(os.path.join(source_path, '*.xml'))
    if not xml_files:
        logger.warning("No XML files found in %s", source_path)
        return
    logger.info("Processing %d XML files for %s", len(xml_files), source_name)
    existing_urls=get_existing_urls(config, source_name)
    logger.info("Phase 1: Collecting unique URLs from %d XML files...", len(xml_files))
    all_urls_from_xml=set()
    for xml_file in sorted(xml_files):
        urls_from_file=extract_unique_urls_from_xml(xml_file, source_name)
        all_urls_from_xml.update(urls_from_file)
    new_urls=all_urls_from_xml-existing_urls
    logger.info(
        "Found %d total URLs, %d already exist, %d are new",
        len(all_urls_from_xml),
        len(existing_urls),
        len(new_urls),
    )
    if not new_urls:
        logger.info("No new articles to process for %s", source_name)
        return
    logger.info("Phase 2: Processing articles from XML files...")
    total_stats={"processed": 0, "skipped": 0, "failed": 0}
    for xml_file in sorted(xml_files):
        file_urls=extract_unique_urls_from_xml(xml_file, source_name)
        file_new_urls=file_urls-existing_urls
        if file_new_urls:
            logger.info("Processing %s - contains %d new URLs", xml_file, len(file_new_urls))
            file_stats=process_xml_file_efficiently(config, xml_file, source_name, existing_urls)
            for key in total_stats:
                total_stats[key]+=file_stats[key]
        else:
            logger.debug("Skipping %s - no new URLs", xml_file)
    logger.info(
        "Source %s completed: %d processed, %d skipped, %d failed",
        source_name,
        total_stats['processed'],
        total_stats['skipped'],
        total_stats['failed'],
    )
