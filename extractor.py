import requests
import os
import trafilatura
from bs4 import BeautifulSoup
from newspaper import Article
from readability import Document
from utils import get_logger
from dotenv import load_dotenv
from camoufox import Camoufox
from browserforge.fingerprints import FingerprintGenerator
import yaml
from urllib.parse import urlparse

load_dotenv()

logger=get_logger(__name__)

def load_config() -> dict:
    config_file="config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_file}: {e}")
        return {}

def get_extraction_method_for_site(url: str) -> str:
    try:
        config = load_config()
        extractor_config = config.get("extractor_mapping", {})
        default_method = config.get("default_extractor_method", "all")

        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]

        if isinstance(extractor_config, dict):
            for method, sites in extractor_config.items():
                if isinstance(sites, list):
                    for site in sites:
                        if site.lower() in domain or domain in site.lower():
                            logger.info(f"Found extraction method '{method}' for site '{domain}'")
                            return method

        logger.info(f"Using default extraction method '{default_method}' for site '{domain}'")
        return default_method

    except Exception as e:
        logger.error(f"Error determining extraction method for {url}: {e}")
        return "all"

def extract_with_newspaper(url: str, proxy: str = None) -> str|None:
    try:
        article=Article(url, browser_user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
        article.download()
        article.parse()
        if len(article.text.strip()) > 100:
            logger.info("Successfully fetched content using newspaper3k.")
            return article.text
        else:
            logger.warning(f"newspaper3k: Content too short ({len(article.text.strip())} chars). Content: {article.text.strip()[:200]}")
    except Exception as e:
        logger.warning(f"Error in newspaper3k: {type(e).__name__}: {e}")
    return None

def extract_with_trafilatura(url: str, proxy: str = None) -> str|None:
    try:
        downloaded=trafilatura.fetch_url(url)
        content=trafilatura.extract(downloaded)
        if content and len(content.strip()) > 100:
            logger.info("Successfully fetched content using trafilatura.")
            return content
        else:
            logger.warning(f"trafilatura: Content too short or empty. Content: {content.strip()[:200] if content else 'None'}")
    except Exception as e:
        logger.warning(f"Error in trafilatura: {type(e).__name__}: {e}")
    return None

def extract_with_readability(url: str, proxy: str = None) -> str|None:
    try:
        proxies = {'http': proxy, 'https': proxy} if proxy else None
        response=requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}, proxies=proxies, timeout=10)
        doc=Document(response.text)
        html=doc.summary()
        text=BeautifulSoup(html, 'html.parser').get_text()
        if len(text.strip()) > 100:
            logger.info("Successfully fetched content using readability-lxml.")
            logger.warning(f"Used readability-lxml to fetch content. Cross check in database. Url: {url}")
            return text
        else:
            logger.warning(f"readability-lxml: Content too short ({len(text.strip())} chars). Content: {text.strip()[:200]}")
    except Exception as e:
        logger.warning(f"Error in readability-lxml: {type(e).__name__}: {e}")
    return None

def extract_with_playwright(url: str, proxy: str = None) -> str|None:
    try:
        fg = FingerprintGenerator()
        fingerprint = fg.generate()
        headers = fg.header_generator.generate()
        proxy_dict = {'server': proxy} if proxy else None
        camoufox = Camoufox(
            headless=True,
            fingerprint=fingerprint,
            proxy=proxy_dict,
            i_know_what_im_doing=True,
        )
        browser = camoufox.start()
        context = browser.new_context(
            viewport={'width': fingerprint.screen.width, 'height': fingerprint.screen.height},
            locale=fingerprint.navigator.language,
        )
        page = context.new_page()
        page.set_extra_http_headers(headers)
        page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [{}]});
        """)
        page.goto(url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_load_state('networkidle')
        page.evaluate("window.scrollTo(0, Math.floor(Math.random() * 500))")
        page.wait_for_timeout(1000)
        html = page.content()
        browser.close()
        content = trafilatura.extract(html)
        if content and len(content.strip()) > 100:
            logger.info("Successfully fetched content using Playwright with stealth.")
            return content
        else:
            logger.warning(f"Playwright: Content too short or empty. Content: {content.strip()[:200] if content else 'None'}")
    except Exception as e:
        logger.warning(f"Error in Playwright: {type(e).__name__}: {e}")
    return None

def extract_with_jina(url: str, use_x_base: bool = False, x_base_value: str = "final", proxy: str = None) -> str|None:
    try:
        jina_key = os.getenv("JINA_API_KEY")
        if jina_key:
            headers = {
                "Authorization": f"Bearer {jina_key}",
                "Content-Type": "application/json",
                "X-Retain-Images": "none",
            }
            if use_x_base and x_base_value:
                headers["X-Base"] = x_base_value

            data = {"url": url}
            try:
                resp = requests.post("https://r.jina.ai/", headers=headers, json=data, timeout=180)
                if resp.ok and resp.text and len(resp.text.strip()) > 100:
                    logger.info("Successfully fetched content using Jina Reader API.")
                    return resp.text
                else:
                    logger.warning(f"Jina API: Response too short or not ok. status={resp.status_code}, content={resp.text.strip()[:200]}")
            except Exception as e:
                logger.warning(f"Error posting to Jina API: {type(e).__name__}: {e}")
        else:
            logger.warning("JINA_API_KEY not set; skipping Jina API call.")
    except Exception as e:
        logger.warning(f"Error preparing Jina API call: {type(e).__name__}: {e}")
    return None

def get_article_content(url: str, method: str = None, use_x_base: bool = False, x_base_value: str = "final", proxy: str = None) -> str|None:
    if method is None:
        method = get_extraction_method_for_site(url)

    logger.info(f"Fetching article content from URL: {url} using method: {method}")

    methods = {
        "newspaper3k": extract_with_newspaper,
        "trafilatura": extract_with_trafilatura,
        "readability": extract_with_readability,
        "playwright": extract_with_playwright,
        "jina": extract_with_jina
    }

    if method.lower() != "all":
        if method.lower() not in methods:
            logger.error(f"Unknown method: {method}. Available methods: {list(methods.keys())}")
            return None

        logger.info(f"Trying specific method: {method}")
        if method.lower() == "jina":
            return methods[method.lower()](url, use_x_base, x_base_value, proxy)
        else:
            return methods[method.lower()](url, proxy)

    for method_name, method_func in methods.items():
        logger.info(f"Trying method: {method_name}")
        if method_name == "jina":
            result = method_func(url, use_x_base, x_base_value, proxy)
        else:
            result = method_func(url, proxy)

        if result:
            return result

    logger.error("Failed to fetch article content from all methods.")
    return None
