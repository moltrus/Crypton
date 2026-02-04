from playwright.sync_api import sync_playwright
from utils import get_logger
import time
from urllib.parse import urlparse

logger=get_logger(__name__)

def get_redirected_url(url: str) -> str | None:
    logger.info(f"Resolving URL: {url}")
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True)
            page=browser.new_page()
            page.goto(url, wait_until="domcontentloaded")
            final_url=page.url
            initial_domain=urlparse(url).netloc

            for attempt in range(1, 4):
                time.sleep(2 ** attempt)
                try:
                    page.reload(wait_until="domcontentloaded")
                    final_url=page.url
                    final_domain=urlparse(final_url).netloc
                    if final_domain != initial_domain:
                        browser.close()
                        logger.info(f"Successfully resolved URL from {initial_domain} to {final_domain}: {final_url}")
                        return final_url
                except Exception as e:
                    logger.debug(f"Error during reload attempt {attempt}: {type(e).__name__}: {e}")
                    break

            browser.close()
            final_domain=urlparse(final_url).netloc

            if final_domain == initial_domain:
                logger.error(f"Failed to resolve URL - domain remained {final_domain}. Returning None.")
                return None

            logger.info(f"Resolved URL: {final_url}")
            return final_url

    except Exception as e:
        logger.error(f"Error in Playwright: {type(e).__name__}: {e}")

    logger.error("Failed to resolve URL. Returning None.")
    return None
