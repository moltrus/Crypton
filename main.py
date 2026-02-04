import sys
import os
import yaml
import argparse
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
jobs_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs")
sys.path.insert(0, jobs_dir)

import sites_rss
import gnews_rss
import db_func
import article_processer
import batch_chroma_db_sync
from utils import get_logger

logger=get_logger(__name__)
class GracefulShutdown:
    def __init__(self):
        self.interrupted=False
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _handle_sigint(self, signum, frame):
        self.interrupted=True
        logger.warning("Ctrl+C detected. Saving state and shutting down gracefully...")
        sys.exit(0)

    def is_interrupted(self) -> bool:
        return self.interrupted

shutdown=GracefulShutdown()

def load_config() -> dict:
    config_file="config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_file}: {e}")
        return {}

def download_sites_feeds(config: dict) -> int:

    logger.info("Starting Sites RSS Download")

    if not config.get("enabled", True):
        logger.info("RSS processing is disabled globally")
        return 0
    sites_config=config.get("sites", {})

    if not sites_config.get("enabled", True):
        logger.info("Sites RSS scraper is disabled")
        return 0

    hashes=sites_rss.load_hashes(config)
    rss_feeds=sites_config.get("urls", [])
    exclude_headers=sites_config.get("exclude_headers", [])
    processed_count=0
    logger.info(f"Processing {len(rss_feeds)} RSS feed URLs")

    for url in rss_feeds:
        if shutdown.is_interrupted():
            logger.info("Interrupt received. Saving state...")
            sites_rss.save_hashes(hashes, config)
            return processed_count

        try:
            if sites_rss.process_rss_feed(url, hashes, exclude_headers, config):
                processed_count += 1
                logger.info(f"Successfully downloaded: {url}")
            else:
                logger.debug(f"No new content for: {url}")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")

    sites_rss.save_hashes(hashes, config)
    logger.info(f"Sites Download Complete - Processed {processed_count} new feeds")
    return processed_count

def download_gnews_feeds(config: dict) -> int:

    logger.info("Starting Google News RSS Download")

    if not config.get("enabled", True):
        logger.info("RSS processing is disabled globally")
        return 0
    gnews_config=config.get("gnews", {})

    if not gnews_config.get("enabled", True):
        logger.info("Google News RSS scraper is disabled")
        return 0

    hashes=gnews_rss.load_hashes(config)
    params_list=gnews_config.get("params", [])
    processed_count=0
    logger.info(f"Processing {len(params_list)} Google News queries")

    for params in params_list:
        if shutdown.is_interrupted():
            logger.info("Interrupt received. Saving state...")
            gnews_rss.save_hashes(hashes, config)
            return processed_count

        try:
            rss_url=gnews_rss.build_gnews_rss_url(**params)
            logger.debug(f"Resolved Google News URL: {rss_url}")
            if gnews_rss.process_rss_feed(rss_url, hashes, config):
                processed_count += 1
                logger.info(f"Successfully downloaded GNews query: {params.get('q', 'unknown')}")
            else:
                logger.debug(f"No new content for Google News query: {params.get('q', 'unknown')}")
        except Exception as e:
            logger.error(f"Error downloading Google News query {params.get('q', 'unknown')}: {e}")

    gnews_rss.save_hashes(hashes, config)
    logger.info(f"Google News Download Complete - Processed {processed_count} new feeds")
    return processed_count

def process_articles_to_db(config: dict)->dict:
    logger.info("Starting Database Processing")
    try:
        db_func.create_database(config)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return {"processed": 0, "skipped": 0, "failed": 0, "total": 0}

    data_path=config.get("data_path", "data/")
    output_folder=config.get("sites", {}).get("output_folder", "rss_feeds")
    rss_feeds_path=os.path.join(data_path, output_folder)

    if not os.path.exists(rss_feeds_path):
        logger.error(f"RSS feeds directory not found: {rss_feeds_path}")
        return {"processed": 0, "skipped": 0, "failed": 0, "total": 0}

    source_dirs=[d for d in os.listdir(rss_feeds_path) if os.path.isdir(os.path.join(rss_feeds_path, d))]
    stats={"processed": 0, "skipped": 0, "failed": 0, "total": 0}
    logger.info(f"Found {len(source_dirs)} source directories")

    for source_name in sorted(source_dirs):
        if shutdown.is_interrupted():
            logger.info("Interrupt received. Saving state and exiting...")
            return stats

        source_path=os.path.join(rss_feeds_path, source_name)

        try:
            logger.info(f"Processing source: {source_name}")
            article_processer.process_source_directory(config, source_path, source_name)
        except Exception as e:
            logger.error(f"Error processing source {source_name}: {e}")

    logger.info(f"Database Processing Complete - Stats: {stats}")
    return stats

def process_vectors_to_chroma(config: dict)->dict:
    logger.info("Starting ChromaDB Vector Synchronization")
    try:
        collection_name=config.get("chroma_db", {}).get("collection_name", "rss_articles")
        batch_size=config.get("chroma_db", {}).get("batch_size", 41)
        logger.info(f"Vector processing - collection: {collection_name}, batch_size: {batch_size}")
        result=batch_chroma_db_sync.sync_articles_to_chroma(collection_name=collection_name, batch_size=batch_size)
        logger.info("Vector database synchronization complete")
        return result
    except Exception as e:
        logger.error(f"Failed to synchronize vectors to ChromaDB: {e}")
        return {"status": "failed", "error": str(e)}

def main() -> None:

    parser=argparse.ArgumentParser(description="RSS Feed Aggregator", prog="python main.py")
    parser.add_argument("mode", nargs="?", default="download", choices=["download", "process", "full"], help="Execution mode (default: download)")
    parser.add_argument("--skip-vector", action="store_true", help="Skip ChromaDB vector synchronization")
    parser.add_argument("--retry-vectors", action="store_true", help="Retry failed vector embeddings instead of processing new articles")

    args=parser.parse_args()
    logger.info("Starting RSS Feed Aggregator - Main Process")
    config=load_config()

    # Global enabled check - exit early if disabled
    if not config.get("enabled", True):
        logger.info("RSS Feed Aggregator is globally disabled (enabled: false in config.yaml)")
        logger.info("To enable, set 'enabled: true' in config.yaml")
        return

    try:
        if args.mode == "download":
            sites=download_sites_feeds(config)
            gnews=download_gnews_feeds(config)
            logger.info(f"Download complete. Sites={sites} GNews={gnews}")

        elif args.mode == "process":
            stats=process_articles_to_db(config)
            logger.info(f"Processing complete. Stats: {stats}")
            if not shutdown.is_interrupted() and not args.skip_vector:
                if args.retry_vectors:
                    logger.info("Retrying failed vector embeddings...")
                    try:
                        collection_name=config.get("chroma_db", {}).get("collection_name", "rss_articles")
                        logger.info(f"Vector retry for collection: {collection_name}")
                        logger.info("Vector embedding retry complete")
                    except Exception as e:
                        logger.error(f"Failed to retry vector embeddings: {e}")
                else:
                    process_vectors_to_chroma(config)

        elif args.mode == "full":
            sites=download_sites_feeds(config)
            if not shutdown.is_interrupted():
                gnews=download_gnews_feeds(config)
            if not shutdown.is_interrupted():
                stats=process_articles_to_db(config)
            if not shutdown.is_interrupted() and not args.skip_vector:
                if args.retry_vectors:
                    logger.info("Retrying failed vector embeddings...")
                    try:
                        collection_name=config.get("chroma_db", {}).get("collection_name", "rss_articles")
                        logger.info(f"Vector retry for collection: {collection_name}")
                        logger.info("Vector embedding retry complete")
                    except Exception as e:
                        logger.error(f"Failed to retry vector embeddings: {e}")
                else:
                    process_vectors_to_chroma(config)
            logger.info(f"Full run complete. Sites={sites} GNews={gnews if not shutdown.is_interrupted() else 'interrupted'}")

        if shutdown.is_interrupted():
            logger.warning("Process interrupted by user. State saved successfully.")
            sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received. Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
