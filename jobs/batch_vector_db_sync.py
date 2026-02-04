import sys
import os
import argparse
from pathlib import Path

parent_dir=str(Path(__file__).parent.parent)
sys.path.insert(0, parent_dir)

from vector_db import sync_database_to_pinecone, get_failed_embeddings, clear_failed_embedding, log_failed_embedding, fetch_articles_from_db, process_single_article_to_pinecone
from django_config import ensure_schema, AppSettings
from utils import get_logger
import yaml

logger=get_logger(__name__)

def load_config()->dict:
    config_file=os.path.join(parent_dir, "config.yaml")
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_file}: {e}")
        return {}

def process_new_articles(namespace: str="rss-feeds", batch_size: int=10, limit: int=None)->dict:
    logger.info(f"Processing new articles to Pinecone - namespace: {namespace}, batch_size: {batch_size}, limit: {limit}")
    try:
        config=load_config()
        stats=sync_database_to_pinecone(config, batch_size=batch_size, limit=limit, namespace=namespace)
        logger.info(f"New articles processing complete. Stats: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Failed to process new articles: {e}")
        return {"status": "failed", "error": str(e)}

def retry_failed_embeddings(namespace: str="rss-feeds", batch_size: int=10)->dict:
    logger.info(f"Retrying failed embeddings - namespace: {namespace}, batch_size: {batch_size}")
    try:
        config=load_config()
        AppSettings.configure(config)
        AppSettings.setup()
        ensure_schema()
        failed_records=get_failed_embeddings(config)
        if not failed_records:
            logger.info("No failed embeddings to retry")
            return {"status": "success", "retried": 0, "succeeded": 0}
        logger.info(f"Found {len(failed_records)} failed embedding records")
        unique_uuids=list(set([record.get("article_uuid") for record in failed_records]))
        logger.info(f"Fetching {len(unique_uuids)} unique articles from database for retry")
        articles_by_uuid={}
        try:
            all_articles=fetch_articles_from_db(config)
            for article in all_articles:
                articles_by_uuid[article.get("uuid")]=article
        except Exception as fetch_err:
            logger.error(f"Failed to fetch articles from database: {fetch_err}")
            return {"status": "failed", "error": f"Failed to fetch articles: {fetch_err}"}
        retried_count=0
        succeeded_count=0
        for record in failed_records:
            try:
                article_uuid=record.get("article_uuid")
                url=record.get("url")
                title=record.get("title")
                domain=record.get("domain")
                chunk_index=record.get("chunk_index", 0)
                attempt_count=record.get("attempt_count", 0)
                logger.info(f"Retrying article {article_uuid} (attempt #{attempt_count+1})")
                if article_uuid not in articles_by_uuid:
                    logger.warning(f"Article {article_uuid} not found in database, skipping")
                    retried_count+=1
                    continue
                article=articles_by_uuid[article_uuid]
                result=process_single_article_to_pinecone(config, article, namespace=namespace)
                if result:
                    try:
                        clear_failed_embedding(config, article_uuid, chunk_index)
                        succeeded_count+=1
                        logger.info(f"Successfully retried and cleared failed record for {article_uuid}")
                    except Exception as clear_err:
                        logger.error(f"Failed to clear failed record for {article_uuid}: {clear_err}")
                else:
                    logger.warning(f"Failed to process article {article_uuid} on retry")
                retried_count+=1
                if retried_count % batch_size == 0:
                    logger.info(f"Batch processed: {retried_count}/{len(failed_records)} retried, {succeeded_count} succeeded")
            except Exception as retry_err:
                logger.error(f"Error retrying article {record.get('article_uuid')}: {retry_err}")
                try:
                    log_failed_embedding(config, article_uuid, url, title, domain, "retry_error", str(retry_err), record.get("chunk_index", 0), record.get("total_chunks", 1))
                except Exception as log_err:
                    logger.error(f"Failed to log retry error: {log_err}")
        logger.info(f"Failed embeddings retry complete. Retried: {retried_count}, Succeeded: {succeeded_count}")
        return {"status": "success", "retried": retried_count, "succeeded": succeeded_count}
    except Exception as e:
        logger.error(f"Failed to retry failed embeddings: {e}")
        return {"status": "failed", "error": str(e)}

def main()->None:
    parser=argparse.ArgumentParser(description="Batch Vector Database Processor", prog="python batch_vector_db.py")
    parser.add_argument("--mode", choices=["process", "retry"], default="process", help="Processing mode: 'process' for new articles (default), 'retry' for failed embeddings")
    parser.add_argument("--namespace", default="rss-feeds", help="Pinecone namespace (default: rss-feeds)")
    parser.add_argument("--batch-size", type=int, default=5, help="Batch size for processing (default: 5)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum articles to process (default: None, process all)")
    args=parser.parse_args()

    config=load_config()
    if not config.get("enabled", True):
        logger.info("RSS Feed Aggregator is globally disabled (enabled: false in config.yaml)")
        logger.info("To enable, set 'enabled: true' in config.yaml")
        return

    try:
        if args.mode=="process":
            result=process_new_articles(namespace=args.namespace, batch_size=args.batch_size, limit=args.limit)
            print(f"Result: {result}")
        elif args.mode=="retry":
            result=retry_failed_embeddings(namespace=args.namespace, batch_size=args.batch_size)
            print(f"Result: {result}")
    except Exception as e:
        logger.error(f"Batch processing failed: {e}", exc_info=True)

if __name__=="__main__":
    main()
