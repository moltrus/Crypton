import sys
import os
import argparse
from pathlib import Path
from typing import Optional

parent_dir=str(Path(__file__).parent.parent)
sys.path.insert(0, parent_dir)

import chroma_db
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

def sync_articles_to_chroma(collection_name: str="rss_articles", batch_size: int=41)->dict:
    logger.info(f"Starting ChromaDB Sync - collection: {collection_name}, batch_size: {batch_size}")
    try:
        config=load_config()
        success=chroma_db.sync_articles_from_db_to_chroma(config, collection_name=collection_name, batch_size=batch_size)
        if success:
            collection_count=chroma_db.get_collection_count(collection_name)
            logger.info(f"ChromaDB Sync Complete - Total articles in collection: {collection_count}")
            return {"status": "success", "collection_count": collection_count}
        else:
            logger.error("ChromaDB Sync Failed")
            return {"status": "failed", "error": "Sync operation failed"}
    except Exception as e:
        logger.error(f"Failed to sync articles to ChromaDB: {e}")
        return {"status": "failed", "error": str(e)}

def query_articles_from_chroma(query_text: str, collection_name: str="rss_articles", n_results: int=5, api_key: Optional[str]=None)->dict:
    logger.info(f"Querying ChromaDB - query: '{query_text}', collection: {collection_name}, n_results: {n_results}")
    try:
        results=chroma_db.query_chroma(query_text, collection_name=collection_name, n_results=n_results, api_key=api_key)
        logger.info(f"Query returned {len(results.get('ids', [[]])[0])} results")
        return {"status": "success", "results": results}
    except Exception as e:
        logger.error(f"Failed to query ChromaDB: {e}")
        return {"status": "failed", "error": str(e)}

def get_collection_stats(collection_name: str="rss_articles", api_key: Optional[str]=None)->dict:
    logger.info(f"Fetching collection stats - collection: {collection_name}")
    try:
        count=chroma_db.get_collection_count(collection_name, api_key)
        all_articles=chroma_db.get_all_articles_from_chroma(collection_name, api_key=api_key)
        domains=set()
        if all_articles.get("metadatas"):
            for metadata_list in all_articles["metadatas"]:
                if isinstance(metadata_list, list):
                    for metadata in metadata_list:
                        if "domain" in metadata:
                            domains.add(metadata["domain"])
                else:
                    if "domain" in metadata_list:
                        domains.add(metadata_list["domain"])
        logger.info(f"Collection '{collection_name}' stats:")
        logger.info(f"  Total articles: {count}")
        logger.info(f"  Unique domains: {len(domains)}")
        if domains:
            for domain in sorted(domains):
                logger.info(f"    - {domain}")
        return {
            "status": "success",
            "collection_name": collection_name,
            "total_articles": count,
            "unique_domains": len(domains),
            "domains": sorted(list(domains))
        }
    except Exception as e:
        logger.error(f"Failed to get collection stats: {e}")
        return {"status": "failed", "error": str(e)}

def reset_collection(collection_name: str="rss_articles")->dict:
    logger.warning(f"Resetting collection - collection: {collection_name}")
    try:
        success=chroma_db.delete_collection(collection_name)
        if success:
            logger.info(f"Collection '{collection_name}' successfully deleted")
            return {"status": "success", "message": f"Collection '{collection_name}' deleted"}
        else:
            logger.error(f"Failed to delete collection '{collection_name}'")
            return {"status": "failed", "error": f"Failed to delete collection"}
    except Exception as e:
        logger.error(f"Failed to reset collection: {e}")
        return {"status": "failed", "error": str(e)}

def main()->None:
    parser=argparse.ArgumentParser(description="Batch ChromaDB Processor", prog="python batch_chroma_db_sync.py")
    parser.add_argument("--mode", choices=["sync", "query", "stats", "reset"], default="sync", help="Processing mode: 'sync' for sync articles (default), 'query' for searching, 'stats' for collection stats, 'reset' to delete collection")
    parser.add_argument("--collection", default="rss_articles", help="Collection name (default: rss_articles)")
    parser.add_argument("--batch-size", type=int, default=41, help="Batch size for processing (default: 41)")
    parser.add_argument("--query-text", default="", help="Query text for search mode")
    parser.add_argument("--n-results", type=int, default=5, help="Number of results for query (default: 5)")
    args=parser.parse_args()

    config=load_config()
    if not config.get("enabled", True):
        logger.info("RSS Feed Aggregator is globally disabled (enabled: false in config.yaml)")
        logger.info("To enable, set 'enabled: true' in config.yaml")
        return

    try:
        if args.mode=="sync":
            result=sync_articles_to_chroma(collection_name=args.collection, batch_size=args.batch_size)
            print(f"Result: {result}")
        elif args.mode=="query":
            if not args.query_text:
                print("Error: --query-text is required for query mode")
                return
            result=query_articles_from_chroma(args.query_text, collection_name=args.collection, n_results=args.n_results)
            print(f"Result: {result}")
        elif args.mode=="stats":
            result=get_collection_stats(collection_name=args.collection)
            print(f"Result: {result}")
        elif args.mode=="reset":
            result=reset_collection(collection_name=args.collection)
            print(f"Result: {result}")
    except Exception as e:
        logger.error(f"Batch processing failed: {e}", exc_info=True)

if __name__=="__main__":
    main()
