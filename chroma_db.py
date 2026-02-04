import os
import yaml
import chromadb
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from utils import get_logger
from django_config import AppSettings, RSSFeedArticleModel, ArticleMetadataModel, ensure_schema
from embedding_funcs import JinaCustomEmbeddingFunction

load_dotenv()
logger=get_logger(__name__)

with open("config.yaml", "r") as f:
    config=yaml.safe_load(f)

client=chromadb.PersistentClient(path=config["chroma_db"]["persist_directory"])


def get_or_create_collection(collection_name: str="rss_articles", api_key: Optional[str]=None, embedding_type: str="mistral")->Any:
    try:
        embedding_function=JinaCustomEmbeddingFunction(
            model="jina-embeddings-v3",
            task="retrieval.passage",
            api_key=api_key,
            delay=config["chroma_db"].get("embedding_delay", 0.1)
        )
        collection=client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_function,
            metadata={"hnsw:space": "cosine"}
        )
        logger.debug(f"Retrieved or created collection: {collection_name}")
        return collection
    except Exception as e:
        logger.error(f"Failed to get or create collection {collection_name}: {e}")
        raise


def add_articles_to_chroma(
    articles: List[Dict[str, Any]],
    collection_name: str="rss_articles",
    batch_size: int=41,
    api_key: Optional[str]=None
)->bool:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        total_articles=len(articles)
        logger.debug(f"Starting to add {total_articles} articles to ChromaDB")

        for i in range(0, total_articles, batch_size):
            batch=articles[i:i+batch_size]
            documents=[article.get("content", "") for article in batch]
            ids=[article.get("id", "") for article in batch]
            metadatas=[{
                "title": article.get("title", ""),
                "url": article.get("url", ""),
                "domain": article.get("domain", ""),
                "published_date": article.get("published_date", ""),
                "article_uuid": article.get("article_uuid", "")
            } for article in batch]

            collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            logger.debug(f"    - Sub-batch: {len(batch)} articles embedded and stored")
            for idx, article in enumerate(batch):
                logger.debug(f"      [{idx+1}] {article.get('title', 'No Title')[:60]}...")

        logger.debug(f"Successfully added {total_articles} articles to ChromaDB")
        return True
    except Exception as e:
        logger.error(f"Failed to add articles to ChromaDB: {e}")
        return False


def query_chroma(
    query_text: str,
    collection_name: str="rss_articles",
    n_results: int=5,
    api_key: Optional[str]=None
)->Dict[str, Any]:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        results=collection.query(
            query_texts=[query_text],
            n_results=n_results
        )
        logger.debug(f"Query executed: '{query_text}', returned {len(results['ids'][0]) if results['ids'] else 0} results")
        return results
    except Exception as e:
        logger.error(f"Failed to query ChromaDB: {e}")
        raise


def update_articles_in_chroma(
    articles: List[Dict[str, Any]],
    collection_name: str="rss_articles",
    api_key: Optional[str]=None
)->bool:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        documents=[article.get("content", "") for article in articles]
        ids=[article.get("id", "") for article in articles]
        metadatas=[{
            "title": article.get("title", ""),
            "url": article.get("url", ""),
            "domain": article.get("domain", ""),
            "published_date": article.get("published_date", ""),
            "article_uuid": article.get("article_uuid", "")
        } for article in articles]

        collection.update(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
        logger.info(f"Updated {len(articles)} articles in ChromaDB")
        return True
    except Exception as e:
        logger.error(f"Failed to update articles in ChromaDB: {e}")
        return False


def delete_articles_from_chroma(
    ids: List[str],
    collection_name: str="rss_articles",
    api_key: Optional[str]=None
)->bool:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        collection.delete(ids=ids)
        logger.info(f"Deleted {len(ids)} articles from ChromaDB")
        return True
    except Exception as e:
        logger.error(f"Failed to delete articles from ChromaDB: {e}")
        return False


def get_collection_count(collection_name: str="rss_articles", api_key: Optional[str]=None)->int:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        count=collection.count()
        logger.debug(f"Collection '{collection_name}' has {count} articles")
        return count
    except Exception as e:
        logger.error(f"Failed to get collection count: {e}")
        return 0


def get_all_articles_from_chroma(
    collection_name: str="rss_articles",
    limit: int=None,
    api_key: Optional[str]=None
)->Dict[str, Any]:
    try:
        collection=get_or_create_collection(collection_name, api_key)
        results=collection.get(limit=limit)
        logger.debug(f"Retrieved articles from collection '{collection_name}'")
        return results
    except Exception as e:
        logger.error(f"Failed to get articles from ChromaDB: {e}")
        raise


def delete_collection(collection_name: str="rss_articles")->bool:
    try:
        client.delete_collection(name=collection_name)
        logger.info(f"Deleted collection: {collection_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete collection {collection_name}: {e}")
        return False


def list_all_collections()->List[str]:
    try:
        collections=client.list_collections()
        collection_names=[col.name for col in collections]
        logger.debug(f"Available collections: {collection_names}")
        return collection_names
    except Exception as e:
        logger.error(f"Failed to list collections: {e}")
        return []


def reset_chroma_db()->bool:
    try:
        client.reset()
        logger.info("ChromaDB has been reset")
        return True
    except Exception as e:
        logger.error(f"Failed to reset ChromaDB: {e}")
        return False


def sync_articles_from_db_to_chroma(config: dict, collection_name: str="rss_articles", batch_size: int=41, api_key: Optional[str]=None)->bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    try:
        rss_model=RSSFeedArticleModel.get_model()
        metadata_model=ArticleMetadataModel.get_model()
        articles=rss_model.objects.all()
        total_articles=articles.count()
        logger.info(f"Found {total_articles} articles in database to sync to ChromaDB")
        if total_articles==0:
            logger.info("No articles to sync")
            return True
        total_synced=0
        batch_num=0
        for i in range(0, total_articles, batch_size):
            batch_num+=1
            batch=articles[i:i+batch_size]
            articles_to_add=[]
            batch_uuids=[]
            for article in batch:
                metadata=metadata_model.objects.filter(uuid=article.uuid).first()
                if metadata and metadata.content:
                    articles_to_add.append({
                        "id": article.uuid,
                        "content": metadata.content,
                        "title": metadata.title,
                        "url": article.url,
                        "domain": article.domain,
                        "published_date": str(metadata.pub_date) if metadata.pub_date else "",
                        "article_uuid": article.uuid
                    })
                    batch_uuids.append(article.uuid)
            if articles_to_add:
                logger.info(f"Processing batch {batch_num}: {len(articles_to_add)} articles")
                try:
                    add_articles_to_chroma(articles_to_add, collection_name, batch_size, api_key)
                    total_synced+=len(articles_to_add)
                    logger.info(f"  Batch {batch_num} complete: {len(articles_to_add)} articles synced (Total: {total_synced}/{total_articles})")
                    for uuid in batch_uuids:
                        logger.debug(f"    - Article: {uuid}")
                except Exception as batch_err:
                    logger.error(f"  Batch {batch_num} failed: {batch_err}")
                    return False
            else:
                logger.warning(f"Batch {batch_num}: No articles with content found")
        logger.info(f"=== Sync Complete ===")
        logger.info(f"Total articles synced: {total_synced}/{total_articles}")
        return True
    except Exception as e:
        logger.error(f"Failed to sync articles from database: {e}")
        return False


