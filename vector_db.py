import os
import yaml
from typing import List, Dict, Any, Optional, Tuple
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv
from utils import get_logger
from django_config import AppSettings, RSSFeedArticleModel, ArticleMetadataModel, ensure_schema, FailedVectorEmbeddingsModel, VectorDatabaseTrackingModel
from django.utils import timezone

load_dotenv()
logger=get_logger(__name__)

def is_article_synced_to_vector_db(config: dict, article_uuid: str, namespace: str="rss-feeds")->bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    tracking_model=VectorDatabaseTrackingModel.get_model()
    try:
        record=tracking_model.objects.filter(article_uuid=article_uuid, namespace=namespace, status="synced").first()
        if record:
            logger.debug(f"Article {article_uuid} already synced to vector DB in namespace {namespace}")
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to check if article is synced: {e}")
        return False

def mark_article_as_synced(config: dict, article_uuid: str, url: str, title: str, domain: str, namespace: str="rss-feeds", vector_id: str="", total_chunks: int=1)->bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    tracking_model=VectorDatabaseTrackingModel.get_model()
    try:
        existing=tracking_model.objects.filter(article_uuid=article_uuid, namespace=namespace).first()
        if existing:
            existing.status="synced"
            existing.vector_id=vector_id
            existing.total_chunks=total_chunks
            existing.synced_chunks=total_chunks
            existing.synced_at=timezone.now()
            existing.error_message=""
            existing.save()
            logger.debug(f"Updated tracking record for article {article_uuid}: status=synced, chunks={total_chunks}")
        else:
            tracking_model.objects.create(
                article_uuid=article_uuid,
                url=url,
                title=title,
                domain=domain,
                namespace=namespace,
                vector_id=vector_id,
                status="synced",
                total_chunks=total_chunks,
                synced_chunks=total_chunks,
                synced_at=timezone.now(),
            )
            logger.info(f"Created tracking record for article {article_uuid}: status=synced, namespace={namespace}, chunks={total_chunks}")
        return True
    except Exception as e:
        logger.error(f"Failed to mark article as synced: {e}")
        return False

def mark_article_as_failed(config: dict, article_uuid: str, url: str, title: str, domain: str, error_message: str, namespace: str="rss-feeds")->bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    tracking_model=VectorDatabaseTrackingModel.get_model()
    try:
        existing=tracking_model.objects.filter(article_uuid=article_uuid, namespace=namespace).first()
        if existing:
            existing.status="failed"
            existing.error_message=error_message
            existing.save()
            logger.debug(f"Updated tracking record for article {article_uuid}: status=failed")
        else:
            tracking_model.objects.create(
                article_uuid=article_uuid,
                url=url,
                title=title,
                domain=domain,
                namespace=namespace,
                status="failed",
                error_message=error_message,
            )
            logger.info(f"Created tracking record for article {article_uuid}: status=failed")
        return True
    except Exception as e:
        logger.error(f"Failed to mark article as failed: {e}")
        return False

def get_pending_articles_for_vector_db(config: dict, namespace: str="rss-feeds")->List[Dict[str, Any]]:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    tracking_model=VectorDatabaseTrackingModel.get_model()
    pending_records=[]
    try:
        records=tracking_model.objects.filter(namespace=namespace, status="pending")
        for record in records:
            pending_records.append({
                "article_uuid": record.article_uuid,
                "url": record.url,
                "title": record.title,
                "domain": record.domain,
            })
        logger.debug(f"Retrieved {len(pending_records)} pending articles for namespace {namespace}")
        return pending_records
    except Exception as e:
        logger.error(f"Failed to get pending articles: {e}")
        return []

def log_failed_embedding(config: dict, article_uuid: str, url: str, title: str, domain: str, error_type: str, error_message: str, chunk_index: int=0, total_chunks: int=1) -> bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    failed_embeddings_model=FailedVectorEmbeddingsModel.get_model()
    try:
        existing=failed_embeddings_model.objects.filter(article_uuid=article_uuid, chunk_index=chunk_index).first()
        if existing:
            existing.attempt_count+=1
            existing.error_message=error_message
            existing.error_type=error_type
            existing.save()
            logger.debug(f"Updated failed embedding record for UUID: {article_uuid}, chunk: {chunk_index}")
        else:
            failed_embeddings_model.objects.create(
                article_uuid=article_uuid,
                url=url,
                title=title,
                domain=domain,
                error_type=error_type,
                error_message=error_message,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                attempt_count=1,
            )
            logger.info(f"Created failed embedding record for UUID: {article_uuid}, chunk: {chunk_index}, error: {error_type}")
        return True
    except Exception as exc:
        logger.error(f"Failed to log failed embedding: {exc}")
        return False

def get_failed_embeddings(config: dict) -> List[Dict[str, Any]]:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    failed_embeddings_model=FailedVectorEmbeddingsModel.get_model()
    failed_records=[]
    try:
        records=failed_embeddings_model.objects.all().order_by('-attempt_count', 'created_at')
        for record in records:
            failed_records.append({
                "article_uuid": record.article_uuid,
                "url": record.url,
                "title": record.title,
                "domain": record.domain,
                "error_type": record.error_type,
                "error_message": record.error_message,
                "chunk_index": record.chunk_index,
                "total_chunks": record.total_chunks,
                "attempt_count": record.attempt_count,
                "last_attempted_at": record.last_attempted_at.isoformat() if record.last_attempted_at else None,
                "created_at": record.created_at.isoformat() if record.created_at else None,
            })
        logger.info(f"Retrieved {len(failed_records)} failed embedding records from database")
        return failed_records
    except Exception as e:
        logger.error(f"Failed to retrieve failed embeddings: {e}")
        return []

def clear_failed_embedding(config: dict, article_uuid: str, chunk_index: int=0) -> bool:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    failed_embeddings_model=FailedVectorEmbeddingsModel.get_model()
    try:
        failed_embeddings_model.objects.filter(article_uuid=article_uuid, chunk_index=chunk_index).delete()
        logger.info(f"Cleared failed embedding record for UUID: {article_uuid}, chunk: {chunk_index}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear failed embedding: {e}")
        return False

def load_config() -> dict:
    config_file="config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_file}: {e}")
        return {}

def get_pinecone_client(config: dict) -> Pinecone:
    api_key=os.environ.get("PINECONE_API_KEY")
    if not api_key:
        raise ValueError("PINECONE_API_KEY environment variable not set. Add it to your .env file.")
    return Pinecone(api_key=api_key)

def create_index_if_not_exists(pc: Pinecone, config: dict) -> str:
    pinecone_config=config.get("pinecone", {})
    index_name=pinecone_config.get("index_name", "rss-articles")
    existing_indexes=[idx.name for idx in pc.list_indexes()]
    if index_name not in existing_indexes:
        dimension=pinecone_config.get("dimension", 1024)
        metric=pinecone_config.get("metric", "cosine")
        cloud=pinecone_config.get("cloud", "aws")
        region=pinecone_config.get("region", "us-east-1")
        try:
            pc.create_index(
                name=index_name,
                dimension=dimension,
                metric=metric,
                spec=ServerlessSpec(cloud=cloud, region=region)
            )
            logger.info(f"Created Pinecone index: {index_name}")
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            raise
    else:
        logger.info(f"Pinecone index already exists: {index_name}")
    return index_name

def get_pinecone_index(config: dict):
    try:
        pc=get_pinecone_client(config)
        index_name=create_index_if_not_exists(pc, config)
        return pc.Index(index_name)
    except Exception as e:
        logger.error(f"Failed to get Pinecone index: {e}")
        raise

def embed_texts_batch(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []
    try:
        from mistralai import Mistral
        api_key=os.environ.get("MISTRAL_API_KEY")
        if not api_key:
            raise ValueError("MISTRAL_API_KEY environment variable not set")
        client=Mistral(api_key=api_key)
        response=client.embeddings.create(model="mistral-embed", inputs=texts)
        embeddings=[e.embedding for e in response.data]
        return embeddings
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        raise

def embed_single_text(text: str) -> List[float]:
    embeddings=embed_texts_batch([text])
    return embeddings[0] if embeddings else []

def chunk_text_by_words(text: str, max_words_per_chunk: int) -> List[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), max_words_per_chunk):
        chunk_words = words[i:i + max_words_per_chunk]
        chunk_text = ' '.join(chunk_words)
        chunks.append(chunk_text)
    return chunks

def prepare_article_for_embedding(article: Dict[str, Any], config: dict) -> List[Tuple[str, Dict[str, Any]]]:
    text_to_embed = f"{article['title']} {article['description']} {article['content']}"
    pinecone_config = config.get("pinecone", {})
    max_words_per_chunk = pinecone_config.get("max_words_per_chunk", 5500)
    words = text_to_embed.split()
    word_count = len(words)
    logger.debug(f"Preparing article {article['uuid']} for embedding: {word_count} words, max chunk size: {max_words_per_chunk}")
    if word_count <= max_words_per_chunk:
        # Single chunk
        metadata = {
            "uuid": article["uuid"],
            "url": article["url"],
            "title": article["title"][:500] if article["title"] else "",
            "pub_date": article["pub_date"],
            "description": article["description"][:1000] if article["description"] else "",
            "content": article["content"][:2000] if article["content"] else "",
            "category": article["category"][:300] if article.get("category") else "",
            "language": article.get("language", "en"),
            "source_url": article.get("source_url", ""),
            "domain": article.get("domain", ""),
            "fetched_at": article.get("fetched_at"),
            "word_count": article.get("word_count", 0),
            "creator": article.get("creator", ""),
            "chunk_index": 0,
            "total_chunks": 1,
        }
        logger.debug(f"Article {article['uuid']} will be processed as single chunk")
        return [(text_to_embed[:8000], metadata)]
    else:
        # Multiple chunks
        chunks = chunk_text_by_words(text_to_embed, max_words_per_chunk)
        chunk_data = []
        for idx, chunk in enumerate(chunks):
            metadata = {
                "uuid": article["uuid"],
                "url": article["url"],
                "title": article["title"][:500] if article["title"] else "",
                "pub_date": article["pub_date"],
                "description": article["description"][:1000] if article["description"] else "",
                "content": article["content"][:2000] if article["content"] else "",
                "category": article["category"][:300] if article.get("category") else "",
                "language": article.get("language", "en"),
                "source_url": article.get("source_url", ""),
                "domain": article.get("domain", ""),
                "fetched_at": article.get("fetched_at"),
                "word_count": article.get("word_count", 0),
                "creator": article.get("creator", ""),
                "chunk_index": idx,
                "total_chunks": len(chunks),
            }
            chunk_data.append((chunk[:8000], metadata))
        logger.info(f"Article {article['uuid']} split into {len(chunks)} chunks ({word_count} words total)")
        return chunk_data

def fetch_articles_from_db(config: dict, limit: Optional[int]=None) -> List[Dict[str, Any]]:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    rss_model=RSSFeedArticleModel.get_model()
    metadata_model=ArticleMetadataModel.get_model()

    # Perform JOIN query equivalent to: SELECT article_metadata.uuid, article_metadata.url, article_metadata.title, 
    # article_metadata.pub_date, article_metadata.description, article_metadata.content, article_metadata.category, 
    # article_metadata.language, rss_feed_articles.source_url, rss_feed_articles.domain, rss_feed_articles.fetched_at 
    # FROM rss_feed_articles, article_metadata WHERE rss_feed_articles.uuid = article_metadata.uuid

    query = metadata_model.objects.select_related('uuid').all()
    if limit:
        query = query[:limit]
    articles = []
    for metadata in query:
        try:
            # Get the related RSS article
            rss_article = metadata.uuid
            article_data = {
                "uuid": str(metadata.uuid.uuid),  # Convert UUID object to string
                "url": metadata.url or rss_article.url,  # Use metadata url if available, otherwise rss url
                "title": metadata.title or "",
                "pub_date": metadata.pub_date.isoformat() if metadata.pub_date else None,
                "description": metadata.description or "",
                "content": metadata.content or "",
                "category": metadata.category or "",
                "language": metadata.language or "en",
                "source_url": rss_article.source_url or "",
                "domain": rss_article.domain or "",
                "fetched_at": rss_article.fetched_at.isoformat() if rss_article.fetched_at else None,
                "word_count": metadata.word_count or 0,
                "creator": metadata.creator or "",
            }
            logger.info(f"Fetched article from DB: UUID={article_data['uuid']}, Title='{article_data['title'][:50]}...', Domain={article_data['domain']}, Word Count={article_data['word_count']}")
            logger.debug(f"Full article data: {article_data}")
            articles.append(article_data)
        except Exception as e:
            logger.error(f"Error processing article metadata: {e}")
    logger.info(f"Fetched {len(articles)} articles from database using JOIN query")
    return articles

def prepare_vectors_batch(articles: List[Dict[str, Any]], config: dict, skip_short: bool=True) -> List[Dict[str, Any]]:
    vectors = []
    texts_to_embed = []
    chunk_info = []  # Store (article_idx, chunk_idx, total_chunks)
    for idx, article in enumerate(articles):
        try:
            chunk_data = prepare_article_for_embedding(article, config)
            logger.info(f"Processing article: UUID={article['uuid']}, Title='{article['title'][:50]}...', Domain={article['domain']}, Word Count={article.get('word_count', 0)}")
            for chunk_idx, (chunk_text, metadata) in enumerate(chunk_data):
                if skip_short and len(chunk_text.strip()) < 10:
                    logger.warning(f"Skipping chunk {chunk_idx} for article {article['uuid']}: insufficient content")
                    continue
                logger.debug(f"Chunk {chunk_idx + 1}/{len(chunk_data)}: Text length={len(chunk_text)}, Metadata={metadata}")
                logger.debug(f"Text to embed (first 200 chars): '{chunk_text[:200]}...'")
                texts_to_embed.append(chunk_text)
                chunk_info.append((idx, chunk_idx, len(chunk_data)))  # (article_index, chunk_index, total_chunks)
        except Exception as e:
            logger.error(f"Error preparing article {article.get('uuid', 'unknown')}: {e}")
    if not texts_to_embed:
        logger.warning("No valid texts to embed")
        return []
    try:
        embeddings = embed_texts_batch(texts_to_embed)
        for embedding, (article_idx, chunk_idx, total_chunks) in zip(embeddings, chunk_info):
            article = articles[article_idx]
            chunk_data = prepare_article_for_embedding(article, config)
            _, metadata = chunk_data[chunk_idx]
            vector_id = f"{article['uuid']}_chunk_{chunk_idx}"
            logger.info(f"Prepared vector: ID={vector_id}, Embedding dimension={len(embedding)}")
            logger.debug(f"Vector metadata: {metadata}")
            vectors.append({
                "id": vector_id,
                "values": embedding,
                "metadata": metadata
            })
    except Exception as e:
        logger.error(f"Failed to generate embeddings for batch: {e}")
        # Log failed embeddings for retry
        for idx, (article_idx, chunk_idx, total_chunks) in enumerate(chunk_info):
            if idx < len(texts_to_embed):
                article=articles[article_idx]
                log_failed_embedding(
                    config,
                    article_uuid=article["uuid"],
                    url=article.get("url", ""),
                    title=article.get("title", "")[:500],
                    domain=article.get("domain", ""),
                    error_type="EmbeddingGenerationError",
                    error_message=str(e),
                    chunk_index=chunk_idx,
                    total_chunks=total_chunks
                )
    return vectors

def upsert_vectors_batch(index, vectors: List[Dict[str, Any]], namespace: str="default") -> bool:
    if not vectors:
        logger.warning("No vectors to upsert")
        return False
    logger.info(f"Upserting {len(vectors)} vectors to namespace '{namespace}'")
    for vector in vectors:
        logger.info(f"Vector ID: {vector['id']}, Metadata: uuid={vector['metadata'].get('uuid')}, title='{vector['metadata'].get('title', '')[:30]}...', domain={vector['metadata'].get('domain')}, chunk={vector['metadata'].get('chunk_index', 0)}/{vector['metadata'].get('total_chunks', 1)}")
    try:
        index.upsert(vectors=vectors, namespace=namespace)
        logger.info(f"Successfully upserted {len(vectors)} vectors to namespace '{namespace}'")
        return True
    except Exception as e:
        logger.error(f"Failed to upsert vectors to namespace '{namespace}': {e}")
        return False

def upsert_articles_to_pinecone(config: dict, articles: List[Dict[str, Any]], batch_size: int=10, namespace: str="rss-feeds") -> Dict[str, int]:
    stats={"total": len(articles), "upserted": 0, "failed": 0, "skipped": 0, "already_synced": 0}
    if not articles:
        logger.info("No articles to upsert")
        return stats
    logger.info(f"Starting upsert of {len(articles)} articles to Pinecone")
    try:
        index=get_pinecone_index(config)
    except Exception as e:
        logger.error(f"Failed to initialize Pinecone index: {e}")
        stats["failed"]=len(articles)
        return stats

    articles_to_process=[]
    for article in articles:
        article_uuid=article.get("uuid")
        if is_article_synced_to_vector_db(config, article_uuid, namespace):
            logger.info(f"Article {article_uuid} already synced to vector DB, skipping")
            stats["already_synced"]+=1
        else:
            articles_to_process.append(article)

    if not articles_to_process:
        logger.info(f"All {len(articles)} articles already synced to vector DB")
        return stats

    logger.info(f"Processing {len(articles_to_process)} articles for namespace: {namespace} (skipped {stats['already_synced']} already synced)")
    for i in range(0, len(articles_to_process), batch_size):
        batch=articles_to_process[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} articles")
        vectors=prepare_vectors_batch(batch, config)
        if vectors:
            success=upsert_vectors_batch(index, vectors, namespace)
            if success:
                for article in batch:
                    article_uuid=article.get("uuid")
                    url=article.get("url", "")
                    title=article.get("title", "")[:500]
                    domain=article.get("domain", "")
                    mark_article_as_synced(config, article_uuid, url, title, domain, namespace)
                stats["upserted"]+=len(vectors)
                logger.info(f"Successfully processed batch: {len(vectors)} vectors upserted and tracked")
            else:
                stats["failed"]+=len(batch)
                logger.error(f"Failed to upsert batch: {len(batch)} articles failed")
                for article in batch:
                    article_uuid=article.get("uuid")
                    url=article.get("url", "")
                    title=article.get("title", "")[:500]
                    domain=article.get("domain", "")
                    mark_article_as_failed(config, article_uuid, url, title, domain, "Failed to upsert vectors to Pinecone", namespace)
                    log_failed_embedding(
                        config,
                        article_uuid=article_uuid,
                        url=url,
                        title=title,
                        domain=domain,
                        error_type="VectorUpsertError",
                        error_message="Failed to upsert vectors to Pinecone"
                    )
        else:
            stats["skipped"]+=len(batch)
            logger.warning(f"Skipped batch: {len(batch)} articles had no valid vectors")
    logger.info(f"Upsert complete. Final stats: {stats}")
    return stats

def query_similar_articles(config: dict, query_text: str, namespace: Optional[str]=None, top_k: int=5, include_metadata: bool=True) -> Dict[str, Any]:
    try:
        index=get_pinecone_index(config)
        query_embedding=embed_single_text(query_text)
        if not query_embedding:
            logger.error("Failed to generate query embedding")
            return {"error": "Failed to generate query embedding", "query": query_text}
        results=index.query(
            namespace=namespace or "rss-feeds",
            vector=query_embedding,
            top_k=top_k,
            include_values=False,
            include_metadata=include_metadata
        )
        return {
            "query": query_text,
            "namespace": namespace or "rss-feeds",
            "results": results.to_dict() if hasattr(results, 'to_dict') else results,
            "count": len(results.matches) if hasattr(results, 'matches') else 0
        }
    except Exception as e:
        logger.error(f"Failed to query similar articles: {e}")
        return {"error": str(e), "query": query_text}

def delete_article_from_pinecone(config: dict, article_uuid: str, namespace: str="rss-feeds") -> bool:
    try:
        index=get_pinecone_index(config)
        index.delete(ids=[article_uuid], namespace=namespace)
        logger.info(f"Deleted article {article_uuid} from namespace '{namespace}'")
        return True
    except Exception as e:
        logger.error(f"Failed to delete article {article_uuid}: {e}")
        return False

def delete_namespace(config: dict, namespace: str) -> bool:
    try:
        index=get_pinecone_index(config)
        index.delete(delete_all=True, namespace=namespace)
        logger.info(f"Deleted all vectors from namespace: {namespace}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete namespace {namespace}: {e}")
        return False

def process_single_article_to_pinecone(config: dict, article: Dict[str, Any], namespace: str="rss-feeds") -> bool:
    try:
        vectors=prepare_vectors_batch([article], config)
        if vectors:
            index=get_pinecone_index(config)
            return upsert_vectors_batch(index, vectors, namespace)
        logger.warning(f"No vectors generated for article {article.get('uuid', 'unknown')}")
        return False
    except Exception as e:
        logger.error(f"Failed to process article to Pinecone: {e}")
        return False

def sync_database_to_pinecone(config: dict, batch_size: int=10, limit: Optional[int]=None, namespace: str="rss-feeds") -> Dict[str, int]:
    try:
        logger.info("Starting database to Pinecone synchronization")
        articles=fetch_articles_from_db(config, limit)
        if not articles:
            logger.warning("No articles found in database")
            return {"total": 0, "upserted": 0, "failed": 0, "skipped": 0}
        stats=upsert_articles_to_pinecone(config, articles, batch_size, namespace)
        logger.info(f"Synchronization complete. Stats: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Error in sync_database_to_pinecone: {e}")
        return {"error": str(e)}
