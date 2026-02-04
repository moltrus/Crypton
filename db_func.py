from typing import Dict, Optional
from utils import get_logger
from django_config import AppSettings, RSSFeedArticleModel, FailedArticlesModel, ensure_schema

logger=get_logger(__name__)

def create_database(config: dict)->None:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    logger.info("Database ready at: %s", AppSettings.get_current_db_path())

def get_existing_urls(config: dict, source_name: Optional[str]=None)->set:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    model=RSSFeedArticleModel.get_model()
    queryset=model.objects.all()
    urls=set(queryset.values_list("url", flat=True))
    logger.info(
        "Found %d existing URLs in database",
        len(urls),
    )
    return urls

def log_failed_article(config: dict, article_uuid: str, article_url: str, error_type: str, error_message: str)->None:
    AppSettings.configure(config)
    AppSettings.setup()
    ensure_schema()
    failed_model=FailedArticlesModel.get_model()
    try:
        existing=failed_model.objects.filter(uuid=article_uuid).first()
        if existing:
            existing.attempt_count+=1
            existing.error_message=error_message
            existing.error_type=error_type
            existing.url=article_url
            existing.save()
            logger.debug("Updated failed article record for UUID: %s", article_uuid)
        else:
            failed_model.objects.create(
                uuid=article_uuid,
                url=article_url,
                error_type=error_type,
                error_message=error_message,
                attempt_count=1,
            )
            logger.info("Created failed article record for UUID: %s with error type: %s", article_uuid, error_type)
    except Exception as exc:
        logger.error("Failed to log failed article: %s", exc)

def write_article_to_db(config: dict, article: Dict) -> bool:
    from article_processer import write_article_to_db as process_article_to_db
    return process_article_to_db(config, article)
