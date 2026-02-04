import sys
from pathlib import Path
from typing import Optional

import django
from django.apps import AppConfig, apps
from django.conf import settings
from django.db import connection, models
from utils import get_logger

logger=get_logger(__name__)

class AppSettings:

    APP_LABEL: str="rss_articles"
    TABLE_NAME: str="rss_feed_articles"
    TABLE_NAME_METADATA: str="article_metadata"
    TABLE_NAME_FAILED: str="failed_articles"
    TABLE_NAME_FAILED_EMBEDDINGS: str="failed_vector_embeddings"
    TABLE_NAME_VECTOR_TRACKING: str="vector_database_tracking"
    _CONFIGURED: bool=False
    _CURRENT_DB_PATH: Optional[str]=None

    @classmethod
    def get_db_path_from_config(cls, config: dict) -> Path:

        db_path_setting=config.get("sql_db", "data/sql_db/db.sqlite3")
        db_path=Path(db_path_setting)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path

    @classmethod
    def configure(cls, config: dict) -> None:

        if logger:
            logger.debug("Configuring Django settings...")

        db_path=cls.get_db_path_from_config(config)
        cls._CURRENT_DB_PATH=str(db_path)

        if settings.configured:

            if settings.DATABASES["default"]["NAME"] != str(db_path):
                settings.DATABASES["default"]["NAME"]=str(db_path)
                connection.close()
                if logger:
                    logger.debug("Updated database path to: %s", str(db_path))
        else:
            settings.configure(
                INSTALLED_APPS=["django.contrib.contenttypes"],
                DATABASES={
                    "default": {
                        "ENGINE": "django.db.backends.sqlite3",
                        "NAME": str(db_path),
                    }
                },
                TIME_ZONE="UTC",
                USE_TZ=True,
                DEFAULT_AUTO_FIELD="django.db.models.AutoField",
            )
            if logger:
                logger.debug("Django settings configured")

        cls._CONFIGURED=True

    @classmethod
    def setup(cls) -> None:

        if not apps.ready:
            django.setup()
            if logger:
                logger.debug("Django setup completed")

        if cls.APP_LABEL not in apps.app_configs:
            app_config=AppConfig(cls.APP_LABEL, sys.modules[__name__])
            app_config.apps=apps
            app_config.models={}
            app_config.models_module=sys.modules[__name__]
            apps.app_configs[cls.APP_LABEL]=app_config
            apps.clear_cache()
            if logger:
                logger.debug("App config registered for: %s", cls.APP_LABEL)

    @classmethod
    def is_configured(cls) -> bool:
        return cls._CONFIGURED and apps.ready

    @classmethod
    def get_current_db_path(cls) -> Optional[str]:
        return cls._CURRENT_DB_PATH

class RSSFeedArticleModel:
    _MODEL_INSTANCE: Optional[models.Model]=None

    @classmethod
    def get_model(cls) -> models.Model:
        if cls._MODEL_INSTANCE is not None:
            return cls._MODEL_INSTANCE

        attrs={
            "__module__": __name__,
            "uuid": models.CharField(max_length=36, unique=True, db_index=True),
            "url": models.URLField(max_length=500, unique=True, db_index=True),
            "source_url": models.URLField(max_length=500),
            "domain": models.CharField(max_length=255, db_index=True),
            "fetched_at": models.DateTimeField(auto_now_add=True),
            "objects": models.Manager(),
        }

        meta=type(
            "Meta",
            (),
            {
                "app_label": AppSettings.APP_LABEL,
                "db_table": AppSettings.TABLE_NAME,
            },
        )
        attrs["Meta"]=meta

        model=type("RSSFeedArticle", (models.Model,), attrs)

        try:
            if "RSSFeedArticle" not in apps.get_app_config(AppSettings.APP_LABEL).models:
                apps.register_model(AppSettings.APP_LABEL, model)
        except (RuntimeError, LookupError):
            try:
                model=apps.get_model(AppSettings.APP_LABEL, "RSSFeedArticle")
            except LookupError:
                apps.register_model(AppSettings.APP_LABEL, model)

        cls._MODEL_INSTANCE=model
        if logger:
            logger.debug("RSSFeedArticle model initialized")
        return model

class ArticleMetadataModel:
    _MODEL_INSTANCE: Optional[models.Model]=None

    @classmethod
    def get_model(cls) -> models.Model:
        if cls._MODEL_INSTANCE is not None:
            return cls._MODEL_INSTANCE

        rss_article_model=RSSFeedArticleModel.get_model()

        attrs={
            "__module__": __name__,
            "uuid": models.ForeignKey(
                rss_article_model,
                on_delete=models.CASCADE,
                to_field="uuid",
                db_column="uuid",
            ),
            "url": models.URLField(max_length=500, blank=True),
            "title": models.TextField(blank=True),
            "pub_date": models.DateTimeField(null=True, blank=True),
            "description": models.TextField(blank=True),
            "content": models.TextField(blank=True),
            "creator": models.CharField(max_length=255, blank=True),
            "category": models.TextField(blank=True),
            "word_count": models.IntegerField(default=0),
            "language": models.CharField(max_length=32, default="en"),
            "objects": models.Manager(),
        }

        meta=type(
            "Meta",
            (),
            {
                "app_label": AppSettings.APP_LABEL,
                "db_table": AppSettings.TABLE_NAME_METADATA,
            },
        )
        attrs["Meta"]=meta

        model=type("ArticleMetadata", (models.Model,), attrs)

        try:
            if "ArticleMetadata" not in apps.get_app_config(AppSettings.APP_LABEL).models:
                apps.register_model(AppSettings.APP_LABEL, model)
        except (RuntimeError, LookupError):
            try:
                model=apps.get_model(AppSettings.APP_LABEL, "ArticleMetadata")
            except LookupError:
                apps.register_model(AppSettings.APP_LABEL, model)

        cls._MODEL_INSTANCE=model
        if logger:
            logger.debug("ArticleMetadata model initialized")
        return model

class FailedArticlesModel:
    _MODEL_INSTANCE: Optional[models.Model]=None

    @classmethod
    def get_model(cls) -> models.Model:
        if cls._MODEL_INSTANCE is not None:
            return cls._MODEL_INSTANCE

        attrs={
            "__module__": __name__,
            "uuid": models.CharField(max_length=36, db_index=True),
            "url": models.URLField(max_length=500, blank=True),
            "error_type": models.CharField(max_length=100),
            "error_message": models.TextField(),
            "attempt_count": models.IntegerField(default=1),
            "last_attempted_at": models.DateTimeField(auto_now=True),
            "created_at": models.DateTimeField(auto_now_add=True),
            "objects": models.Manager(),
        }

        meta=type(
            "Meta",
            (),
            {
                "app_label": AppSettings.APP_LABEL,
                "db_table": AppSettings.TABLE_NAME_FAILED,
            },
        )
        attrs["Meta"]=meta

        model=type("FailedArticles", (models.Model,), attrs)

        try:
            if "FailedArticles" not in apps.get_app_config(AppSettings.APP_LABEL).models:
                apps.register_model(AppSettings.APP_LABEL, model)
        except (RuntimeError, LookupError):
            try:
                model=apps.get_model(AppSettings.APP_LABEL, "FailedArticles")
            except LookupError:
                apps.register_model(AppSettings.APP_LABEL, model)

        cls._MODEL_INSTANCE=model
        if logger:
            logger.debug("FailedArticles model initialized")
        return model

class FailedVectorEmbeddingsModel:
    _MODEL_INSTANCE: Optional[models.Model]=None

    @classmethod
    def get_model(cls) -> models.Model:
        if cls._MODEL_INSTANCE is not None:
            return cls._MODEL_INSTANCE

        attrs={
            "__module__": __name__,
            "article_uuid": models.CharField(max_length=36, db_index=True),
            "url": models.URLField(max_length=500, blank=True),
            "title": models.CharField(max_length=500, blank=True),
            "domain": models.CharField(max_length=255, blank=True),
            "error_type": models.CharField(max_length=100),
            "error_message": models.TextField(),
            "chunk_index": models.IntegerField(default=0),
            "total_chunks": models.IntegerField(default=1),
            "attempt_count": models.IntegerField(default=1),
            "last_attempted_at": models.DateTimeField(auto_now=True),
            "created_at": models.DateTimeField(auto_now_add=True),
            "objects": models.Manager(),
        }

        meta=type(
            "Meta",
            (),
            {
                "app_label": AppSettings.APP_LABEL,
                "db_table": AppSettings.TABLE_NAME_FAILED_EMBEDDINGS,
            },
        )
        attrs["Meta"]=meta

        model=type("FailedVectorEmbeddings", (models.Model,), attrs)

        try:
            if "FailedVectorEmbeddings" not in apps.get_app_config(AppSettings.APP_LABEL).models:
                apps.register_model(AppSettings.APP_LABEL, model)
        except (RuntimeError, LookupError):
            try:
                model=apps.get_model(AppSettings.APP_LABEL, "FailedVectorEmbeddings")
            except LookupError:
                apps.register_model(AppSettings.APP_LABEL, model)

        cls._MODEL_INSTANCE=model
        if logger:
            logger.debug("FailedVectorEmbeddings model initialized")
        return model

class VectorDatabaseTrackingModel:
    _MODEL_INSTANCE: Optional[models.Model]=None

    @classmethod
    def get_model(cls) -> models.Model:
        if cls._MODEL_INSTANCE is not None:
            return cls._MODEL_INSTANCE

        attrs={
            "__module__": __name__,
            "article_uuid": models.CharField(max_length=36, unique=True, db_index=True),
            "url": models.URLField(max_length=500, blank=True),
            "title": models.CharField(max_length=500, blank=True),
            "domain": models.CharField(max_length=255, blank=True),
            "namespace": models.CharField(max_length=255, default="rss-feeds", db_index=True),
            "vector_id": models.CharField(max_length=500, blank=True),
            "status": models.CharField(max_length=50, default="pending", db_index=True, choices=[("pending", "Pending"), ("synced", "Synced"), ("failed", "Failed")]),
            "total_chunks": models.IntegerField(default=1),
            "synced_chunks": models.IntegerField(default=0),
            "error_message": models.TextField(blank=True),
            "synced_at": models.DateTimeField(null=True, blank=True),
            "created_at": models.DateTimeField(auto_now_add=True),
            "updated_at": models.DateTimeField(auto_now=True),
            "objects": models.Manager(),
        }

        meta=type(
            "Meta",
            (),
            {
                "app_label": AppSettings.APP_LABEL,
                "db_table": AppSettings.TABLE_NAME_VECTOR_TRACKING,
            },
        )
        attrs["Meta"]=meta

        model=type("VectorDatabaseTracking", (models.Model,), attrs)

        try:
            if "VectorDatabaseTracking" not in apps.get_app_config(AppSettings.APP_LABEL).models:
                apps.register_model(AppSettings.APP_LABEL, model)
        except (RuntimeError, LookupError):
            try:
                model=apps.get_model(AppSettings.APP_LABEL, "VectorDatabaseTracking")
            except LookupError:
                apps.register_model(AppSettings.APP_LABEL, model)

        cls._MODEL_INSTANCE=model
        if logger:
            logger.debug("VectorDatabaseTracking model initialized")
        return model

def ensure_schema() -> None:
    rss_model=RSSFeedArticleModel.get_model()
    rss_table_name=rss_model._meta.db_table

    existing_tables=connection.introspection.table_names()
    if rss_table_name not in existing_tables:
        with connection.schema_editor() as editor:
            editor.create_model(rss_model)
        if logger:
            logger.info("Created database table: %s", rss_table_name)
    else:
        if logger:
            logger.debug("Database table already exists: %s", rss_table_name)

    metadata_model=ArticleMetadataModel.get_model()
    metadata_table_name=metadata_model._meta.db_table

    existing_tables=connection.introspection.table_names()
    if metadata_table_name not in existing_tables:
        with connection.schema_editor() as editor:
            editor.create_model(metadata_model)
        if logger:
            logger.info("Created database table: %s", metadata_table_name)
    else:
        if logger:
            logger.debug("Database table already exists: %s", metadata_table_name)

    failed_model=FailedArticlesModel.get_model()
    failed_table_name=failed_model._meta.db_table

    existing_tables=connection.introspection.table_names()
    if failed_table_name not in existing_tables:
        with connection.schema_editor() as editor:
            editor.create_model(failed_model)
        if logger:
            logger.info("Created database table: %s", failed_table_name)
    else:
        if logger:
            logger.debug("Database table already exists: %s", failed_table_name)

    failed_embeddings_model=FailedVectorEmbeddingsModel.get_model()
    failed_embeddings_table_name=failed_embeddings_model._meta.db_table

    existing_tables=connection.introspection.table_names()
    if failed_embeddings_table_name not in existing_tables:
        with connection.schema_editor() as editor:
            editor.create_model(failed_embeddings_model)
        if logger:
            logger.info("Created database table: %s", failed_embeddings_table_name)
    else:
        if logger:
            logger.debug("Database table already exists: %s", failed_embeddings_table_name)

    vector_tracking_model=VectorDatabaseTrackingModel.get_model()
    vector_tracking_table_name=vector_tracking_model._meta.db_table

    existing_tables=connection.introspection.table_names()
    if vector_tracking_table_name not in existing_tables:
        with connection.schema_editor() as editor:
            editor.create_model(vector_tracking_model)
        if logger:
            logger.info("Created database table: %s", vector_tracking_table_name)
    else:
        if logger:
            logger.debug("Database table already exists: %s", vector_tracking_table_name)
