# RSS Feed Aggregator for Cryptocurrency News

## Project Description
This is a comprehensive RSS feed aggregator specifically designed for collecting, processing, and analyzing cryptocurrency and blockchain news articles. The system downloads RSS feeds from multiple sources (including Cointelegraph, CryptoSlate, and Google News), extracts full article content, stores everything in a structured database, and enables semantic search through vector embeddings. It includes a chatbot interface powered by Mistral AI for natural language queries about the news corpus.

## Key Features
- **Multi-Source RSS Collection**: Downloads from configured cryptocurrency news sites and Google News RSS feeds
- **Intelligent Content Extraction**: Uses multiple extraction methods (newspaper3k, trafilatura, readability, Playwright with stealth browsing, Jina Reader API) with automatic fallbacks
- **Structured Data Storage**: SQLite database with Django ORM models for articles, metadata, and tracking
- **Vector Search Capabilities**: Syncs articles to both Pinecone (cloud) and ChromaDB (local) vector databases for semantic search
- **Chatbot Interface**: Mistral AI-powered chatbot that can search and summarize news articles
- **Robust Error Handling**: Tracks failed articles and embeddings with retry mechanisms
- **Configurable Processing**: YAML-based configuration for sources, extraction methods, and database settings
- **Batch Processing Jobs**: Separate scripts for syncing to vector databases and retrying failed operations
- **Graceful Shutdown**: Handles interrupts and saves state during long-running operations

## Installation Instructions

### Prerequisites
- Python 3.8+
- Git

### Clone and Setup
```bash
git clone <repository-url>
cd rss-feed-aggregator
uv venv venv
```

### Install Dependencies
```bash
uv pip install -r requirements.txt
```

### Environment Configuration
```bash
cp env.example .env
# Edit .env with your API keys:
# JINA_API_KEY=your_jina_api_key
# PINECONE_API_KEY=your_pinecone_api_key  
# MISTRAL_API_KEY=your_mistral_api_key
```

### Initial Setup
```bash
uv run setup.py
```

## Usage

### Basic Operation
```bash
# Download RSS feeds only
uv run main.py download

# Process downloaded feeds into database
uv run main.py process

# Full pipeline (download + process + vector sync)
uv run main.py full

# Skip vector sync in full mode
uv run main.py full --skip-vector

# Retry failed vector embeddings
uv run main.py process --retry-vectors
```

### Vector Database Management
```bash
# Sync to ChromaDB
uv run jobs/batch_chroma_db_sync.py --mode sync

# Query ChromaDB
uv run jobs/batch_chroma_db_sync.py --mode query --query-text "bitcoin price"

# Get ChromaDB stats
uv run jobs/batch_chroma_db_sync.py --mode stats

# Sync to Pinecone
uv run jobs/batch_vector_db_sync.py --mode process

# Retry failed Pinecone embeddings
uv run jobs/batch_vector_db_sync.py --mode retry
```

### Chatbot
```bash
uv run chatbot.py
```

## Configuration

The `config.yaml` file controls:
- **Sources**: RSS feed URLs for sites and Google News queries
- **Extraction Methods**: Site-specific content extraction preferences
- **Databases**: Paths for SQLite, ChromaDB, and Pinecone settings
- **Processing**: Batch sizes, refresh rates, and enabled features

## Database Schema

- **RSSFeedArticle**: Core article data (URL, UUID, domain, source)
- **ArticleMetadata**: Content details (title, description, content, pub_date, etc.)
- **FailedArticles**: Tracking of extraction failures
- **FailedVectorEmbeddings**: Embedding failure tracking
- **VectorDatabaseTracking**: Sync status to vector databases

## API Keys Required

- **Jina AI**: For content extraction and embeddings
- **Pinecone**: Cloud vector database
- **Mistral AI**: Chatbot and optional embeddings

## Architecture Notes

- Uses Django ORM without full Django installation for lightweight database operations
- Implements multiple fallback strategies for reliable content extraction
- Supports both cloud (Pinecone) and local (ChromaDB) vector storage
- Designed for cryptocurrency news but configurable for other domains
- Includes comprehensive logging and error recovery mechanisms

This project provides a complete pipeline from RSS collection to AI-powered news analysis, making it suitable for building news aggregation services, research tools, or automated content monitoring systems.