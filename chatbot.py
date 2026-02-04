import os
import json
import yaml
import chromadb
from datetime import datetime, timezone
from dotenv import load_dotenv
from mistralai import Mistral
from mistralai.models import AssistantMessage, SystemMessage, UserMessage, ToolMessage
from embedding_funcs import JinaCustomEmbeddingFunction
from utils import get_logger

load_dotenv()
logger=get_logger(__name__)

with open("config.yaml", "r") as f:
    config=yaml.safe_load(f)

chroma_client=chromadb.PersistentClient(path=config["chroma_db"]["persist_directory"])
embedding_function=JinaCustomEmbeddingFunction(
    model="jina-embeddings-v3",
    task="retrieval.query",
    api_key=os.environ.get("JINA_API_KEY"),
    delay=config["chroma_db"].get("embedding_delay", 0.1)
)
collection=chroma_client.get_or_create_collection(
    name=config["chroma_db"]["collection_name"],
    embedding_function=embedding_function,
    metadata={"hnsw:space": "cosine"}
)

def search_by_text(query_text: str, n_results: int=5)->str:
    try:
        results=collection.query(query_texts=[query_text], n_results=n_results)
        return json.dumps(results)
    except Exception as e:
        logger.error(f"Search failed for query '{query_text}': {e}")
        return json.dumps({"error": str(e)})

def search_with_metadata_filter(query_text: str, domain: str=None, n_results: int=5)->str:
    try:
        where_filter=None
        if domain:
            where_filter={"domain": domain}
        results=collection.query(query_texts=[query_text], n_results=n_results, where=where_filter)
        return json.dumps(results)
    except Exception as e:
        logger.error(f"Metadata filter search failed: {e}")
        return json.dumps({"error": str(e)})

def search_with_document_filter(query_text: str, contains: str=None, n_results: int=5)->str:
    try:
        where_document_filter=None
        if contains:
            where_document_filter={"$contains": contains}
        results=collection.query(query_texts=[query_text], n_results=n_results, where_document=where_document_filter)
        return json.dumps(results)
    except Exception as e:
        logger.error(f"Document filter search failed: {e}")
        return json.dumps({"error": str(e)})

def get_by_metadata(domain: str=None, limit: int=10)->str:
    try:
        where_filter=None
        if domain:
            where_filter={"domain": domain}
        results=collection.get(where=where_filter, limit=limit, include=["documents", "metadatas"])
        return json.dumps(results)
    except Exception as e:
        logger.error(f"Get by metadata failed: {e}")
        return json.dumps({"error": str(e)})

def get_current_time()->str:
    try:
        gmt_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        local_tz=datetime.now(timezone.utc).astimezone().tzinfo
        local_time=datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
        return json.dumps({"gmt": gmt_time, "local": local_time, "timezone": str(local_tz)})
    except Exception as e:
        logger.error(f"Failed to get time: {e}")
        return json.dumps({"error": str(e)})

tools=[
    {
        "type": "function",
        "function": {
            "name": "search_by_text",
            "description": "Search articles in the database by text query using automatic embeddings",
            "parameters": {
                "type": "object",
                "properties": {
                    "query_text": {
                        "type": "string",
                        "description": "The search query text"
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Number of results to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["query_text"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_with_metadata_filter",
            "description": "Search articles filtered by domain and text query",
            "parameters": {
                "type": "object",
                "properties": {
                    "query_text": {
                        "type": "string",
                        "description": "The search query text"
                    },
                    "domain": {
                        "type": "string",
                        "description": "Domain to filter by (e.g., cointelegraph.com, cryptoslate.com)"
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Number of results to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["query_text"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_with_document_filter",
            "description": "Search articles that contain a specific keyword in the document content",
            "parameters": {
                "type": "object",
                "properties": {
                    "query_text": {
                        "type": "string",
                        "description": "The search query text"
                    },
                    "contains": {
                        "type": "string",
                        "description": "Keyword that must be contained in the document"
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Number of results to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["query_text"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_by_metadata",
            "description": "Browse articles by metadata filter (e.g., by domain)",
            "parameters": {
                "type": "object",
                "properties": {
                    "domain": {
                        "type": "string",
                        "description": "Domain to filter by"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of articles to retrieve (default 10)",
                        "default": 10
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": "Get the current GMT time and local time with timezone",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        }
    }
]

names_to_functions={
    'search_by_text': search_by_text,
    'search_with_metadata_filter': search_with_metadata_filter,
    'search_with_document_filter': search_with_document_filter,
    'get_by_metadata': get_by_metadata,
    'get_current_time': get_current_time
}

class SimpleChatBot:
    def __init__(self, api_key, model="mistral-small-latest", system_message=None):
        self.client = Mistral(api_key=api_key)
        self.model = model
        self.messages = []
        if system_message:
            self.messages.append(SystemMessage(content=system_message))

    def chat(self):
        print("Type your message and hit enter. Press CTRL+C to exit.")
        while True:
            try:
                user_input = input("YOU: ")
                self.messages.append(UserMessage(content=user_input))

                print("MISTRAL:")
                response=self.client.chat.complete(
                    model=self.model,
                    messages=self.messages,
                    tools=tools,
                    tool_choice="auto",
                    parallel_tool_calls=True
                )

                while response.choices[0].message.tool_calls:
                    tool_calls=response.choices[0].message.tool_calls
                    self.messages.append(AssistantMessage(content=None, tool_calls=tool_calls))

                    for tool_call in tool_calls:
                        function_name=tool_call.function.name
                        function_params=json.loads(tool_call.function.arguments)
                        function_result=names_to_functions[function_name](**function_params)
                        self.messages.append(ToolMessage(content=function_result, tool_call_id=tool_call.id))

                    response=self.client.chat.complete(
                        model=self.model,
                        messages=self.messages,
                        tools=tools,
                        tool_choice="auto"
                    )

                assistant_response=response.choices[0].message.content
                if assistant_response:
                    print(assistant_response)
                    self.messages.append(AssistantMessage(content=assistant_response))

            except KeyboardInterrupt:
                print("\nExiting...")
                break

if __name__ == "__main__":
    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        raise ValueError("Please set the MISTRAL_API_KEY environment variable.")

    system_msg="You are a helpful assistant that searches a cryptocurrency news database. Use the available search tools to find relevant articles when asked. Always provide useful context from the search results."
    bot = SimpleChatBot(api_key, system_message=system_msg)
    bot.chat()
