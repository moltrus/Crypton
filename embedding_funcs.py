import os
import time
import logging
import requests
import json
from chromadb.api.types import Documents, Embeddings
from chromadb.utils.embedding_functions import EmbeddingFunction
from mistralai import Mistral

logger=logging.getLogger(__name__)

class MistralCustomEmbeddingFunction(EmbeddingFunction):
    def __init__(self, model: str="mistral-embed", api_key: str=None, delay: float=0.1)->None:
        self.model=model
        self.api_key=api_key or os.environ.get("MISTRAL_API_KEY")
        self.client=Mistral(api_key=self.api_key)
        self.delay=delay
        if not self.api_key:
            raise ValueError("Mistral API key not provided and MISTRAL_API_KEY environment variable not set.")

    def __call__(self, input: Documents)->Embeddings:
        embeddings=[]
        for i, document in enumerate(input):
            if i>0:
                time.sleep(self.delay)
            try:
                response=self.client.embeddings.create(
                    model=self.model,
                    inputs=[document]
                )
                embedding=response.data[0].embedding
                embeddings.append(embedding)
            except Exception as e:
                logger.error(f"Error embedding document {i}: {e}")
                raise
        return embeddings

class JinaCustomEmbeddingFunction(EmbeddingFunction):
    def __init__(
        self,
        model: str="jina-embeddings-v3",
        task: str="text-matching",
        api_key: str=None,
        delay: float=0.1
    )->None:
        self.model=model
        self.task=task
        self.api_key=api_key or os.environ.get("JINA_API_KEY")
        self.delay=delay
        self.url="https://api.jina.ai/v1/embeddings"

        if not self.api_key:
            raise ValueError("Jina API key not provided and JINA_API_KEY environment variable not set.")

    def __call__(self, input: Documents)->Embeddings:
        embeddings=[]
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        for i, doc in enumerate(input):
            if i>0:
                time.sleep(self.delay)

            try:
                data={
                    "model": self.model,
                    "task": self.task,
                    "input": [doc]
                }
                response=requests.post(self.url, headers=headers, data=json.dumps(data))
                response.raise_for_status()
                result=response.json()
                if "data" in result and len(result["data"])>0:
                    embedding=result["data"][0]["embedding"]
                    embeddings.append(embedding)
                else:
                    logger.error(f"Unexpected response format for document {i}: {result}")
                    raise ValueError(f"No embedding in response for document {i}")
            except Exception as e:
                logger.error(f"Error embedding document {i}: {e}")
                raise

        return embeddings
