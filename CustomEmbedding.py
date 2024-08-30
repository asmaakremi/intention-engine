import numpy as np
from typing import List
from llama_index.core.embeddings import BaseEmbedding
import requests

class CustomAPIEmbeddings(BaseEmbedding):
    _api_key: str = "vtYvpB9U+iUQwl0K0MZIj+Uo5u6kilAZJdgHGVBEhNc="
    _embeddings_url: str = "http://px101.prod.exalead.com:8110/v1/embeddings"
    _headers = {
        'Authorization': 'Bearer ' + _api_key,
        'Content-Type': 'application/json'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def class_name(cls) -> str:
        return "custom_api"

    def _aget_query_embedding(self, query: str) -> List[float]:
        return self._get_embeddings([query], "Represent a document for semantic search:")[0]

    def _get_query_embedding(self, query: str) -> List[float]:
        return self._get_embeddings([query], "Represent a document for semantic search:")[0]

    def _get_text_embedding(self, text: str) -> List[float]:
        return self._get_embeddings([text], "Represent a document for semantic search:")[0]

    def _get_text_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self._get_embeddings(texts, "Represent a document for semantic search:")

    def _get_embeddings(self, texts: List[str], instruction: str) -> List[List[float]]:
        payload = {
            "model": "BAAI/bge-large-en-v1.5",
            "input": texts,
            "encoding_format": "float",
            "instruct": instruction,
        }
        response = requests.post(self._embeddings_url, headers=self._headers, json=payload)
        if response.status_code == 200:
            response_data = response.json()
            embeddings_list = [item['embedding'] for item in response_data['data']]
            return np.array(embeddings_list).tolist()  
        else:
            raise Exception(f"Failed to get embeddings: {response.status_code}, {response.text}")
