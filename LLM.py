import requests
from typing import Optional, List, Mapping, Any

from llama_index.core import SimpleDirectoryReader, SummaryIndex
from llama_index.core.callbacks import CallbackManager
from llama_index.core.llms import (
    CustomLLM,
    CompletionResponse,
    CompletionResponseGen,
    LLMMetadata,
)
from llama_index.core.llms.callbacks import llm_completion_callback
from llama_index.core import Settings


class LLM(CustomLLM):
    context_window: int = 3900
    num_output: int = 256
    model_name: str = "custom"
    api_url: str = "http://px101.prod.exalead.com:8110/v1/chat/completions"

    headers = {
        'Authorization': 'Bearer vtYvpB9U+iUQwl0K0MZIj+Uo5u6kilAZJdgHGVBEhNc=',
        'Content-Type': 'application/json'
    }

    @property
    def metadata(self) -> LLMMetadata:
        """Get LLM metadata."""
        return LLMMetadata(
            context_window=self.context_window,
            num_output=self.num_output,
            model_name=self.model_name,
        )

    @llm_completion_callback()
    def complete(self,prompt: str, **kwargs: Any) -> CompletionResponse:
        messages = [{"role": "user", "content": prompt}]
        payload = {
        "model":"meta-llama/Meta-Llama-3-8B-Instruct",
        "messages": messages,  
        "max_tokens": 500,
        "top_p": 1,
        "stop": ["string"],
        "response_format": {
            "type": "text", 
            "temperature": 0.7
        }
    }
        response = requests.post(self.api_url, headers=self.headers, json=payload)
        if response.status_code == 200:
            generated_response = response.json()['choices'][0]['message']['content'].strip()
            return CompletionResponse(text=generated_response)
        else:
            return CompletionResponse(text="Error: API request failed")

    @llm_completion_callback()
    def stream_complete(
        self, prompt: str, **kwargs: Any
    ) -> CompletionResponseGen:
        # Stream complete can iterate through response one token at a time
        messages = [{"role": "user", "content": prompt}]
        payload = {
            "model":"meta-llama/Meta-Llama-3-8B-Instruct",
            "messages": messages,
            "max_tokens": 1500,
            "top_p": 1,
            "stop": ["string"],
            "response_format": {
                "type": "text", 
                "temperature": 0.7
            }
        }
        response = requests.post(self.api_url, headers=self.headers, json=payload)
        if response.status_code == 200:
            generated_response = response.json()['choices'][0]['message']['content'].strip()
            for token in generated_response:
                yield CompletionResponse(text=token, delta=token)
        else:
            yield CompletionResponse(text="Error", delta="Error")