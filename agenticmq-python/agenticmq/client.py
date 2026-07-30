import httpx
from typing import Dict, Any, List, Optional

class AgenticMQError(Exception):
    """Base exception for AgenticMQ errors."""
    pass

class AgenticMQClient:
    """Async client for interacting with the AgenticMQ broker."""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8080"):
        self.base_url = base_url.rstrip("/")
        # Set a 60-second default timeout to handle server-side long polling safely
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=httpx.Timeout(60.0))

    async def close(self):
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def submit_task(
        self,
        prompt_data: str,
        system_context: Optional[str] = None,
        token_budget: int = 2000,
        max_cost_usd: float = 0.05,
        model: str = "gpt-4o",
        fallback_models: Optional[List[str]] = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """Submit a new task to the broker."""
        payload = {
            "prompt_data": prompt_data,
            "system_context": system_context,
            "token_budget": token_budget,
            "max_cost_usd": max_cost_usd,
            "model": model,
            "fallback_models": fallback_models or [],
            "max_retries": max_retries,
        }
        
        response = await self._client.post("/tasks", json=payload)
        if response.status_code != 201:
            raise AgenticMQError(f"Failed to submit task: {response.status_code} - {response.text}")
        return response.json()

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Fetch details/status of a specific task."""
        response = await self._client.get(f"/tasks/{task_id}")
        if response.status_code != 200:
            raise AgenticMQError(f"Failed to get task {task_id}: {response.status_code} - {response.text}")
        return response.json()

    async def resume_task(self, task_id: str, resume_key: str) -> Dict[str, Any]:
        """Resume a paused task using its unique resume verification key."""
        headers = {"x-resume-key": resume_key}
        response = await self._client.post(f"/tasks/{task_id}/resume", headers=headers)
        if response.status_code != 200:
            raise AgenticMQError(f"Failed to resume task {task_id}: {response.status_code} - {response.text}")
        return response.json()

    async def set_limit(self, model: str, tpm: int, rpm: int):
        """Set or update rate limits for a specific model."""
        payload = {
            "model": model,
            "tpm": tpm,
            "rpm": rpm,
        }
        response = await self._client.post("/limits", json=payload)
        if response.status_code != 200:
            raise AgenticMQError(f"Failed to set limit for {model}: {response.status_code} - {response.text}")
        return response.text
