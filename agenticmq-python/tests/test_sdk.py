import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import os
import sys

# Add local path to import agenticmq
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from agenticmq import (
    AgenticMQClient,
    AgentWorker,
    HumanApprovalRequired,
    TaskFailed,
    AgenticMQError,
)

class TestAgenticMQSDK(unittest.IsolatedAsyncioTestCase):
    
    @patch("httpx.AsyncClient.post")
    async def test_client_submit_task(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "task_id": "test-uuid-123",
            "status": "pending",
            "current_model": "gpt-4o"
        }
        mock_post.return_value = mock_response

        async with AgenticMQClient() as client:
            task = await client.submit_task(
                prompt_data="Hello, world",
                model="gpt-4o",
                fallback_models=["gpt-4o-mini"]
            )
            
            self.assertEqual(task["task_id"], "test-uuid-123")
            self.assertEqual(task["status"], "pending")
            mock_post.assert_called_once()

    @patch("httpx.AsyncClient.post")
    async def test_client_resume_task(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "task_id": "test-uuid-123",
            "status": "pending",
            "current_model": "gpt-4o"
        }
        mock_post.return_value = mock_response

        async with AgenticMQClient() as client:
            task = await client.resume_task("test-uuid-123", "x-resume-key-123")
            self.assertEqual(task["status"], "pending")
            mock_post.assert_called_once()
            headers = mock_post.call_args[1]["headers"]
            self.assertEqual(headers["x-resume-key"], "x-resume-key-123")

    @patch("httpx.AsyncClient.get")
    @patch("httpx.AsyncClient.post")
    async def test_worker_processing_success(self, mock_post, mock_get):
        # Setup mock poll response
        mock_poll_resp = MagicMock()
        mock_poll_resp.status_code = 200
        mock_poll_resp.json.return_value = {
            "task_id": "test-uuid-123",
            "prompt_data": "Write code",
            "token_budget": 1000
        }
        mock_get.return_value = mock_poll_resp

        # Setup mock complete response
        mock_comp_resp = MagicMock()
        mock_comp_resp.status_code = 200
        mock_post.return_value = mock_comp_resp

        worker = AgentWorker(model="gpt-4o")
        
        async def mock_handler(task):
            return {
                "output": "print('hello')",
                "tokens_used": 150,
                "cost_usd": 0.003
            }
            
        worker.register_handler(mock_handler)
        
        # Test private process task
        await worker._process_task(mock_poll_resp.json())
        
        # Verify it posted complete
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn("/tasks/test-uuid-123/complete", args[0])
        payload = kwargs["json"]
        self.assertEqual(payload["output"], "print('hello')")
        self.assertEqual(payload["tokens_used"], 150)
        self.assertEqual(payload["cost_usd"], 0.003)
        await worker.stop()

    @patch("httpx.AsyncClient.post")
    async def test_worker_processing_pause(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "task_id": "test-uuid-123",
            "status": "paused",
            "resume_key": "x-resume-secret"
        }
        mock_post.return_value = mock_resp

        worker = AgentWorker(model="gpt-4o")
        
        async def mock_handler(task):
            raise HumanApprovalRequired(
                intermediate_output="draft text",
                tokens_used=100,
                cost_usd=0.002
            )
            
        worker.register_handler(mock_handler)
        
        task = {
            "task_id": "test-uuid-123",
            "prompt_data": "Dangerous write",
            "token_budget": 1000
        }
        
        await worker._process_task(task)
        
        # Verify it checkpointed with pause
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn("/tasks/test-uuid-123/checkpoint", args[0])
        payload = kwargs["json"]
        self.assertTrue(payload["pause_request"])
        self.assertEqual(payload["output"], "draft text")
        self.assertEqual(payload["tokens_used"], 100)
        await worker.stop()

    @patch("httpx.AsyncClient.post")
    async def test_worker_processing_failure(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "task_id": "test-uuid-123",
            "status": "pending",
            "current_model": "gpt-4o-mini"
        }
        mock_post.return_value = mock_resp

        worker = AgentWorker(model="gpt-4o")
        
        async def mock_handler(task):
            raise TaskFailed(
                error_message="API Timeout",
                tokens_used=50,
                cost_usd=0.001
            )
            
        worker.register_handler(mock_handler)
        
        task = {
            "task_id": "test-uuid-123",
            "prompt_data": "Call GPT",
            "token_budget": 1000
        }
        
        await worker._process_task(task)
        
        # Verify it posted fail
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn("/tasks/test-uuid-123/fail", args[0])
        payload = kwargs["json"]
        self.assertEqual(payload["error"], "API Timeout")
        self.assertEqual(payload["tokens_used"], 50)
        await worker.stop()

if __name__ == "__main__":
    unittest.main()
