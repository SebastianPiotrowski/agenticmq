import asyncio
import logging
import httpx
from typing import Callable, Awaitable, Dict, Any, Optional

logger = logging.getLogger("agenticmq.worker")

class AgenticMQError(Exception):
    """Base exception class for AgenticMQ client/worker issues."""
    pass

class HumanApprovalRequired(AgenticMQError):
    """Raised by a task handler when human intervention is needed."""
    def __init__(self, intermediate_output: str, tokens_used: int = 0, cost_usd: float = 0.0):
        super().__init__("Human approval required to resume task.")
        self.intermediate_output = intermediate_output
        self.tokens_used = tokens_used
        self.cost_usd = cost_usd

class TaskFailed(AgenticMQError):
    """Raised by a task handler when an LLM call or processing fails, triggering fallback."""
    def __init__(self, error_message: str, tokens_used: int = 0, cost_usd: float = 0.0):
        super().__init__(error_message)
        self.error_message = error_message
        self.tokens_used = tokens_used
        self.cost_usd = cost_usd

class AgentWorker:
    """Worker daemon that pulls tasks from AgenticMQ and processes them using registered handlers."""
    
    def __init__(self, model: str, base_url: str = "http://127.0.0.1:8080", poll_interval: float = 1.0):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.poll_interval = poll_interval
        # Configured a 60-second default timeout to handle server-side long polling safely
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=httpx.Timeout(60.0))
        self._handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
        self._running = False

    def register_handler(self, handler_fn: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """Register an async handler function to process polled tasks."""
        self._handler = handler_fn

    async def start(self):
        """Start the worker polling loop."""
        if not self._handler:
            raise AgenticMQError("No handler registered. Call register_handler first.")
        
        self._running = True
        logger.info(f"Starting AgentWorker for model '{self.model}'...")
        
        while self._running:
            try:
                task = await self._poll_task()
                if task:
                    logger.info(f"Polled task {task['task_id']}. Running handler...")
                    await self._process_task(task)
                else:
                    await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                await asyncio.sleep(self.poll_interval)

    async def stop(self):
        """Stop the worker loop and close the HTTP client."""
        self._running = False
        await self._client.aclose()
        logger.info("AgentWorker stopped.")

    async def _poll_task(self) -> Optional[Dict[str, Any]]:
        try:
            # We long poll the broker for up to 30 seconds per request
            response = await self._client.get("/tasks/poll", params={"model": self.model, "timeout_seconds": 30})
            if response.status_code == 200:
                # Axum returns JSON task or null
                return response.json()
            elif response.status_code == 204:
                return None
            return None
        except Exception as e:
            logger.error(f"Failed to poll broker: {e}")
            return None

    async def _process_task(self, task: Dict[str, Any]):
        task_id = task["task_id"]
        try:
            # Execute handler
            result = await self._handler(task)
            
            # Extract metrics and output
            output = result.get("output", "")
            tokens_used = result.get("tokens_used", task["token_budget"])
            cost_usd = result.get("cost_usd", 0.0)
            
            complete_payload = {
                "output": output,
                "tokens_used": tokens_used,
                "cost_usd": cost_usd,
            }
            res = await self._client.post(f"/tasks/{task_id}/complete", json=complete_payload)
            if res.status_code == 200:
                logger.info(f"Task {task_id} completed successfully.")
            else:
                logger.error(f"Failed to submit completion for task {task_id}: {res.text}")
                
        except HumanApprovalRequired as har:
            logger.warning(f"Task {task_id} requires human approval.")
            checkpoint_payload = {
                "tokens_used": har.tokens_used,
                "cost_usd": har.cost_usd,
                "output": har.intermediate_output,
                "error_message": None,
                "pause_request": True,
            }
            res = await self._client.post(f"/tasks/{task_id}/checkpoint", json=checkpoint_payload)
            if res.status_code == 200:
                updated_task = res.json()
                logger.warning(
                    f"\n========================================\n"
                    f"TASK {task_id} PAUSED - HUMAN APPROVAL REQUIRED\n"
                    f"Intermediate output:\n{har.intermediate_output}\n"
                    f"\nRESUME KEY (Header: x-resume-key): {updated_task.get('resume_key')}\n"
                    f"========================================\n"
                )
            else:
                logger.error(f"Failed to pause task {task_id}: {res.text}")
                
        except TaskFailed as tf:
            logger.error(f"Task {task_id} handler reported failure: {tf.error_message}")
            fail_payload = {
                "error": tf.error_message,
                "tokens_used": tf.tokens_used,
                "cost_usd": tf.cost_usd,
            }
            res = await self._client.post(f"/tasks/{task_id}/fail", json=fail_payload)
            if res.status_code == 200:
                updated_task = res.json()
                status = updated_task.get("status")
                # If status is back to 'pending', it means it fell back or scheduled a retry
                if status == "pending" or status == "Pending":
                    # Check if it was rescheduled for retry or routed to fallback
                    run_after = updated_task.get("run_after")
                    if run_after:
                        logger.info(f"Task {task_id} failed. Rescheduled for retry count {updated_task.get('retry_count')} with delay.")
                    else:
                        logger.info(f"Task {task_id} failed. Routed to fallback model '{updated_task.get('current_model')}'")
                else:
                    logger.error(f"Task {task_id} marked as Terminally Failed.")
            else:
                logger.error(f"Failed to submit failure for task {task_id}: {res.text}")
                
        except Exception as e:
            logger.error(f"Unexpected exception processing task {task_id}: {e}")
            fail_payload = {
                "error": f"Unexpected error: {str(e)}",
                "tokens_used": task.get("token_budget", 1000),
                "cost_usd": 0.0,
            }
            await self._client.post(f"/tasks/{task_id}/fail", json=fail_payload)
