import asyncio
import logging
import os
import sys

# Automatically inject the agenticmq-python package path into sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../agenticmq-python")))

from agenticmq import AgenticMQClient, AgentWorker, HumanApprovalRequired

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

async def main():
    print("\n======================================================================")
    print("🚀 AgenticMQ Demo: Human-in-the-Loop Task Coordination")
    print("======================================================================\n")

    # 1. Initialize client
    client = AgenticMQClient(base_url="http://127.0.0.1:8080")

    # 2. Setup the worker for model 'gpt-4o'
    worker = AgentWorker(model="gpt-4o", base_url="http://127.0.0.1:8080", poll_interval=0.5)

    async def coder_agent_handler(task: dict) -> dict:
        output = task.get("output")
        if not output:
            print("\n[Agent] Phase 1: Planning dangerous operations...")
            draft_code = (
                "import os\n"
                "def clean_system():\n"
                "    print('Running safe diagnostics...')\n"
                "    # DANGER: System removal command\n"
                "    os.system('rm -rf /tmp/test-directories/*')\n"
                "    return 'Execution check complete.'"
            )
            # Raise checkpoint pause request
            raise HumanApprovalRequired(
                intermediate_output=draft_code,
                tokens_used=420,
                cost_usd=0.0084
            )
        else:
            print(f"\n[Agent] Phase 2: Resuming task! Human approved code:\n"
                  f"----------------------------------------\n"
                  f"{output}\n"
                  f"----------------------------------------")
            print("[Agent] Running script on target server...")
            
            return {
                "output": "Script execution returned code 0: /tmp/test-directories clean.",
                "tokens_used": 680,
                "cost_usd": 0.0136
            }

    worker.register_handler(coder_agent_handler)

    # Start worker in background
    worker_task = asyncio.create_task(worker.start())
    await asyncio.sleep(0.5) # Wait for worker client to establish

    # 3. Submit task to broker
    print("[Client] Submitting system cleanup task...")
    task = await client.submit_task(
        prompt_data="Write a script to clean up test directories.",
        system_context="You are a DevOps assistant agent.",
        token_budget=1500,
        max_cost_usd=0.05,
        model="gpt-4o",
        fallback_models=["gpt-4o-mini"]
    )
    task_id = task["task_id"]
    print(f"[Client] Task submitted successfully. ID: {task_id}")

    # 4. Wait for worker to poll & pause the task
    print("[Client] Monitoring task state. Waiting for pause checkpoint...")
    resume_key = None
    while True:
        await asyncio.sleep(0.5)
        status_task = await client.get_task(task_id)
        status = status_task.get("status")
        
        if status == "paused":
            resume_key = status_task.get("resume_key")
            print(f"[Client] Task paused. Verification key obtained: {resume_key}")
            break
        elif status == "failed":
            print(f"[Client] Task failed prematurely: {status_task.get('error_message')}")
            return

    # 5. Simulate human approval
    print("\n[Human Admin] Reviewing proposed script changes...")
    await asyncio.sleep(2.0) # Simulating manual review time
    print("[Human Admin] Script verified. Resuming execution...")
    
    # Resume the task on the broker
    await client.resume_task(task_id, resume_key)
    print("[Client] Sent resume command to broker.")

    # 6. Wait for worker to pick up and complete the task
    print("[Client] Monitoring task state. Waiting for completion...")
    while True:
        await asyncio.sleep(0.5)
        status_task = await client.get_task(task_id)
        status = status_task.get("status")
        
        if status == "completed":
            print(f"\n🎉 Task Completed! Final Output:\n"
                  f"----------------------------------------\n"
                  f"{status_task.get('output')}\n"
                  f"----------------------------------------")
            print(f"Total Accumulative Tokens: {status_task.get('tokens_used')}")
            print(f"Total Cost: ${status_task.get('cost_usd'):.5f}")
            break
        elif status == "failed":
            print(f"\n❌ Task Terminally Failed: {status_task.get('error_message')}")
            break

    # Stop the background worker
    await worker.stop()
    await worker_task
    await client.close()
    print("\n======================================================================")
    print("Demo execution finished.")
    print("======================================================================\n")

if __name__ == "__main__":
    asyncio.run(main())
