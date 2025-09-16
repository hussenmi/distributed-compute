import asyncio
import aiohttp
import cloudpickle
import json
import time
import socket
from typing import Optional, Any


class WorkerClient:
    def __init__(self, coordinator_url: str = "http://localhost:8000"):
        self.coordinator_url = coordinator_url
        self.worker_id: Optional[str] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.running = False
        
    async def start(self):
        self.session = aiohttp.ClientSession()
        await self.register()
        self.running = True
        
        await asyncio.gather(
            self.heartbeat_loop(),
            self.work_loop()
        )
    
    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()
    
    async def register(self):
        hostname = socket.gethostname()
        registration_data = {
            "host": hostname,
            "port": 0
        }
        
        async with self.session.post(
            f"{self.coordinator_url}/workers/register",
            json=registration_data
        ) as response:
            if response.status == 200:
                result = await response.json()
                self.worker_id = result["worker_id"]
                print(f"🤖 Worker {self.worker_id[:8]} registered and ready for work!")
            else:
                raise Exception(f"Failed to register: {response.status}")
    
    async def heartbeat_loop(self):
        while self.running:
            try:
                async with self.session.post(
                    f"{self.coordinator_url}/workers/{self.worker_id}/heartbeat"
                ) as response:
                    if response.status != 200:
                        print(f"Heartbeat failed: {response.status}")
                        
            except Exception as e:
                print(f"Heartbeat error: {e}")
            
            await asyncio.sleep(10)
    
    async def work_loop(self):
        while self.running:
            try:
                work = await self.get_work()
                if work:
                    result = await self.execute_work(work)
                    await self.submit_result(work["work_unit_id"], work["task_id"], result)
                else:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                print(f"Work loop error: {e}")
                await asyncio.sleep(5)
    
    async def get_work(self) -> Optional[dict]:
        try:
            async with self.session.get(
                f"{self.coordinator_url}/workers/{self.worker_id}/work"
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("work")
                    
        except Exception as e:
            print(f"Error getting work: {e}")
        
        return None
    
    async def execute_work(self, work: dict) -> dict:
        try:
            print(f"Worker {self.worker_id[:8]} processing data: {work['data_chunk']}")
            
            function_bytes = bytes.fromhex(work["function_code"])
            code_obj = cloudpickle.loads(function_bytes)
            
            local_vars = {}
            exec(code_obj, {}, local_vars)
            
            if 'process_data' not in local_vars:
                return {"error": "Function 'process_data' not found in submitted code"}
            
            process_data = local_vars['process_data']
            result = process_data(work["data_chunk"])
            
            print(f"Worker {self.worker_id[:8]} completed: {work['data_chunk']} -> {result}")
            return {"result": result}
            
        except Exception as e:
            print(f"Worker {self.worker_id[:8]} error: {e}")
            return {"error": str(e)}
    
    async def submit_result(self, work_unit_id: str, task_id: str, result: dict):
        try:
            result_data = {
                "work_unit_id": work_unit_id,
                "task_id": task_id,
                **result
            }
            
            async with self.session.post(
                f"{self.coordinator_url}/workers/{self.worker_id}/results",
                json=result_data
            ) as response:
                if response.status != 200:
                    print(f"Failed to submit result: {response.status}")
                    
        except Exception as e:
            print(f"Error submitting result: {e}")


async def main():
    worker = WorkerClient()
    try:
        await worker.start()
    except KeyboardInterrupt:
        print("Shutting down worker...")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())