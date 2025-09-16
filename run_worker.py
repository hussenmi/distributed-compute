import asyncio
from worker.client import WorkerClient

if __name__ == "__main__":
    asyncio.run(WorkerClient().start())