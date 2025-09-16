from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import redis.asyncio as redis
import cloudpickle
import json
from typing import List, Dict, Any
from datetime import datetime

from shared.models import (
    Task, WorkUnit, Worker, TaskSubmission, TaskResult, 
    TaskStatus, WorkerStatus
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await redis.from_url("redis://localhost:6379")
    app.state.workers = {}
    app.state.tasks = {}
    yield
    await app.state.redis.close()


app = FastAPI(title="Distributed Computing Coordinator", lifespan=lifespan)

# Mount the web interface
app.mount("/static", StaticFiles(directory="web"), name="static")

@app.get("/")
async def root():
    return FileResponse("web/index.html")

@app.get("/worker")
async def web_worker():
    return FileResponse("web/index.html")


@app.get("/health")
async def health_check():
    try:
        await app.state.redis.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}


@app.post("/tasks/submit")
async def submit_task(submission: TaskSubmission) -> TaskResult:
    try:
        function_bytes = cloudpickle.dumps(compile(submission.function_code, '<string>', 'exec'))
        
        task = Task(
            function_code=function_bytes,
            input_data=submission.input_data
        )
        
        app.state.tasks[task.id] = task
        
        work_units = []
        for i, data_chunk in enumerate(submission.input_data):
            work_unit = WorkUnit(
                task_id=task.id,
                data_chunk=data_chunk
            )
            work_units.append(work_unit)
            
            await app.state.redis.lpush(
                "work_queue", 
                json.dumps({
                    "work_unit_id": work_unit.id,
                    "task_id": task.id,
                    "function_code": function_bytes.hex(),
                    "data_chunk": data_chunk
                })
            )
        
        return TaskResult(
            task_id=task.id,
            status=task.status,
            created_at=task.created_at
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to submit task: {str(e)}")


@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str) -> TaskResult:
    if task_id not in app.state.tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = app.state.tasks[task_id]
    
    await check_task_completion(task_id)
    
    return TaskResult(
        task_id=task.id,
        status=task.status,
        result=task.result,
        error=task.error,
        created_at=task.created_at,
        completed_at=task.completed_at
    )


async def check_task_completion(task_id: str):
    task = app.state.tasks[task_id]
    
    if task.status != TaskStatus.PENDING:
        return
    
    # Get all results for this task
    result_count = await app.state.redis.llen(f"results:{task_id}")
    print(f"Task {task_id}: {result_count}/{len(task.input_data)} results received")
    
    if result_count >= len(task.input_data):
        results = []
        while True:
            result_data = await app.state.redis.rpop(f"results:{task_id}")
            if not result_data:
                break
            results.append(json.loads(result_data))
        
        try:
            aggregated_results = []
            has_error = False
            error_messages = []
            
            for result in results:
                if result.get("error"):
                    has_error = True
                    error_messages.append(result["error"])
                else:
                    aggregated_results.append(result["result"])
            
            if has_error:
                task.status = TaskStatus.FAILED
                task.error = "; ".join(error_messages)
            else:
                task.status = TaskStatus.COMPLETED
                task.result = aggregated_results
            
            task.completed_at = datetime.utcnow()
            print(f"Task {task_id} completed with {len(aggregated_results)} results")
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = f"Failed to aggregate results: {str(e)}"
            task.completed_at = datetime.utcnow()


@app.get("/workers")
async def list_workers():
    return {"workers": list(app.state.workers.values())}


@app.post("/workers/register")
async def register_worker(worker_data: dict):
    worker = Worker(
        host=worker_data.get("host", "unknown"),
        port=worker_data.get("port", 0)
    )
    
    app.state.workers[worker.id] = worker
    
    await app.state.redis.set(
        f"worker:{worker.id}",
        json.dumps({
            "id": worker.id,
            "host": worker.host,
            "port": worker.port,
            "status": worker.status.value,
            "last_heartbeat": worker.last_heartbeat.isoformat()
        }),
        ex=300
    )
    
    return {"worker_id": worker.id, "status": "registered"}


@app.post("/workers/{worker_id}/heartbeat")
async def worker_heartbeat(worker_id: str):
    if worker_id not in app.state.workers:
        return {"status": "worker_not_found", "message": "Please re-register"}
    
    worker = app.state.workers[worker_id]
    worker.last_heartbeat = datetime.utcnow()
    worker.status = WorkerStatus.IDLE
    
    await app.state.redis.set(
        f"worker:{worker_id}",
        json.dumps({
            "id": worker.id,
            "host": worker.host,
            "port": worker.port,
            "status": worker.status.value,
            "last_heartbeat": worker.last_heartbeat.isoformat()
        }),
        ex=300
    )
    
    return {"status": "heartbeat_received"}


@app.get("/workers/{worker_id}/work")
async def get_work(worker_id: str):
    if worker_id not in app.state.workers:
        return {"work": None, "message": "Please re-register"}
    
    # Non-blocking check for work
    work_item = await app.state.redis.rpop("work_queue")
    if not work_item:
        return {"work": None}
    
    work_data = json.loads(work_item)
    
    worker = app.state.workers[worker_id]
    worker.status = WorkerStatus.ACTIVE
    worker.current_task = work_data["work_unit_id"]
    
    print(f"Assigned work to worker {worker_id[:8]}: data_chunk={work_data['data_chunk']}")
    
    return {
        "work": {
            "work_unit_id": work_data["work_unit_id"],
            "task_id": work_data["task_id"],
            "function_code": work_data["function_code"],
            "data_chunk": work_data["data_chunk"]
        }
    }


@app.post("/workers/{worker_id}/results")
async def submit_result(worker_id: str, result_data: dict):
    if worker_id not in app.state.workers:
        return {"status": "worker_not_found", "message": "Please re-register"}
    
    worker = app.state.workers[worker_id]
    worker.status = WorkerStatus.IDLE
    worker.current_task = None
    
    task_id = result_data["task_id"]
    work_unit_id = result_data["work_unit_id"]
    
    await app.state.redis.lpush(
        f"results:{task_id}",
        json.dumps({
            "work_unit_id": work_unit_id,
            "result": result_data.get("result"),
            "error": result_data.get("error"),
            "completed_at": datetime.utcnow().isoformat()
        })
    )
    
    return {"status": "result_received"}


@app.get("/stats")
async def get_stats():
    queue_length = await app.state.redis.llen("work_queue")
    active_workers = sum(1 for w in app.state.workers.values() if w.status == WorkerStatus.ACTIVE)
    
    return {
        "queue_length": queue_length,
        "total_workers": len(app.state.workers),
        "active_workers": active_workers,
        "total_tasks": len(app.state.tasks)
    }