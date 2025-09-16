#!/usr/bin/env python3
"""
Remote worker for distributed computing platform.
Run this on any device to contribute compute power.
"""

import asyncio
import sys
from worker.client import WorkerClient

def main():
    if len(sys.argv) < 2:
        print("Usage: python remote_worker.py <coordinator_ip>")
        print("Example: python remote_worker.py 192.168.175.158")
        sys.exit(1)
    
    coordinator_ip = sys.argv[1]
    coordinator_url = f"http://{coordinator_ip}:8000"
    
    print(f"Connecting to coordinator at {coordinator_url}")
    worker = WorkerClient(coordinator_url)
    
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        print("Worker shutting down...")

if __name__ == "__main__":
    main()