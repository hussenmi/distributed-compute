import requests

def check_stats():
    try:
        response = requests.get("http://localhost:8000/stats")
        if response.status_code == 200:
            stats = response.json()
            print("📊 System Stats:")
            print(f"   Active Workers: {stats['active_workers']}")
            print(f"   Total Workers: {stats['total_workers']}")
            print(f"   Queue Length: {stats['queue_length']}")
            print(f"   Total Tasks: {stats['total_tasks']}")
        else:
            print(f"Failed to get stats: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_stats()