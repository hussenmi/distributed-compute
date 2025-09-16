import requests
import time


def submit_example_task():
    coordinator_url = "http://localhost:8000"
    
    function_code = """
def process_data(x):
    return x * x
"""
    
    input_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    task_data = {
        "function_code": function_code,
        "input_data": input_data
    }
    
    print("Submitting task...")
    response = requests.post(f"{coordinator_url}/tasks/submit", json=task_data)
    
    if response.status_code == 200:
        result = response.json()
        task_id = result["task_id"]
        print(f"Task submitted successfully! Task ID: {task_id}")
        
        print("Waiting for completion...")
        while True:
            status_response = requests.get(f"{coordinator_url}/tasks/{task_id}")
            if status_response.status_code == 200:
                status = status_response.json()
                print(f"Status: {status['status']}")
                
                if status["status"] == "completed":
                    print(f"Results: {status['result']}")
                    break
                elif status["status"] == "failed":
                    print(f"Task failed: {status['error']}")
                    break
                    
            time.sleep(2)
    else:
        print(f"Failed to submit task: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    submit_example_task()