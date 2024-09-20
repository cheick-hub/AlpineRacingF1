import requests
from multiprocessing import Process

def send_request(url : str, body : dict):
    r = requests.post(url, json=body)
    print("Request sent and response received : ", r.json())


def send_logs(logs : dict):
    process = Process(target=send_request, args=("http://127.0.0.1:8000/logs", logs))
    process.start()
