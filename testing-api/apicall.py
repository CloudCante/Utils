import psycopg2
import requests

API_URL = "http://192.168.0.99:5000/api/v1/sql-portal"

response = requests.get(f"{API_URL}/tables")
print(f"Tables: {response.json()['tables'][:3]}")

response = requests.post(
    f"{API_URL}/serial-lookup",
    json={"serialNumbers": ["1652324037004"]}
)

data = response.json()
print(f"Found {data['testboard']['count']} testboard records")
print(f"Sample: {data['testboard']['records'][0]}")