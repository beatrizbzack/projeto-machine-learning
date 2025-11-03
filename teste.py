# inspect_429.py
import requests, certifi
from urllib.parse import urlencode

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 32.127602,
    "longitude": -81.202103,
    "start_date": "2024-01-01",
    "end_date": "2024-01-02",
    "daily": "weather_code,temperature_2m_max",
    "timezone": "UTC"
}
full = url + "?" + urlencode(params)
print("Chamando:", full)
try:
    r = requests.get(url, params=params, timeout=30, verify=certifi.where())
    print("Status:", r.status_code)
    print("Headers:")
    for k,v in r.headers.items():
        print(k, ":", v)
    print("Body (first 300 chars):", r.text[:300])
except requests.exceptions.HTTPError as e:
    r = e.response
    print("HTTPError status:", r.status_code)
    print("Headers on error:")
    for k,v in r.headers.items():
        print(k, ":", v)
    print("Body (first 300 chars):", r.text[:300])
except Exception as e:
    print("Request failed:", type(e).__name__, e)
