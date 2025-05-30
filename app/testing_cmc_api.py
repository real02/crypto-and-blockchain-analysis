from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

import requests

import json


BASE_URL = "https://pro-api.coinmarketcap.com"
API_KEY = "7861b72a-91f1-4b5b-8d6c-fa49c1b63189"

if __name__ == "__main__":
    # url = f"{BASE_URL}/v4/dex/networks/list"
    url = f"{BASE_URL}/v4/dex/spot-pairs/latest?network_slug=Starknet"
    headers = {"X-CMC_PRO_API_KEY": API_KEY, "Accepts": "application/json"}

    session = Session()
    session.headers.update(headers)

    response = requests.get(url, headers=headers)

    try:
        response = session.get(url)
        data = json.loads(response.text)
        print(data)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
