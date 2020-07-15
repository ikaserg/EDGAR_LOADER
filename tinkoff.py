import pandas as pd
import requests

def get_api_url():
    return 'https://api-invest.tinkoff.ru/openapi'

def load_ticker_dataframe(token):
    r=requests.get(url + '/market/stocks', headers={"Authorization":"Bearer "+token})
    return pd.DataFrame(r.json()['payload']['instruments']) 