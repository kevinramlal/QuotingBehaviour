import json
import requests
import urllib.parse

URL = 'https://cloud.iexapis.com/{version}'
URLSB = 'https://sandbox.iexapis.com/{version}/'
VERSION = 'beta'


class IEXApiException(Exception):
    pass


class IEXApi:
    def __init__(self, test=True):
        try:
            with open('token.json', 'r') as f:
                token = json.load(f)
        except FileNotFoundError:
            raise IEXApiException('Token file does not exist!')

        if test:
            self._token = token.get('public_test')
            self._url = URLSB.format(version=VERSION)
        else:
            self._token = token.get('public')
            self._url = URL.format(version=VERSION)

        if self._token is None:
            raise IEXApiException('Token is missing!')

    def __http_request(self, endpoint, param):
        url = self._url + endpoint
        res = requests.get(url, params=urllib.parse.urlencode(param))
        if res.status_code == 200:
            return res.json()
        else:
            raise IEXApiException('Response {0}: {1}'.format(res.status_code, res.text))

    def __http_request_batch(self, param):
        return self.__http_request('/stock/market/batch', param)

    def get_stock_data(self, endpoints=None, symbols=None):
        if isinstance(symbols, list) and isinstance(endpoints, list):
            param = {'token': self._token, 'symbols': ','.join(symbols), 'types': ','.join(endpoints)}
        elif isinstance(symbols, str) and isinstance(endpoints, list):
            param = {'token': self._token, 'symbols': symbols, 'types': ','.join(endpoints)}
        elif isinstance(symbols, list) and isinstance(endpoints, str):
            param = {'token': self._token, 'symbols': ','.join(symbols), 'types': endpoints}
        elif isinstance(symbols, str) and isinstance(endpoints, str):
            param = {'token': self._token, 'symbols': symbols, 'types': endpoints}
        else:
            raise IEXApiException('Invalid input format! symbols and endpoints should either be list or str.')

        try:
            res = self.__http_request_batch(param)
        except IEXApiException:
            raise IEXApiException('Supported stock data endpoints: https://iexcloud.io/docs/api/#stocks')
        return res

    def get_exchange_data(self, endpoint=None, symbols=None):
        if isinstance(symbols, str):
            param = {'token': self._token, 'symbols': symbols}
        elif isinstance(symbols, list):
            param = {'token': self._token, 'symbols': ','.join(symbols)}
        else:
            raise IEXApiException('Invalid symbols format! symbols should either be list or str.')

        if not isinstance(endpoint, str):
            raise IEXApiException('Invalid endpoint format! endpoint should be str')

        try:
            res = self.__http_request('/' + endpoint, param)
        except IEXApiException:
            raise IEXApiException('Supported exchange data type: https://iexcloud.io/docs/api/#investors-exchange-data')
        return res


if __name__ == "__main__":
    SYMBOL = 'AAPL'
    SYMBOLS = ['GOOG', 'AAPL']
    ENDPOINT = 'quote'
    ENDPOINTS = ['quote', 'book']

    iex = IEXApi()

    # test single symbol and single endpoint
    res = iex.get_stock_data(symbols=SYMBOL, endpoints=ENDPOINT)
    print(res)

    # test single symbol and multiple endpoints
    res = iex.get_stock_data(symbols=SYMBOL, endpoints=ENDPOINTS)
    print(res)
