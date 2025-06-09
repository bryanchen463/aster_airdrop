
from aster.rest_api import Client
import time
import logging
import yaml


symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]


'''
[
  {
  	"buyer": false,
  	"commission": "-0.07819010",
  	"commissionAsset": "USDT",
  	"id": 698759,
  	"maker": false,
  	"orderId": 25851813,
  	"price": "7819.01",
  	"qty": "0.002",
  	"quoteQty": "15.63802",
  	"realizedPnl": "-0.91539999",
  	"side": "SELL",
  	"positionSide": "SHORT",
  	"symbol": "BTCUSDT",
  	"time": 1569514978020
  }
]
'''
def get_trade_vol(client: Client, symbol: str, start_time: int, end_time: int):
    response = client.get_account_trades(symbol=symbol, recvWindow=6000, startTime=start_time, endTime=end_time)
    vol = 0
    for trade in response:
        vol += float(trade['quoteQty'])

    position_risk = client.get_position_risk(symbol=symbol, recvWindow=6000)
    if len(position_risk) > 0:
        print(position_risk[0])
    return vol

def load_config():
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config["accounts"]

if __name__ == "__main__":
    accounts = load_config()
    for account in accounts:
        proxies = { 'https': account["proxy"] }
        client = Client(key=account["key"], secret=account["secret"], proxies=proxies)
        vol = 0
        for symbol in symbols:
            start_time = int(time.time() * 1000) - 1000 * 60 * 60 * 24 * 7
            end_time = int(time.time() * 1000)  - 1000 * 60 * 60 * 24 * 0

            vol += get_trade_vol(client, symbol, start_time, end_time)
        print(account["name"], vol)
