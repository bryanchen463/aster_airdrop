import logging
from aster.rest_api import Client
from aster.lib.utils import config_logging
from aster.error import ClientError
from aster.websocket.client.stream import WebsocketClient
import time
import random
import yaml
import threading
config_logging(logging, logging.INFO, "aster.log")

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT", "BCHUSDT", "LTCUSDT", "XLMUSDT", "XMRUSDT", "XRPUSDT", "XLMUSDT", "XMRUSDT", "XRPUSDT", "XLMUSDT", "XMRUSDT", "XRPUSDT", "XLMUSDT", "XMRUSDT"]

def run(key, secret):
    client = Client(key, secret,base_url="https://fapi.asterdex.com")
    market_info = client.exchange_info()
    logging.info(f"market_info: {market_info}")
    symbol_limits = {}
    for symbol in symbols:
        for symbol_info in market_info["symbols"]:
            if symbol_info["symbol"] == symbol:
                symbol_limits[symbol] = {
                    "qty_precision": symbol_info["quantityPrecision"],
                    "price_precision": symbol_info["pricePrecision"],
                }
    logging.info(f"symbol_limits: {symbol_limits}")
    def message_handler(message):
        print(message)
    # response = client.new_listen_key()

    # logging.info("Receving listen key : {}".format(response["listenKey"]))

    # ws_client = WebsocketClient()
    # ws_client.start()

    # ws_client.user_data(
    #     listen_key=response["listenKey"],
    #     id=1,
    #     callback=message_handler,
    # )

    while True:
        try:
            sleep_time = random.randint(10, 30)
            logging.info(f"sleep_time: {sleep_time}")
            order_timeout = 300000
            orders = client.get_orders()
            logging.info(orders)
            if len(orders) > 0:
                for order in orders:
                    # 30s还没有成交修改价格，里面成交
                    logging.info(f"order symbol {order['symbol']} updateTime: {order['updateTime']} time: {time.time()} diff: {time.time() * 1000 - order['updateTime']}")
                    if time.time() * 1000 - order['updateTime'] > order_timeout:
                        response = client.cancel_order(symbol=order['symbol'], orderId=order['orderId'], origClientOrderId=order['clientOrderId'])
                        logging.info(f"cancel order response: {response}")
                        response = client.new_order(
                            symbol=order['symbol'],
                            side=order['side'],
                            quantity=order['origQty'],
                            type="MARKET",
                        )
                        logging.info(f"new order response: {response}")
                time.sleep(sleep_time)
                continue
            response = client.balance(recvWindow=6000)
            # logging.info(response)
            symbol = random.choice(symbols)
            book_ticker = client.book_ticker(symbol)
            logging.info(f"book_ticker: {book_ticker}")
            balances = client.balance()
            net_balance = 0
            for balance in balances:
                if balance["asset"] == "USDT":
                    net_balance = balance["availableBalance"]
            logging.info(net_balance)
            if float(net_balance) < 0.001:
                logging.info("net_balance is less than 0.001, not trading")
                continue
            symbol_limit = symbol_limits[symbol]
            bid_price = book_ticker["bidPrice"]
            ask_price = book_ticker["askPrice"]
            mid_price = (float(bid_price) + float(ask_price)) / 2
            mid_price = round(mid_price, symbol_limit["price_precision"])
            if mid_price - float(bid_price) <= 0.0000000000001 and float(ask_price) - mid_price <= 0.0000000000001:
                # 价格波动太小，不交易
                continue
            quantity = random.uniform(0.001, 0.005)
            quantity = round(quantity, 3)
            symbol = random.choice(symbols)
            logging.info(f"symbol: {symbol} quantity: {quantity} price: {mid_price}")
            response = client.new_order(
                symbol=symbol,
                side="BUY",
                quantity=quantity,
                price=mid_price,
                timeInForce="GTC",
                type="LIMIT",
            )
            logging.info(f"new order response: {response}")
            response = client.new_order(
                symbol=symbol,
                side="SELL",
                quantity=quantity,
                price=mid_price,
                timeInForce="GTC",
                type="LIMIT",
            )
            logging.info(f"new order response: {response}")
        except ClientError as error:
            logging.error(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
            )
        )
        time.sleep(sleep_time)



    # time.sleep(30)

    # logging.info("closing ws connection")
    # ws_client.stop()

def init_accounts():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    return config["accounts"]

if __name__ == "__main__":
    accounts = init_accounts()
    logging.info(f"accounts: {accounts}")
    threads = []
    for account in accounts:
        # run in parallel
        thread = threading.Thread(target=run, args=(account["key"], account["secret"]))
        thread.start()
        threads.append(thread)
    # wait for all threads to finish
    for thread in threads:
        thread.join()