import logging
from aster.rest_api import Client
from aster.lib.utils import config_logging
from aster.error import ClientError
from aster.websocket.client.stream import WebsocketClient
import time
import random
import yaml
import threading
from datetime import datetime
config_logging(logging, logging.INFO, "aster.log")

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT","XRPUSDT", "DOGEUSDT", "AAVEUSDT", "LTCUSDT", "AVAXUSDT"]
random.seed(time.time())

def close_position(client: Client):
    positions = client.get_position_risk()
    logging.info(f"positions: {positions}")
    for position in positions:
        if time.time() * 1000 - position["updateTime"] <= 1000 * 60:
            continue
        if float(position["notional"]) < 5:
            continue
        if position["symbol"] in symbols:
            side = "SELL" if position["positionSide"] == "LONG" else "BUY"
            amount = abs(float(position["positionAmt"]))
            logging.info(f"close position {position['symbol']} {side} {amount}")
            response = client.new_order(symbol=position["symbol"], side=side, type="MARKET", quantity=amount)
            logging.info(f"close position response: {response}")

def is_cost_enough(client: Client, cost_per_day: float):
    # 计算当天整点的时间戳
    start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    income_history = client.get_income_history(startTime=start_time)
    cost = 0
    for income in income_history:
        if income["symbol"] in symbols:
            cost += float(income["income"])
    return cost >= cost_per_day

def run(key, secret, proxy, cost_per_day):
    proxies = { 'https': proxy }
    client = Client(key, secret,base_url="https://fapi.asterdex.com", proxies=proxies)
    if is_cost_enough(client, cost_per_day):
        logging.info("cost is enough, not trading")
        return
    market_info = client.exchange_info()
    logging.info(f"market_info: {market_info}")
    symbol_limits = {}
    for symbol in symbols:
        for symbol_info in market_info["symbols"]:
            if symbol_info["symbol"] == symbol:
                qty_precision = symbol_info["quantityPrecision"]
                price_precision = symbol_info["pricePrecision"]
                tick_size = 0
                min_qty = 0
                max_qty = 0
                step_size = 0
                for filter in symbol_info["filters"]:
                    if filter["filterType"] == "LOT_SIZE":
                        min_qty = filter["minQty"]
                        max_qty = filter["maxQty"]
                        step_size = filter["stepSize"]
                    elif filter["filterType"] == "PRICE_FILTER":
                        tick_size = filter["tickSize"]
                symbol_limits[symbol] = {
                    "qty_precision": int(qty_precision),
                    "price_precision": int(price_precision),
                    "min_qty": float(min_qty),
                    "max_qty": float(max_qty),
                    "tick_size": float(tick_size),
                    "step_size": float(step_size),
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
            sleep_time = random.randint(60, 120)
            logging.info(f"sleep_time: {sleep_time}")
            order_timeout = 1000
            orders = client.get_orders()
            logging.info(orders)
            if len(orders) > 0:
                for order in orders:
                    # 30s还没有成交修改价格，里面成交
                    logging.info(f"order symbol {order['symbol']} updateTime: {order['updateTime']} time: {time.time()} diff: {time.time() * 1000 - order['updateTime']}")
                    if time.time() * 1000 - order['updateTime'] > order_timeout:
                        response = client.cancel_open_orders(symbol=order['symbol'])
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
            close_position(client)
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
            if float(net_balance) < 0.001:
                logging.info("net_balance is less than 0.001, not trading")
                continue
            symbol_limit = symbol_limits[symbol]
            bid_price = book_ticker["bidPrice"]
            ask_price = book_ticker["askPrice"]
            mid_price = (float(bid_price) + float(ask_price)) / 2
            mid_price = int(mid_price / float(symbol_limit["tick_size"])) * float(symbol_limit["tick_size"])
            mid_price = round(mid_price, symbol_limit["price_precision"])
            if abs(mid_price - float(bid_price)) <= 0.0000000000001 or abs(float(ask_price) - mid_price) <= 0.0000000000001:
                # 价格波动太小，不交易
                continue
            # 一笔价值50usdt
            times = random.randint(1, 5)
            min_qty = symbol_limit["min_qty"]
            max_qty = symbol_limit["max_qty"]
            quantity = 50/mid_price
            quantity = quantity + times * min_qty
            quantity = int(quantity / float(symbol_limit["step_size"])) * float(symbol_limit["step_size"])
            quantity = round(quantity, int(symbol_limit["qty_precision"]))
            if quantity > float(max_qty):
                quantity = float(max_qty)
            if quantity * mid_price < 5:
                logging.info(f"quantity * mid_price < 5, not trading")
                continue
            logging.info(f"symbol: {symbol} quantity: {quantity} price: {mid_price}")
            batch_orders = []
            batch_orders.append({
                "symbol":symbol,
                "side":"BUY",
                "quantity":quantity,
                "price":mid_price,
                "timeInForce":"GTC",
                "type":"LIMIT"
            })
            batch_orders.append({
                "symbol":symbol,
                "side":"SELL",
                "quantity":quantity,
                "price":mid_price,
                "timeInForce":"GTC",
                "type":"LIMIT"
            })
            for order in batch_orders:
                response = client.new_order(symbol=order["symbol"], side=order["side"], type=order["type"], quantity=order["quantity"], price=order["price"], timeInForce=order["timeInForce"])
                logging.info(f"new order response: {response}")
        except ClientError as error:
            logging.exception(
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
        thread = threading.Thread(target=run, args=(account["key"], account["secret"], account["proxy"], account["cost_per_day"]))
        thread.start()
        threads.append(thread)
    # wait for all threads to finish
    for thread in threads:
        thread.join()