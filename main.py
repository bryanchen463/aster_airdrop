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
import os
import gzip

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
random.seed(time.time())

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    def doRollover(self):
        # 调用父类的doRollover方法，进行日志文件滚动
        super().doRollover()
        
        # # 获取滚动后的文件名
        rotated_filename = self.baseFilename + ".1"
        target_filename = self.baseFilename + "." + datetime.now().strftime("%Y%m%d-%H%M%S") + ".gz"
        
        # 压缩滚动后的日志文件
        if os.path.exists(rotated_filename):
            with open(rotated_filename, 'rb') as f_in:
                with gzip.open(target_filename, 'wb') as f_out:
                    f_out.writelines(f_in)
            
            # 删除未压缩的滚动文件
            os.remove(rotated_filename)

def get_logger(name) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.WARN)
    single_file_size = 1 * 1024 * 1024 * 1024 # 1GB
    monitorHandler = CompressedRotatingFileHandler(filename=os.path.join(log_dir, f"{name}.log"), maxBytes=single_file_size, backupCount=5)

    monitorHandler.setLevel(logging.WARN)
    monitorFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    monitorHandler.setFormatter(monitorFormatter)
    logger.addHandler(monitorHandler)
    return logger

logger = get_logger("aster")

def close_position(client: Client):
    positions = client.get_position_risk()
    for position in positions:
        if time.time() * 1000 - position["updateTime"] <= 100:
            continue
        if abs(float(position["notional"])) > 1:
            if position["symbol"] in symbols:
                side = "SELL" if float(position["positionAmt"]) > 0 else "BUY"
                amount = abs(float(position["positionAmt"]))
                response = client.new_order(symbol=position["symbol"], side=side, type="MARKET", quantity=amount, reduceOnly=True)

def get_income_history(client: Client, start_time: int, end_time: int):
    income_history = []
    while True:
        items = client.get_income_history(startTime=start_time, endTime=end_time)
        for item in items:
            if item["symbol"] in symbols:
                if int(item["time"]) < start_time:
                    continue
                income_history.append(item)
        if len(items) == 0:
            break
        start_time = int(items[-1]["time"]) + 1
        time.sleep(0.1)
    return income_history

def is_cost_enough(client: Client, api_key: str, cost_per_day: float):
    # 计算当天整点的时间戳
    start_time = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end_time = int(datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    income_history = get_income_history(client, start_time, end_time)
    # logger.info(f"income_history: {income_history}")
    cost = 0
    for income in income_history:
        if income["symbol"] in symbols:
            item_cost = float(income.get("income", 0))
            cost -= item_cost
    # logger.info(f"{api_key} cost: {cost}")
    return cost >= cost_per_day

def run(key, secret, proxy, cost_per_day):
    proxies = { 'https': proxy }
    client = Client(key, secret,base_url="https://fapi.asterdex.com", proxies=proxies)
    
    market_info = client.exchange_info()
    # logger.info(f"market_info: {market_info}")
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

    while True:
        try:
            sleep_time = random.randint(600, 1200)
            logger.info(f"sleep_time: {sleep_time}")
            if is_cost_enough(client, key, cost_per_day):
                logger.info("cost is enough, not trading")
                close_position(client)
                time.sleep(sleep_time)
                continue
            order_timeout = 1000
            orders = client.get_orders()
            # logger.info(orders)
            if len(orders) > 0:
                for order in orders:
                    # 30s还没有成交修改价格，里面成交
                    logger.info(f"order symbol {order['symbol']} updateTime: {order['updateTime']} time: {time.time()} diff: {time.time() * 1000 - order['updateTime']}")
                    if time.time() * 1000 - order['updateTime'] > order_timeout:
                        response = client.cancel_open_orders(symbol=order['symbol'])
                        logger.info(f"cancel order response: {response}")
                        close_position(client)
                time.sleep(sleep_time)
                continue
            close_position(client)
            response = client.balance(recvWindow=6000)
            # logger.info(response)
            symbol = random.choice(symbols)
            book_ticker = client.book_ticker(symbol)
            logger.info(f"book_ticker: {book_ticker}")
            balances = client.balance()
            net_balance = 0
            for balance in balances:
                if balance["asset"] == "USDT":
                    net_balance = balance["availableBalance"]
            if float(net_balance) < 0.001:
                logger.info("net_balance is less than 0.001, not trading")
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
                logger.info(f"quantity * mid_price < 5, not trading")
                continue
            logger.info(f"symbol: {symbol} quantity: {quantity} price: {mid_price}")
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
                logger.info(f"new order response: {response}")
        except ClientError as error:
            logger.exception(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
            )
        )
        close_position(client)
        time.sleep(sleep_time)



    # time.sleep(30)

    # logger.info("closing ws connection")
    # ws_client.stop()

def thread_function(key, secret, proxy, cost_per_day):
    while True:  # 循环确保线程持续运行
        try:
            logger.info(f"start run {key} {proxy} {cost_per_day}")    
            run(key, secret, proxy, cost_per_day)
        except Exception as e:
            print(f"Caught exception: {e}")
            # 此处可添加错误恢复逻辑（如重试、清理资源等）
        # 异常处理后，循环继续，线程不会终止
        time.sleep(1)  # 模拟后续操作

def init_accounts():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    return config["accounts"]

if __name__ == "__main__":
    accounts = init_accounts()
    threads = []
    for account in accounts:
        # run in parallel
        thread = threading.Thread(target=thread_function, args=(account["key"], account["secret"], account["proxy"], account["cost_per_day"]))
        thread.start()
        threads.append(thread)
    # wait for all threads to finish
    for thread in threads:
        thread.join()