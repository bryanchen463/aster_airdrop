import logging
import logging.handlers
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

symbols = ["ASTERUSDT", "ASTERUSDT", "ASTERUSDT"]
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
    logger.setLevel(logging.INFO)
    logger.propagate = False  # 禁止传播到根日志记录器
    
    # 如果已有处理器则先清除（避免重复添加）
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
    
    # 文件处理器配置保持不变
    single_file_size = 1 * 1024 * 1024 * 1024 # 1GB
    monitorHandler = CompressedRotatingFileHandler(filename=os.path.join(log_dir, f"{name}.log"), maxBytes=single_file_size, backupCount=5)

    monitorHandler.setLevel(logging.INFO)
    monitorFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    monitorHandler.setFormatter(monitorFormatter)
    
    # 移除所有控制台处理器（如果有）
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.StreamHandler):
            root_logger.removeHandler(handler)
            
    logger.addHandler(monitorHandler)
    return logger

logger = get_logger("aster")

def close_position(client: Client):
    positions = client.get_position_risk()
    try:
        for position in positions:
            if time.time() * 1000 - position["updateTime"] <= 100:
                continue
            if abs(float(position["notional"])) > 1:
                side = "SELL" if float(position["positionAmt"]) > 0 else "BUY"
                amount = abs(float(position["positionAmt"]))
                logger.info(f"symbol: {position['symbol']} quantity: {amount} price: {position['entryPrice']}")
                response = client.new_order(symbol=position["symbol"], side=side, type="MARKET", quantity=amount, reduceOnly=True)
            elif abs(float(position["notional"])) > 0:
                logger.info(f"position {position['symbol']} notional: {position['notional']} updateTime: {position['updateTime']}")
    except Exception as e:
        logger.exception(e)

def get_income_history(client: Client, start_time: int, end_time: int):
    income_history = []
    while True:
        items = client.get_income_history(startTime=start_time, endTime=end_time, incomeType="COMMISSION")
        if len(items) == 0:
            break
        income_history.extend(items)
        start_time = int(items[-1]["time"]) + 1
        time.sleep(0.1)
    return income_history

def get_mark_price(mark_price_dict: dict, symbol: str):
    if symbol in mark_price_dict:
        return mark_price_dict[symbol]['markPrice']
    if symbol == "USDTUSDT":
        return 1
    logger.error(f"symbol {symbol} not found in mark_price_dict")
    return 0
    

def calc_cost(client: Client, api_key: str, cost_per_day: float):
    # 计算当天整点的时间戳
    start_time = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end_time = int(datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    income_history = get_income_history(client, start_time, end_time)
    # logger.info(f"income_history: {income_history}")
    cost = 0
    mark_price_dict = {}
    mark_price_info = client.mark_price()
    for mark_price_info in mark_price_info:
        mark_price_dict[mark_price_info['symbol']] = mark_price_info
    for income in income_history:
        if income["incomeType"] != "COMMISSION":
            continue
        symbol = income["asset"]+"USDT"
        mark_price = get_mark_price(mark_price_dict, symbol)
        cost += float(income.get("income", 0)) * float(mark_price)
    # logger.info(f"{api_key} cost: {cost}")
    return cost 

def is_cost_enough(client: Client, api_key: str, cost_per_day: float):
   cost = calc_cost(client, api_key, cost_per_day)
   return abs(cost) >= cost_per_day

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
                time.sleep(10)
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
                time.sleep(10)
                continue
            value = 250
            if float(net_balance) < value:
                value = 20 * float(net_balance) / 2
            # 一笔价值50usdt
            times = random.randint(1, 5)
            min_qty = symbol_limit["min_qty"]
            max_qty = symbol_limit["max_qty"]
            quantity = value/mid_price
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

def init_config():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    # 兼容旧配置
    if "hedge_mode" not in config:
        config["hedge_mode"] = False
    if "dry_run" not in config:
        config["dry_run"] = False
    return config

def create_client(key: str, secret: str, proxy: str) -> Client:
    proxies = { 'https': proxy }
    return Client(key, secret, base_url="https://fapi.asterdex.com", proxies=proxies)

def build_symbol_limits(client: Client):
    market_info = client.exchange_info()
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
    return symbol_limits

def get_net_balance(client: Client, account: dict):
    mark_price_dict = {}
    mark_price_info = client.mark_price()
    net_balance = 0
    for mark_price_info in mark_price_info:
        mark_price_dict[mark_price_info['symbol']] = mark_price_info
    #logger.info(f"mark_price: {mark_price_dict}")
    for asset in account['assets']:
        if abs(float(asset['marginBalance'])) <= 1e-10:
            continue
        symbol = asset['asset'] + "USDT"
        mark_price = 0
        if symbol in mark_price_dict:
            mark_price_info = mark_price_dict[symbol]
            mark_price = mark_price_info['markPrice']
        # logger.info(f"{key}: asset: {asset['asset']} walletBalance: {asset['walletBalance']} marginBalance: {asset['marginBalance']} crossWalletBalance: {asset['crossWalletBalance']} markPrice: {mark_price} asset:{asset}")
        value = float(asset['marginBalance']) * float(mark_price)
        if asset['asset'] == "USDT" or asset['asset'] == "BUSD" or asset['asset'] == "USDC" or asset['asset'] == "USDF":
            value =  float(asset['marginBalance'])
        net_balance += value
    for position in account['positions']:
        if position['notional'] == "0":
            continue
        #logger.info(f"{key}: position: {position['symbol']} positionAmt: {position['positionAmt']} entryPrice: {position['entryPrice']} leverage: {position['leverage']} isolated: {position['isolated']} positionSide: {position['positionSide']} notional: {position['notional']}")
        # net_balance += float(position['notional'])/float(position['leverage'])
    # logger.info(f"{key}: totalWalletBalance: {account['totalWalletBalance']} totalMarginBalance: {account['totalMarginBalance']} totalCrossWalletBalance: {account['totalCrossWalletBalance']} account:{account}")
    return net_balance

def compute_symbol_and_qty(client: Client, symbol_limits: dict):
    symbol = random.choice(symbols)
    book_ticker = client.book_ticker(symbol)
    logger.info(f"book_ticker: {book_ticker}")
    account = client.account()
    net_balance = get_net_balance(client, account)
    if float(net_balance) < 0.001:
        return None, None, None
    symbol_limit = symbol_limits[symbol]
    bid_price = book_ticker["bidPrice"]
    ask_price = book_ticker["askPrice"]
    mid_price = (float(bid_price) + float(ask_price)) / 2
    mid_price = int(mid_price / float(symbol_limit["tick_size"])) * float(symbol_limit["tick_size"])
    mid_price = round(mid_price, symbol_limit["price_precision"])
    if abs(mid_price - float(bid_price)) <= 0.0000000000001 or abs(float(ask_price) - mid_price) <= 0.0000000000001:
        return None, None, None
    value = 250
    times = random.randint(1, 5)
    min_qty = symbol_limit["min_qty"]
    max_qty = symbol_limit["max_qty"]
    quantity = value / mid_price
    quantity = quantity + times * min_qty
    quantity = int(quantity / float(symbol_limit["step_size"])) * float(symbol_limit["step_size"])
    quantity = round(quantity, int(symbol_limit["qty_precision"]))
    if quantity > float(max_qty):
        quantity = float(max_qty)
    if quantity * mid_price < 5:
        return None, None, None
    return symbol, quantity, mid_price

def hedge_run(account_a: dict, account_b: dict, dry_run: bool):
    client_a = create_client(account_a["key"], account_a["secret"], account_a["proxy"])
    client_b = create_client(account_b["key"], account_b["secret"], account_b["proxy"])

    symbol_limits = build_symbol_limits(client_a)

    while True:
        try:
            sleep_time = random.randint(100, 300)
            logger.info(f"sleep_time: {sleep_time}")

            # 成本控制：两个账户都达到阈值则不交易
            enough_a = is_cost_enough(client_a, account_a["key"], account_a.get("cost_per_day", 0))
            enough_b = is_cost_enough(client_b, account_b["key"], account_b.get("cost_per_day", 0))
            if enough_a and enough_b:
                logger.info("cost is enough for both accounts, not trading")
                close_position(client_a)
                close_position(client_b)
                time.sleep(sleep_time)
                continue

            order_timeout = 300 + random.randint(0, 60 * 10)
            # 两边清理超时订单
            for c in (client_a, client_b):
                orders = c.get_orders()
                if len(orders) > 0:
                    for order in orders:
                        logger.info(f"order symbol {order['symbol']} updateTime: {order['updateTime']} diff: {time.time() * 1000 - order['updateTime']}")
                        if time.time() * 1000 - order['updateTime'] > order_timeout:
                            response = c.cancel_open_orders(symbol=order['symbol'])
                            logger.info(f"cancel order response: {response}")
                            close_position(c)
                    time.sleep(10)
                    # 有挂单则等待下次循环
                    continue

            # 平掉残留仓位
            close_position(client_a)
            close_position(client_b)

            symbol, quantity, price = compute_symbol_and_qty(client_a, symbol_limits)
            if symbol is None:
                time.sleep(10)
                continue

            logger.info(f"hedge plan -> symbol: {symbol} qty: {quantity} price: {price}")

            if dry_run:
                logger.info("dry_run enabled, skip placing orders")
            else:
                # A 买，B 卖
                resp_a = client_a.new_order(symbol=symbol, side="BUY", type="LIMIT", quantity=quantity, price=price, timeInForce="GTC")
                logger.info(f"A new order response: {resp_a}")
                resp_b = client_b.new_order(symbol=symbol, side="SELL", type="LIMIT", quantity=quantity, price=price, timeInForce="GTC")
                logger.info(f"B new order response: {resp_b}")

        except ClientError as error:
            logger.exception(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
            )
            )
            close_position(client_a)
            close_position(client_b)
        except Exception as e:
            logger.exception(e)
            close_position(client_a)
            close_position(client_b)

        time.sleep(sleep_time)

if __name__ == "__main__":
    config = init_config()
    accounts = config["accounts"]
    hedge_mode = config.get("hedge_mode", False)
    dry_run = config.get("dry_run", False)

    threads = []
    if hedge_mode and len(accounts) >= 2:
        # 两两成对运行
        for i in range(0, len(accounts) - 1, 2):
            acc_a = accounts[i]
            acc_b = accounts[i + 1]
            thread = threading.Thread(target=hedge_run, args=(acc_a, acc_b, dry_run))
            thread.start()
            threads.append(thread)
        # 如果为奇数，最后一个账户仍按单账户策略
        if len(accounts) % 2 == 1:
            last = accounts[-1]
            thread = threading.Thread(target=thread_function, args=(last["key"], last["secret"], last["proxy"], last.get("cost_per_day", 0)))
            thread.start()
            threads.append(thread)
    else:
        # 兼容原有单账户并行
        for account in accounts:
            thread = threading.Thread(target=thread_function, args=(account["key"], account["secret"], account["proxy"], account["cost_per_day"]))
            thread.start()
            threads.append(thread)

    for thread in threads:
        thread.join()