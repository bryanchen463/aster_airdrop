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

symbols = ["BTCUSDT", "ETHUSDT", "ASTERUSDT", "ASTERUSDT", "ASTERUSDT"]

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

logger = get_logger("aster_init")

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

def run(key, secret, proxy, cost_per_day):
    proxies = { 'https': proxy }
    client = Client(key, secret,base_url="https://fapi.asterdex.com", proxies=proxies)
    cost = calc_cost(client, key, cost_per_day)
    logger.info(f"{key} cost: {cost}")
    for symbol in symbols:
        response = client.change_leverage(symbol=symbol, leverage=10)
        logger.info(f"{key} {response}")

    try:
        client.change_multi_asset_mode(multiAssetsMargin=True)
    except Exception as e:
        logger.error(f"{key} {e}")
    


def thread_function(key, secret, proxy, cost_per_day):
    try:
        logger.info(f"start run {key} {proxy} {cost_per_day}")
        run(key, secret, proxy, cost_per_day)
    except Exception as e:
        print(f"Caught exception: {e} key: {key} proxy:{proxy}")
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