
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

logger = get_logger("aster_balance")

def run(key, secret, proxy, cost_per_day):
    proxies = { 'https': proxy }
    client = Client(key, secret,base_url="https://fapi.asterdex.com", proxies=proxies)
    account = client.account()
    logger.info(f"{key}: account: {account}")
    

def thread_function(key, secret, proxy, cost_per_day):
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