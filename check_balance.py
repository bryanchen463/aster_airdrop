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
    '''
    {
        "feeTier": 0, // account commisssion tier
        "canTrade": true, // if can trade
        "canDeposit": true, // if can transfer in asset
        "canWithdraw": true, // if can transfer out asset
        "updateTime": 0,
        "totalInitialMargin": "0.00000000", // total initial margin required with current mark price (useless with isolated positions), only for USDT asset
        "totalMaintMargin": "0.00000000", // total maintenance margin required, only for USDT asset
        "totalWalletBalance": "23.72469206", // total wallet balance, using BidRate/AskRate for value caculation under multi-asset mode
        "totalUnrealizedProfit": "0.00000000", // total unrealized profit in USDT
        "totalMarginBalance": "23.72469206", // total margin balance, using BidRate/AskRate for value caculation under multi-asset mode
        "totalPositionInitialMargin": "0.00000000", // initial margin required for positions with current mark price, only for USDT asset
        "totalOpenOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price, only for USDT asset
        "totalCrossWalletBalance": "23.72469206", // crossed wallet balance, using BidRate/AskRate for value caculation under multi-asset mode
        "totalCrossUnPnl": "0.00000000", // unrealized profit of crossed positions in USDT
        "availableBalance": "23.72469206", // available balance, only for USDT asset
        "maxWithdrawAmount": "23.72469206" // maximum amount for transfer out, using BidRate for value caculation under multi-asset mode
        "assets": [
            {
                "asset": "USDT", // asset name
                "walletBalance": "23.72469206", // wallet balance
                "unrealizedProfit": "0.00000000", // unrealized profit
                "marginBalance": "23.72469206", // margin balance
                "maintMargin": "0.00000000", // maintenance margin required
                "initialMargin": "0.00000000", // total initial margin required with current mark price
                "positionInitialMargin": "0.00000000", //initial margin required for positions with current mark price
                "openOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price
                "crossWalletBalance": "23.72469206", // crossed wallet balance
                "crossUnPnl": "0.00000000" // unrealized profit of crossed positions
                "availableBalance": "23.72469206", // available balance
                "maxWithdrawAmount": "23.72469206", // maximum amount for transfer out
                "marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
                "updateTime": 1625474304765 // last update time
            },
            {
                "asset": "BUSD", // asset name
                "walletBalance": "103.12345678", // wallet balance
                "unrealizedProfit": "0.00000000", // unrealized profit
                "marginBalance": "103.12345678", // margin balance
                "maintMargin": "0.00000000", // maintenance margin required
                "initialMargin": "0.00000000", // total initial margin required with current mark price
                "positionInitialMargin": "0.00000000", //initial margin required for positions with current mark price
                "openOrderInitialMargin": "0.00000000", // initial margin required for open orders with current mark price
                "crossWalletBalance": "103.12345678", // crossed wallet balance
                "crossUnPnl": "0.00000000" // unrealized profit of crossed positions
                "availableBalance": "103.12345678", // available balance
                "maxWithdrawAmount": "103.12345678", // maximum amount for transfer out
                "marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
                "updateTime": 1625474304765 // last update time
            }
        ],
        "positions": [ // positions of all symbols in the market are returned
            // only "BOTH" positions will be returned with One-way mode
            // only "LONG" and "SHORT" positions will be returned with Hedge mode
            {
                "symbol": "BTCUSDT", // symbol name
                "initialMargin": "0", // initial margin required with current mark price
                "maintMargin": "0", // maintenance margin required
                "unrealizedProfit": "0.00000000", // unrealized profit
                "positionInitialMargin": "0", // initial margin required for positions with current mark price
                "openOrderInitialMargin": "0", // initial margin required for open orders with current mark price
                "leverage": "100", // current initial leverage
                "isolated": true, // if the position is isolated
                "entryPrice": "0.00000", // average entry price
                "maxNotional": "250000", // maximum available notional with current leverage
                "positionSide": "BOTH", // position side
                "positionAmt": "0", // position amount
                "updateTime": 0 // last update time
            }
        ]
    }
    '''
    # print balance marginBalance and asset position
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
    logger.info(f"{key}: net_balance: {net_balance}")


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