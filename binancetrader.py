import time
import hmac
import json
import pandas as pd
from requests import Request, Session
import requests
import datetime
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import traceback
import urllib


class binanceTrader:

    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    def request_authenticated(self, url, method='GET', body=None):  # 带身份验证的请求
        ts = int(time.time() * 1000)
        if body is not None:
            body_encoded = urllib.parse.urlencode(body)
            params = body_encoded + \
                '&recvWindow=5000&timestamp={}'.format(ts)
        else:
            params = '&recvWindow=5000&timestamp={}'.format(ts)
        signature = hmac.new(
            self.secret_key.encode(), params.encode(), 'sha256').hexdigest()
        url_final = url + '?' + params + '&signature={}'.format(signature)
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        if method == 'GET':
            # r = json.loads(requests.get(
            #     url_final, headers=headers, proxies=self.proxies).text)
            r = json.loads(requests.get(url_final, headers=headers).text)
        if method == 'POST':
            # r = json.loads(requests.post(
            #     url_final, headers=headers, proxies=self.proxies).text)
            r = json.loads(requests.post(url_final, headers=headers).text)
        if method == 'DELETE':
            # r = json.loads(requests.delete(
            #     url_final, headers=headers, proxies=self.proxies).text)
            r = json.loads(requests.delete(url_final, headers=headers).text)
        return r

    def request_get(self, url):  # 不带身份验证的请求
        # r = json.loads(requests.get(url, proxies=self.proxies, timeout=5).text)
        r = json.loads(requests.get(url, timeout=5).text)
        return r

    def get_latest_prices(self):  # 获取全品种即时报价
        '''

        响应：
        [
            {
                "symbol": "BTCUSDT",    // 交易对
                "price": "6000.01",     // 价格
                "time": 1589437530011   // 撮合引擎时间
            }
        ]

        '''
        return pd.DataFrame(self.request_get("https://fapi.binance.com/fapi/v1/ticker/price"))

    def get_historical_price(self, symbol, interval, start_time, end_time):  # 获取单个品种K线数据
        '''

        :param symbol: 合约名称，例如：'BTCUSDT'
        :param interval: K线周期，可选：['1m', '3m', '5m', '15m', '30m',
                                  '1h', '2h', '4h', '6h', '8h',
                                  '12h', '1d', '3d', '1w', '1M']
        :param start_time: 起始时间，例如：'2021-11-26'
        :param end_time: 结束时间，例如：'2021-11-26'
        :return: K线数据表，为DataFrame格式

        '''
        # start_time = int(time.mktime(
        #     time.strptime(start_time, '%Y-%m-%d')) * 1000)
        # end_time = int(time.mktime(time.strptime(end_time, '%Y-%m-%d')) * 1000)
        url = 'https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&'\
            'startTime={}&endTime={}&limit=1500'.format(
                symbol, interval, start_time, end_time
            )
        columns = ['openTime', 'open', 'high', 'low', 'close',
                   'volume', 'closeTime', 'volumeUsd', 'trades', 'buyVolume',
                   'buyVolumeUSD', 'redundant']
        df = pd.DataFrame(self.request_get(url), columns=columns)
        df['openDatetime'] = [time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(i/1000)) for i in df['openTime']]
        df['closeDatetime'] = [time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(i/1000)) for i in df['closeTime']]
        return df

    def place_order(self, symbol, side, position_side, price, quantity,
                    order_type="LIMIT", reduce_only=False, stop_price=None, close_position=False,
                    activation_price=None, callback_rate=None):  # 下单
        '''

        :param symbol: 交易对
        :param side: 买卖方向 SELL, BUY
        :param position_side: 持仓方向，单向持仓模式下非必填，默认且仅可填BOTH;在双向持仓模式下必填,且仅可选择 LONG 或 SHORT
        :param price: 委托价格
        :param quantity: 下单数量,使用closePosition不支持此参数。
        :param order_type: 订单类型 LIMIT, MARKET, STOP, TAKE_PROFIT, STOP_MARKET, TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET
        :param reduce_only: true, false; 非双开模式下默认false；双开模式下不接受此参数； 使用closePosition不支持此参数。
        :param stop_price: 触发价, 仅 STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET 需要此参数
        :param close_position: true, false；触发后全部平仓，仅支持STOP_MARKET和TAKE_PROFIT_MARKET；不与quantity合用；自带只平仓效果，不与reduceOnly 合用
        :param activation_price: 追踪止损激活价格，仅TRAILING_STOP_MARKET 需要此参数, 默认为下单当前市场价格(支持不同workingType)
        :param callback_rate: 追踪止损回调比例，可取值范围[0.1, 5],其中 1代表1% ,仅TRAILING_STOP_MARKET 需要此参数
        :return: 订单信息

        响应：
        {
            "clientOrderId": "testOrder", // 用户自定义的订单号
            "cumQty": "0",
            "cumQuote": "0", // 成交金额
            "executedQty": "0", // 成交量
            "orderId": 22542179, // 系统订单号
            "avgPrice": "0.00000",  // 平均成交价
            "origQty": "10", // 原始委托数量
            "price": "0", // 委托价格
            "reduceOnly": false, // 仅减仓
            "side": "SELL", // 买卖方向
            "positionSide": "SHORT", // 持仓方向
            "status": "NEW", // 订单状态
            "stopPrice": "0", // 触发价，对`TRAILING_STOP_MARKET`无效
            "closePosition": false,   // 是否条件全平仓
            "symbol": "BTCUSDT", // 交易对
            "timeInForce": "GTC", // 有效方法
            "type": "TRAILING_STOP_MARKET", // 订单类型
            "origType": "TRAILING_STOP_MARKET",  // 触发前订单类型
            "activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "priceRate": "0.3", // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "updateTime": 1566818724722, // 更新时间
            "workingType": "CONTRACT_PRICE", // 条件价格触发类型
            "priceProtect": false            // 是否开启条件单触发保护
        }

        '''
        url = 'https://fapi.binance.com/fapi/v1/order/'
        if order_type == 'LIMIT':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "price": price,
            }
        if order_type == 'MARKET':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
            }
        if order_type == 'TAKE_PROFIT':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "price": price,
                "stopPrice": stop_price,
            }
        if order_type == 'TAKE_PROFIT_MARKET':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "stopPrice": stop_price,
            }
        if order_type == 'STOP':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "price": price,
                "stopPrice": stop_price,
            }
        if order_type == 'STOP_MARKET':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "stopPrice": stop_price,
            }
        if order_type == 'TRAILING_STOP_MARKET':
            order = {
                "symbol": symbol,
                "side": side,
                "positionSide": position_side,
                "type": order_type,
                "quantity": quantity,
                "price": price,
                "activationPrice": activation_price,
                "closePosition": close_position,
                "callbackRate": callback_rate,
            }
        if reduce_only:
            order['reduceOnly'] = True
        elif close_position and order_type in ['STOP_MARKET', 'TAKE_PROFIT_MARKET']:
            order['closePosition'] = True
            order.pop('quantity')
        order = self.request_authenticated(url, 'POST', order)
        return order

    def cancel_all_orders(self, symbol):  # 取消某合约所有订单
        '''

        响应：
        {
            "code": "200",
            "msg": "The operation of cancel all open order is done."
        }

        '''
        url = 'https://fapi.binance.com/fapi/v1/allOpenOrders'
        body = {'symbol': symbol}
        return self.request_authenticated(url, 'DELETE', body=body)

    def get_all_open_orders(self):  # 查看当前全部挂单
        '''

        响应：
        [
          {
            "avgPrice": "0.00000",              // 平均成交价
            "clientOrderId": "abc",             // 用户自定义的订单号
            "cumQuote": "0",                        // 成交金额
            "executedQty": "0",                 // 成交量
            "orderId": 1917641,                 // 系统订单号
            "origQty": "0.40",                  // 原始委托数量
            "origType": "TRAILING_STOP_MARKET", // 触发前订单类型
            "price": "0",                   // 委托价格
            "reduceOnly": false,                // 是否仅减仓
            "side": "BUY",                      // 买卖方向
            "positionSide": "SHORT", // 持仓方向
            "status": "NEW",                    // 订单状态
            "stopPrice": "9300",                    // 触发价，对`TRAILING_STOP_MARKET`无效
            "closePosition": false,   // 是否条件全平仓
            "symbol": "BTCUSDT",                // 交易对
            "time": 1579276756075,              // 订单时间
            "timeInForce": "GTC",               // 有效方法
            "type": "TRAILING_STOP_MARKET",     // 订单类型
            "activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "priceRate": "0.3", // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "updateTime": 1579276756075,        // 更新时间
            "workingType": "CONTRACT_PRICE", // 条件价格触发类型
            "priceProtect": false            // 是否开启条件单触发保护
          }
        ]

        '''
        url = 'https://fapi.binance.com/fapi/v1/openOrders'
        return self.request_authenticated(url)

    def get_historical_orders(self, symbol):  # 查询所有订单(包括历史订单)
        '''

        响应：
        [
          {
            "avgPrice": "0.00000",              // 平均成交价
            "clientOrderId": "abc",             // 用户自定义的订单号
            "cumQuote": "0",                        // 成交金额
            "executedQty": "0",                 // 成交量
            "orderId": 1917641,                 // 系统订单号
            "origQty": "0.40",                  // 原始委托数量
            "origType": "TRAILING_STOP_MARKET", // 触发前订单类型
            "price": "0",                   // 委托价格
            "reduceOnly": false,                // 是否仅减仓
            "side": "BUY",                      // 买卖方向
            "positionSide": "SHORT", // 持仓方向
            "status": "NEW",                    // 订单状态
            "stopPrice": "9300",                    // 触发价，对`TRAILING_STOP_MARKET`无效
            "closePosition": false,             // 是否条件全平仓
            "symbol": "BTCUSDT",                // 交易对
            "time": 1579276756075,              // 订单时间
            "timeInForce": "GTC",               // 有效方法
            "type": "TRAILING_STOP_MARKET",     // 订单类型
            "activatePrice": "9020", // 跟踪止损激活价格, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "priceRate": "0.3", // 跟踪止损回调比例, 仅`TRAILING_STOP_MARKET` 订单返回此字段
            "updateTime": 1579276756075,        // 更新时间
            "workingType": "CONTRACT_PRICE", // 条件价格触发类型
            "priceProtect": false            // 是否开启条件单触发保护
          }
        ]

        '''
        url = 'https://fapi.binance.com/fapi/v1/allOrders'
        body = {'symbol': symbol}
        return self.request_authenticated(url, body=body)

    def get_trades(self, symbol):  # 账户成交历史
        '''

        响应：
        [
          {
            "buyer": false, // 是否是买方
            "commission": "-0.07819010", // 手续费
            "commissionAsset": "USDT", // 手续费计价单位
            "id": 698759,   // 交易ID
            "maker": false, // 是否是挂单方
            "orderId": 25851813, // 订单编号
            "price": "7819.01", // 成交价
            "qty": "0.002", // 成交量
            "quoteQty": "15.63802", // 成交额
            "realizedPnl": "-0.91539999",   // 实现盈亏
            "side": "SELL", // 买卖方向
            "positionSide": "SHORT",  // 持仓方向
            "symbol": "BTCUSDT", // 交易对
            "time": 1569514978020 // 时间
          }
        ]

        '''
        url = 'https://fapi.binance.com/fapi/v1/userTrades'
        body = {'symbol': symbol}
        return self.request_authenticated(url, body=body)

    def get_account(self):  # 账户信息
        '''

        响应:
        {
            "feeTier": 0,  // 手续费等级
            "canTrade": true,  // 是否可以交易
            "canDeposit": true,  // 是否可以入金
            "canWithdraw": true, // 是否可以出金
            "updateTime": 0,
            "totalInitialMargin": "0.00000000",  // 但前所需起始保证金总额(存在逐仓请忽略), 仅计算usdt资产
            "totalMaintMargin": "0.00000000",  // 维持保证金总额, 仅计算usdt资产
            "totalWalletBalance": "23.72469206",   // 账户总余额, 仅计算usdt资产
            "totalUnrealizedProfit": "0.00000000",  // 持仓未实现盈亏总额, 仅计算usdt资产
            "totalMarginBalance": "23.72469206",  // 保证金总余额, 仅计算usdt资产
            "totalPositionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格), 仅计算usdt资产
            "totalOpenOrderInitialMargin": "0.00000000",  // 当前挂单所需起始保证金(基于最新标记价格), 仅计算usdt资产
            "totalCrossWalletBalance": "23.72469206",  // 全仓账户余额, 仅计算usdt资产
            "totalCrossUnPnl": "0.00000000",    // 全仓持仓未实现盈亏总额, 仅计算usdt资产
            "availableBalance": "23.72469206",       // 可用余额, 仅计算usdt资产
            "maxWithdrawAmount": "23.72469206"     // 最大可转出余额, 仅计算usdt资产
            "assets": [
                {
                    "asset": "USDT",        //资产
                    "walletBalance": "23.72469206",  //余额
                    "unrealizedProfit": "0.00000000",  // 未实现盈亏
                    "marginBalance": "23.72469206",  // 保证金余额
                    "maintMargin": "0.00000000",    // 维持保证金
                    "initialMargin": "0.00000000",  // 当前所需起始保证金
                    "positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
                    "openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
                    "crossWalletBalance": "23.72469206",  //全仓账户余额
                    "crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
                    "availableBalance": "23.72469206",       // 可用余额
                    "maxWithdrawAmount": "23.72469206",     // 最大可转出余额
                    "marginAvailable": true,   // 是否可用作联合保证金
                    "updateTime": 1625474304765  //更新时间
                },
                {
                    "asset": "BUSD",        //资产
                    "walletBalance": "103.12345678",  //余额
                    "unrealizedProfit": "0.00000000",  // 未实现盈亏
                    "marginBalance": "103.12345678",  // 保证金余额
                    "maintMargin": "0.00000000",    // 维持保证金
                    "initialMargin": "0.00000000",  // 当前所需起始保证金
                    "positionInitialMargin": "0.00000000",  // 持仓所需起始保证金(基于最新标记价格)
                    "openOrderInitialMargin": "0.00000000", // 当前挂单所需起始保证金(基于最新标记价格)
                    "crossWalletBalance": "103.12345678",  //全仓账户余额
                    "crossUnPnl": "0.00000000" // 全仓持仓未实现盈亏
                    "availableBalance": "103.12345678",       // 可用余额
                    "maxWithdrawAmount": "103.12345678",     // 最大可转出余额
                    "marginAvailable": true,   // 否可用作联合保证金
                    "updateTime": 0  // 更新时间
                   }
            ],
            "positions": [  // 头寸，将返回所有市场symbol。
                //根据用户持仓模式展示持仓方向，即单向模式下只返回BOTH持仓情况，双向模式下只返回 LONG 和 SHORT 持仓情况
                {
                    "symbol": "BTCUSDT",  // 交易对
                    "initialMargin": "0",   // 当前所需起始保证金(基于最新标记价格)
                    "maintMargin": "0", //维持保证金
                    "unrealizedProfit": "0.00000000",  // 持仓未实现盈亏
                    "positionInitialMargin": "0",  // 持仓所需起始保证金(基于最新标记价格)
                    "openOrderInitialMargin": "0",  // 当前挂单所需起始保证金(基于最新标记价格)
                    "leverage": "100",  // 杠杆倍率
                    "isolated": true,  // 是否是逐仓模式
                    "entryPrice": "0.00000",  // 持仓成本价
                    "maxNotional": "250000",  // 当前杠杆下用户可用的最大名义价值
                    "positionSide": "BOTH",  // 持仓方向
                    "positionAmt": "0",      // 持仓数量
                    "updateTime": 0         // 更新时间
                }
            ]
        }

        '''
        url = 'https://fapi.binance.com/fapi/v2/account'
        return self.request_authenticated(url)

    def get_balance(self):  # 账户余额
        '''

        响应:
        [
            {
                "accountAlias": "SgsR",    // 账户唯一识别码
                "asset": "USDT",        // 资产
                "balance": "122607.35137903",   // 总余额
                "crossWalletBalance": "23.72469206", // 全仓余额
                "crossUnPnl": "0.00000000"  // 全仓持仓未实现盈亏
                "availableBalance": "23.72469206",       // 下单可用余额
                "maxWithdrawAmount": "23.72469206",     // 最大可转出余额
                "marginAvailable": true,    // 是否可用作联合保证金
                "updateTime": 1617939110373
            }
        ]

        '''
        url = 'https://fapi.binance.com/fapi/v2/balance'
        return self.request_authenticated(url)

def login(api_key, secret_key):
    trader = binanceTrader(api_key=api_key, secret_key=secret_key)
    return trader

