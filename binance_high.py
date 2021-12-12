import binance_f
import time
import json
import pandas as pd
import datetime
import traceback
from threading import Thread, active_count
import smtplib
from email.mime.text import MIMEText
from binancetrader import binanceTrader


def send_mail(sender, password, receivers, subject, mail_content):
    sender = sender
    password = password
    receivers = receivers
    subject = subject

    def mail():
        ret = True
        try:
            msg = MIMEText(mail_content, 'plain', 'utf-8')
            msg['From'] = ''
            msg['To'] = f'{receivers[0]}'
            msg['Subject'] = subject
            server = smtplib.SMTP_SSL('smtp.163.com', 465)
            server.login(sender, password)
            server.sendmail(sender, receivers, msg.as_string())
            server.quit()
        except Exception:
            ret = False
            traceback.print_exc()
        return ret
    ret = mail()
    if ret:
        print("Email sent")
        return ret
    else:
        print("Fail to send email")


def login(api_key, secret_key):
    client = binance_f.RequestClient(api_key=api_key, secret_key=secret_key)
    print(str(datetime.datetime.now())[:19])
    print('登录成功！')
    return client


def save_as_json(dictionary, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(json.dumps(dictionary))


def load_from_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.loads(f.read())


def log(filename, content):
    dt = str(datetime.datetime.now())[:19]
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(dt + '\n' + content + '\n\n')


def create_dataframe(li):
    return pd.DataFrame([i.__dict__ for i in li])


def get_price_dict(symbols, client, n_day_high, n_day_new):
    high_dict = {}
    print(str(datetime.datetime.now())[:19])
    print(f'正在获取全币种{n_day_high}天最高价……')
    for symbol in symbols:
        hpThread(symbol, high_dict, client, n_day_high, n_day_new).start()
        time.sleep(0.1)
    # while active_count() > 6:
    while active_count() > 1:
        time.sleep(3)
    print(str(datetime.datetime.now())[:19])
    print(f'全币种{n_day_high}天最高价获取成功！')
    save_as_json(high_dict, 'binance_alltime_high.json')


def update_holding_records(client):
    positions = create_dataframe(client.get_position())
    holdings = list(positions[positions['positionAmt']!=0]['symbol'])
    return holdings


class hpThread(Thread):
    def __init__(self, symbol, high_dict, client, n_day_high, n_day_new):
        Thread.__init__(self)
        self.symbol = symbol
        self.high_dict = high_dict
        self.client = client
        self.n_day_high = n_day_high
        self.n_day_new = n_day_new

    def run(self):
        try:
            endTime = int(time.time() * 1000)
            startTime = endTime - 60*60*24*(self.n_day_high+1)*1000
            df = create_dataframe(self.client.get_candlestick_data(
                self.symbol, '1d', startTime, endTime, limit=1500))
            if len(df) >= self.n_day_new:  #只记录历史数据大于等于n_day_new天的币种
                df = df.iloc[1:, :]  # 去掉第一天的数据
                df.high = [float(i) for i in df.high]
                high = df.high.max()
                self.high_dict[self.symbol] = high
        except:
            traceback.print_exc()

if __name__ == '__main__':
    # 参数设置
    config_dict = load_from_json('binance_high参数配置.txt')
    api_key = config_dict['API_KEY']
    secret_key = config_dict['SECRET_KEY']
    dual_direction = config_dict['是否双向持仓（Y/N）']
    buy_value = config_dict['买入金额']
    leverage = config_dict['杠杆倍数']
    stop_percentage = config_dict['止损百分比']
    activation_percentage = config_dict['追踪止损激活百分比']
    callback_rate = config_dict['追踪止损回调比例']
    interval = config_dict['监控频率（秒）']
    n_day_high = config_dict['N日最高价']
    n_day_new= config_dict['至少上市N日']
    email_sender = config_dict['发邮件邮箱']
    email_password = config_dict['发邮件邮箱密码']
    email_receivers = config_dict['收件人邮箱']

    # 登录
    client = login(api_key=api_key, secret_key=secret_key)

    lk = client.start_user_data_stream()
    print('参数配置：')
    print('是否双向持仓（Y/N）', dual_direction)
    print('买入金额', buy_value)
    print('杠杆倍数', leverage)
    print('止损百分比', stop_percentage)
    print('追踪止损激活百分比', activation_percentage)
    print('追踪止损回调比例', callback_rate)
    print('监控频率（秒）', interval)
    print('N日最高价', n_day_high)
    print('至少上市N日', n_day_new)

    # 创建下单专用对象
    trader = binanceTrader(api_key, secret_key)

    # 获取全部交易对
    prices_24h = create_dataframe(client.get_ticker_price_change_statistics())
    symbols = prices_24h.symbol

    # 获取交易信息
    exchange_info = create_dataframe(
        client.get_exchange_information().__dict__['symbols'])

    # 获取全部交易对历史高价
    get_price_dict(symbols, client, n_day_high, n_day_new)

    # 设置循环次数计数
    count = 0
    count_2 = 1

    # 初始化错误信息
    last_error_info = ''

    # 初始化下单记录
    holding_records = update_holding_records(client)

    # 确认单/双向持仓是否正确填写
    if dual_direction == 'Y':
        position_side = 'LONG'
    else:
        position_side = 'BOTH'


    # 循环监控
    while True:
        try:
            # 读取历史低价字典
            high_dict = load_from_json('binance_alltime_high.json')
            # 获取全币种最新价格
            latest_prices = create_dataframe(client.get_symbol_price_ticker())
            # 交易名单
            buy_list = []
            for index, row in latest_prices.iterrows():
                if row.symbol in high_dict.keys():
                    # 如果价格高于史高，加入交易名单，同时更新历史高价字典
                    if row.price > high_dict[row.symbol]:
                        buy_list.append(row.symbol)
                        high_dict[row.symbol] = float(row.price)
            if len(buy_list) > 0:
                for symbol in buy_list:
                    # 排除无历史最高价记录的币种和已经有持仓的币种，并进入下单流程
                    if symbol in high_dict.keys() and symbol not in holding_records:
                        # symbol = 'CTKUSDT'
                        # 设置为leverage倍杠杆
                        client.change_initial_leverage(symbol, leverage)
                        # 设置该symbol下单精度
                        quantity_precision = int(
                            exchange_info[exchange_info['symbol'] == symbol]['quantityPrecision'])
                        price_precision = int(
                            exchange_info[exchange_info['symbol'] == symbol]['pricePrecision'])
                        # 确定交易价格和数量
                        buy_price = float(latest_prices[latest_prices['symbol']
                                                         == symbol]['price'])
                        buy_quantity = round(
                            float(buy_value / buy_price), quantity_precision)
                        # 市价止损单
                        stop_price = round(
                            buy_price * (1 - stop_percentage), price_precision)
                        response = trader.place_order(symbol, 'SELL', position_side, buy_price, buy_quantity,
                                                      order_type='STOP_MARKET', stop_price=stop_price,
                                                      close_position=True)
                        if 'orderId' not in response.keys():
                            error = f'{symbol}下单失败！'
                            print(error)
                            print(response)
                            # 记录订单失败原因
                            log('binance_high错误日志.txt', error +
                                '\n' + json.dumps(response))
                        # 下单成功，才继续下单
                        else:
                            print(str(datetime.datetime.now())[:19])
                            log_content = f'设置市价止损{symbol}，触发价格{stop_price}'
                            print(log_content)
                            log('binance_high下单记录.txt', log_content)
                            # 市价跟踪止损单
                            activation_price = round(
                                buy_price * (1 + activation_percentage), price_precision)
                            if dual_direction == 'Y':
                                response = trader.place_order(symbol, 'SELL', position_side, buy_price, buy_quantity,
                                                              order_type='TRAILING_STOP_MARKET',
                                                              activation_price=activation_price,
                                                              callback_rate=callback_rate*100)
                            elif dual_direction == 'N':
                                response = trader.place_order(symbol, 'SELL', position_side, buy_price, buy_quantity,
                                                              order_type='TRAILING_STOP_MARKET',
                                                              activation_price=activation_price,
                                                              callback_rate=callback_rate*100,
                                                              reduce_only=True)
                            if 'orderId' not in response.keys():
                                error = f'{symbol}下单失败！'
                                print(error)
                                print(response)
                                # 记录订单失败原因
                                log('binance_high错误日志.txt', error +
                                    '\n' + json.dumps(response))
                            # 下单成功，才继续下单
                            else:
                                print(str(datetime.datetime.now())[:19])
                                log_content = f'设置市价跟踪止损{symbol}，触发价格{activation_price}，回调比例{callback_rate}'
                                print(log_content)
                                log('binance_high下单记录.txt', log_content)
                                # 市价买单
                                response = trader.place_order(symbol, 'BUY', position_side, buy_price,
                                                   buy_quantity, order_type='MARKET')
                                if 'orderId' not in response.keys():
                                    error = f'{symbol}下单失败！'
                                    print(error)
                                    print(response)
                                    # 记录订单失败原因
                                    log('binance_high错误日志.txt', error +
                                        '\n' + json.dumps(response))
                                else:
                                    holding_records.append(symbol)
                                    print(str(datetime.datetime.now())[:19])
                                    log_content = f'市价买入{symbol}，价格{buy_price}，数量{buy_quantity}'
                                    print(log_content)
                                    log('binance_high下单记录.txt', log_content)
                                    mail_content = str(datetime.datetime.now())[
                                        :19] + f'  已下单{symbol}'
                                    email_subject = f'已下单{symbol}'
                                    send_mail(email_sender, email_password, email_receivers,
                                              email_subject, mail_content)
                    else:
                        print(str(datetime.datetime.now())[:19])
                        print(f'{symbol}该品种已有持仓或上市未满180天')
                save_as_json(high_dict, 'binance_alltime_high.json')
            else:
                # 无历史新高情况下，每300次监控输出一次，主要用来检测程序是否正在运行
                if count % 300 == 0:
                    print(str(datetime.datetime.now())[:19])
                    print(f'无{n_day_high}天新高')
            # 每300次监控，更新一遍high_dict和holding_records
            if count_2 % 300 == 0:
                prices_24h = create_dataframe(client.get_ticker_price_change_statistics())
                symbols = prices_24h.symbol
                get_price_dict(symbols, client, n_day_high, n_day_new)
                holding_records = update_holding_records(client)
            time.sleep(interval)
            count += 1
            count_2 += 1
        except:
            error_info = traceback.format_exc()
            # 如果报错信息跟上次不一样，打印并记录
            if error_info != last_error_info:
                traceback.print_exc()
                log('binance_high错误日志.txt', error_info)
                last_error_info = error_info
