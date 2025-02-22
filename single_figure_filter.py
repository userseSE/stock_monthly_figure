import akshare as ak
import pandas as pd
import numpy as np
import mplfinance as mpf

# 获取所有 A 股股票列表
stock_list = ak.stock_info_a_code_name()
cnt = 0
def is_monthly_platform(stock_code, cnt):
    """
    判断某支股票是否符合"月线平台"形态
    :param stock_code: 股票代码（如 '600519'）
    :return: 是否符合形态（True / False）
    """
    try:
        # 获取月线数据
        df = ak.stock_zh_a_hist(symbol=stock_code, period='monthly', start_date='20240201', adjust='qfq')
        if df.empty or len(df) < 12:
            return False  # 数据不足 12 个月

        # 数据预处理
        df['date'] = pd.to_datetime(df['日期'])
        df.set_index('date', inplace=True)
        df.sort_index(ascending=False, inplace=True)  # 按日期降序
        globals()['cnt'] += 1
        print(f"{globals()['cnt']} 数据预处理完成")
         # Return the incremented count instead of modifying global

        # 1~6 个月内激涨期（75% 涨幅）
        for i in range(1, 10):
            period = df.iloc[:i]
            low_price = period['最低'].min()
            high_price = period['最高'].max()
            increase = (high_price - low_price) / low_price

            if increase >= 0.75:
                platform_start = i  # 记录激涨期结束的位置
                print(f"{stock_code} 出现激涨期")
                break
        else:
            return False  # 没有符合条件的激涨期

        # 3~6 个月的横盘整理
        for j in range(3, 7):
            platform_period = df.iloc[platform_start:platform_start + j]
            if len(platform_period) < j:
                break
            platform_low = platform_period['最低'].min()
            avg_price = (high_price + low_price) / 2
            if platform_low >= avg_price:
                print(f"{stock_code} 符合月线平台形态")
                return True  # 满足条件
        return False
    except Exception as e:
        print(f"处理 {stock_code} 时出错: {e}")
        return False

def scan_stocks(cnt):
    """
    扫描所有 A 股股票，筛选符合月线平台形态的股票
    """
    platform_stocks = []

    for _, row in stock_list.iterrows():
        stock_code = row['code']
        name = row['name']
        if is_monthly_platform(stock_code, cnt):
            platform_stocks.append((stock_code, name))

    # 打印筛选出的股票
    print("符合月线平台形态的股票：")
    for code, name in platform_stocks:
        print(f"{code}: {name}")

    return platform_stocks

def plot_stock_kline(stock_code):
    """
    绘制股票的月线 K 线图
    """
    df = ak.stock_zh_a_hist(symbol=stock_code, period='monthly', start_date='20150101', adjust='qfq')
    if df.empty:
        print(f"无法获取股票 {stock_code} 的数据")
        return

    # 数据预处理
    df['date'] = pd.to_datetime(df['日期'])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    # 绘制 K 线图
    mpf.plot(df[['开盘', '最高', '最低', '收盘']], type='candle', style='charles',
             title=f'{stock_code} 月线 K 线图',
             ylabel='价格',
             volume=df['成交量'])

# 执行筛选
selected_stocks = scan_stocks(cnt)

# 可视化第一支符合条件的股票
if selected_stocks:
    plot_stock_kline(selected_stocks[0][0])
