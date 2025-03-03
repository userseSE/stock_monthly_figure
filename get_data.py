import akshare as ak
import pandas as pd
import os
import time
import sqlite3

# 设定存储方式（选择 "csv" 或 "sqlite"）
STORAGE_TYPE = "sqlite"  # 可选 "csv" 或 "sqlite"

# 设定数据存储目录（如果选择 CSV 方式）
DATA_DIR = "./stock_data/"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 设定 SQLite 数据库文件（如果选择 SQLite 方式）
DB_FILE = "stock_data.db"

# 获取所有 A 股股票列表
stock_list = ak.stock_info_a_code_name()
print(f"总共有 {len(stock_list)} 只 A 股股票")

# 连接 SQLite 数据库（如果选择 SQLite）
if STORAGE_TYPE == "sqlite":
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_kline (
        stock_code TEXT,
        date TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (stock_code, date)
    )""")
    conn.commit()

# 批量下载月K线数据
for i, stock_code in enumerate(stock_list["code"]):
    print(f"正在下载 {stock_code} ({i+1}/{len(stock_list)})")

    try:
        # 获取该股票的月K线数据
        df = ak.stock_zh_a_hist(symbol=stock_code, period="monthly", adjust="qfq")

        # **动态获取列名**（确保适配不同股票的数据格式）
        expected_columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量"]
        actual_columns = [col for col in expected_columns if col in df.columns]

        if len(actual_columns) < len(expected_columns):
            print(f"⚠️ {stock_code} 数据列不完整，跳过")
            continue  # 跳过异常股票，避免报错

        df = df[actual_columns]  # 只保留实际存在的列

        # 统一列名
        rename_dict = {"日期": "date", "开盘": "open", "收盘": "close",
                       "最高": "high", "最低": "low", "成交量": "volume"}
        df.rename(columns=rename_dict, inplace=True)

        # 添加股票代码列
        df["stock_code"] = stock_code

        # 存储数据
        if STORAGE_TYPE == "csv":
            csv_path = os.path.join(DATA_DIR, f"{stock_code}.csv")
            df.to_csv(csv_path, index=False)
        elif STORAGE_TYPE == "sqlite":
            df.to_sql("stock_kline", conn, if_exists="append", index=False)

        time.sleep(0.5)  # 避免请求过快被封
    except Exception as e:
        print(f"⚠️ 下载 {stock_code} 失败: {e}")

# 关闭数据库连接
if STORAGE_TYPE == "sqlite":
    conn.close()

print("✅ 数据下载完成！")
