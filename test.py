import akshare as ak
df = ak.stock_zh_a_hist(symbol="832876", period="monthly", adjust="qfq")
print(df.head())
