import pandas as pd
import yfinance as yf
import numpy as np
from crawler.worker import app
from database.main import write_etf_daily_price_to_db

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def crawler_etf_us(etf_list_df):
    
    tickers = [etf["etf_id"] for etf in etf_list_df]    
    start_date = '2015-05-01'
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')

    failed_tickers = []
    all_etf_data = []
    for r in tickers:
        print(f"正在下載：{r}")
        try:
            df = yf.download(r, start=start_date, end=end_date, auto_adjust=False)
            df = df[df["Volume"] > 0].ffill()
            df.reset_index(inplace=True)
            df.rename(columns={
                "Date": "date",
                "Adj Close": "adj_close",
                "Close": "close",
                "High": "high",
                "Low": "low",
                "Open": "open",
                "Volume": "volume"
            }, inplace=True)
            if df.empty:
                raise ValueError("下載結果為空")
        except Exception as e:
            print(f"[⚠️ 錯誤] {r} 下載失敗：{e}")
            failed_tickers.append(r)
            continue
        df.columns = df.columns.droplevel(1)  # 把 'Price' 這層拿掉

        df.insert(0, "etf_id", r)  # 新增一欄「etf_id」
        print (df)
        #df.columns = ["etf_id","date", "dividend_per_unit"]    # 調整欄位名稱
        columns_order = ['etf_id', 'date', 'adj_close','close','high', 'low', 'open','volume']
        df = df[columns_order]

        
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.replace([np.inf, -np.inf], np.nan)
        #df = df.dropna(subset=[
    #'rsi', 'ma5', 'ma20', 'macd_line', 'macd_signal', 'macd_hist',
    #'pct_k', 'pct_d', 'daily_return', 'cumulative_return'])
        all_etf_data.append(df)

    # Step 4️⃣ 合併所有 ETF 成一張總表
    if all_etf_data:
        daily_price_df = pd.concat(all_etf_data, ignore_index=True)
        daily_price_df = daily_price_df[columns_order]
    write_etf_daily_price_to_db(daily_price_df)
    # return df