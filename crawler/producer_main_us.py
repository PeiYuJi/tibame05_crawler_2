from crawler.tasks_etf_list_us import etf_list_us
from crawler.tasks_crawler_etf_us import crawler_etf_us
from crawler.tasks_backtest_utils_us import backtest_utils_us
from crawler.tasks_crawler_etf_dps_us import crawler_etf_dps_us

if __name__ == "__main__":
    us_etf_url = "https://tw.tradingview.com/markets/etfs/funds-usa/"

    print("ETF 清單")
    etf_list_result = etf_list_us(crawler_url=us_etf_url)

    print("配息資料")
    crawler_etf_dps_us(etf_list_df=etf_list_result)
    
    print("歷史價格")
    crawler_etf_us(etf_list_df=etf_list_result)

    print("歷史價格、技術指標與績效分析")
    backtest_utils_us(etf_list_df=etf_list_result)