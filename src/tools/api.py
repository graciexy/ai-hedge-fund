import os
import tushare as ts
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
import pandas as pd
from src.data.models import Price, FinancialMetrics, CompanyNews, InsiderTrade

load_dotenv()
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TUSHARE_TOKEN:
    raise ValueError("TUSHARE_TOKEN not found")
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def _normalize_ticker(ticker: str) -> str:
    """处理股票代码：如果传入多个代码（空格分隔），取第一个；并确保带后缀 .SH 或 .SZ"""
    if ' ' in ticker:
        ticker = ticker.split()[0]
    if '.' not in ticker:
        if ticker.startswith('6'):
            ticker = f"{ticker}.SH"
        else:
            ticker = f"{ticker}.SZ"
    return ticker

# ==================== 价格数据 ====================
def get_prices(ticker: str, start_date: str, end_date: str, **kwargs) -> List[Price]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.daily(ts_code=ticker, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
        if df.empty:
            return []
        prices = []
        for _, row in df.iterrows():
            price = Price(
                open=row['open'],
                close=row['close'],
                high=row['high'],
                low=row['low'],
                volume=row['vol'],
                date=row['trade_date']
            )
            prices.append(price)
        prices.sort(key=lambda x: x.date)
        return prices
    except Exception as e:
        print(f"Error in get_prices for {ticker}: {e}")
        return []

# ==================== 财务指标 ====================
def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> FinancialMetrics:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.fina_indicator(ts_code=ticker)
        if df.empty:
            return FinancialMetrics()
        latest = df.iloc[0]
        metrics = FinancialMetrics(
            pe_ratio=latest.get('pe'),
            pb_ratio=latest.get('pb'),
            roe=latest.get('roe'),
            market_cap=latest.get('market_cap')
        )
        return metrics
    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
        return FinancialMetrics()

# ==================== 公司新闻 ====================
def get_company_news(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[CompanyNews]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.news(ts_code=ticker,
                      start_date=start_date.replace('-', '') if start_date else None,
                      end_date=end_date.replace('-', '') if end_date else None)
        if df.empty:
            return []
        news_list = []
        for _, row in df.iterrows():
            news = CompanyNews(
                title=row.get('title', ''),
                content=row.get('content', ''),
                source=row.get('source', ''),
                datetime=row.get('datetime', '')
            )
            news_list.append(news)
        return news_list
    except Exception:
        return []

# ==================== 内幕交易 ====================
def get_insider_trades(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[InsiderTrade]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.disclosure(ts_code=ticker,
                            start_date=start_date.replace('-', '') if start_date else None,
                            end_date=end_date.replace('-', '') if end_date else None)
        if df.empty:
            return []
        trades = []
        for _, row in df.iterrows():
            trade = InsiderTrade(
                transaction_shares=row.get('shares_change'),
                transaction_value=row.get('change_value'),
                transaction_date=row.get('ann_date')
            )
            trades.append(trade)
        return trades
    except Exception:
        return []

# ==================== 公司概况 ====================
def get_company_facts(ticker: str) -> dict:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.stock_basic(ts_code=ticker, fields='name,industry,list_date,market')
        if not df.empty:
            row = df.iloc[0]
            return {
                "name": row.get('name', ticker),
                "industry": row.get('industry', 'Unknown'),
                "list_date": row.get('list_date', ''),
                "market": row.get('market', 'Unknown'),
                "description": f"{row.get('name', ticker)} is listed on {row.get('market', 'China')} market."
            }
    except Exception as e:
        print(f"Error fetching company facts for {ticker}: {e}")
    return {"name": ticker, "industry": "Unknown", "description": "Data not available"}

# ==================== 市值 ====================
def get_market_cap(ticker: str, **kwargs) -> float:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.daily_basic(ts_code=ticker, fields='total_mv')
        if not df.empty:
            return float(df.iloc[0]['total_mv']) * 10000  # 万元转元
    except Exception as e:
        print(f"Error in get_market_cap for {ticker}: {e}")
    return 0.0

# ==================== 资产负债表 ====================
def get_balance_sheet(ticker: str, **kwargs) -> Dict:
    ticker = _normalize_ticker(ticker)
    # 返回空字典，避免程序崩溃；可根据需要扩展
    return {}

# ==================== 现金流量表 ====================
def get_cash_flow(ticker: str, **kwargs) -> Dict:
    ticker = _normalize_ticker(ticker)
    return {}

# ==================== 利润表 ====================
def get_income_statement(ticker: str, **kwargs) -> Dict:
    ticker = _normalize_ticker(ticker)
    return {}

# ==================== 搜索财务报表行项目 ====================
def search_line_items(ticker: str, line_items: List[str], period: str = "ttm", **kwargs) -> List[Dict]:
    """
    搜索财务报表行项目（占位实现）
    原项目用于从财务报表中提取特定指标，这里返回空列表避免报错
    """
    return []

# ==================== 辅助函数 ====================
def prices_to_df(prices: List[Price]) -> pd.DataFrame:
    data = {
        'date': [p.date for p in prices],
        'open': [p.open for p in prices],
        'high': [p.high for p in prices],
        'low': [p.low for p in prices],
        'close': [p.close for p in prices],
        'volume': [p.volume for p in prices],
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df
