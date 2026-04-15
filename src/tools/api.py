import datetime
import logging
import os
import pandas as pd
import requests
import time
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

from src.data.cache import get_cache
from src.data.models import (
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    Price,
    PriceResponse,
    LineItem,
    LineItemResponse,
    InsiderTrade,
    InsiderTradeResponse,
    CompanyFactsResponse,
)

# Global cache instance
_cache = get_cache()


def _make_api_request(url: str, headers: dict, method: str = "GET", json_data: dict = None, max_retries: int = 3) -> requests.Response:
    """
    Make an API request with rate limiting handling and moderate backoff.
    
    Args:
        url: The URL to request
        headers: Headers to include in the request
        method: HTTP method (GET or POST)
        json_data: JSON data for POST requests
        max_retries: Maximum number of retries (default: 3)
    
    Returns:
        requests.Response: The response object
    
    Raises:
        Exception: If the request fails with a non-429 error
    """
    for attempt in range(max_retries + 1):  # +1 for initial attempt
        if method.upper() == "POST":
            response = requests.post(url, headers=headers, json=json_data)
        else:
            response = requests.get(url, headers=headers)
        
        if response.status_code == 429 and attempt < max_retries:
            # Linear backoff: 60s, 90s, 120s, 150s...
            delay = 60 + (30 * attempt)
            print(f"Rate limited (429). Attempt {attempt + 1}/{max_retries + 1}. Waiting {delay}s before retrying...")
            time.sleep(delay)
            continue
        
        # Return the response (whether success, other errors, or final 429)
        return response


async def get_prices(ticker: str, start_date: str, end_date: str, **kwargs) -> List[Price]:
    # 调用 Tushare 的 daily 接口获取日线行情
    df = pro.daily(ts_code=ticker, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
    
    # 如果获取到的数据为空，直接返回空列表
    if df.empty:
        return []
    
    # 将 Tushare 返回的 DataFrame 数据，转换成项目需要的 Price 对象列表
    prices = []
    for _, row in df.iterrows():
        price = Price(
            open=row['open'],
            close=row['close'],
            high=row['high'],
            low=row['low'],
            volume=row['vol'],  # 注意 Tushare 的成交量字段是 'vol'
            date=row['trade_date']
        )
        prices.append(price)
    
    # 按日期升序排序，让数据从旧到新，方便后续分析
    prices.sort(key=lambda x: x.date)
    return prices

def prices_to_df(prices: List[Price]) -> 'pd.DataFrame':
    """将 Price 对象列表转换为 pandas DataFrame"""
    import pandas as pd
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

async def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> FinancialMetrics:
    """获取财务指标，end_date 参数忽略（Tushare 返回最新数据）"""
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


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
    api_key: str = None,
) -> list[LineItem]:
    """Fetch line items from API."""
    # If not in cache or insufficient data, fetch from API
    headers = {}
    financial_api_key = api_key or os.environ.get("FINANCIAL_DATASETS_API_KEY")
    if financial_api_key:
        headers["X-API-KEY"] = financial_api_key

    url = "https://api.financialdatasets.ai/financials/search/line-items"

    body = {
        "tickers": [ticker],
        "line_items": line_items,
        "end_date": end_date,
        "period": period,
        "limit": limit,
    }
    response = _make_api_request(url, headers, method="POST", json_data=body)
    if response.status_code != 200:
        return []
    
    try:
        data = response.json()
        response_model = LineItemResponse(**data)
        search_results = response_model.search_results
    except Exception as e:
        logger.warning("Failed to parse line items response for %s: %s", ticker, e)
        return []
    if not search_results:
        return []

    # Cache the results
    return search_results[:limit]


async def get_insider_trades(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[InsiderTrade]:
    """获取股东增减持信息"""
    try:
        df = pro.disclosure(ts_code=ticker, start_date=start_date.replace('-', '') if start_date else None,
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



async def get_company_news(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[CompanyNews]:
    """获取公司新闻"""
    # Tushare 新闻接口可能需要积分，若不可用则返回空列表
    try:
        df = pro.news(ts_code=ticker, start_date=start_date.replace('-', '') if start_date else None, 
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

async def get_company_facts(ticker: str) -> dict:
    """获取公司基本信息（名称、行业等）"""
    try:
        df = pro.stock_basic(ts_code=ticker, fields='name,industry,list_date,market')
        if not df.empty:
            row = df.iloc[0]
            return {
                "name": row.get('name', ticker),
                "industry": row.get('industry', 'Unknown'),
                "list_date": row.get('list_date', ''),
                "market": row.get('market', 'Unknown')
                "description": f"{row.get('name', ticker)} is listed on {row.get('market', 'China')} market."
            }
    except Exception as e:
        print(f"Error fetching company facts for {ticker}: {e}")
    # 返回最小化信息，避免中断流程
    return {"name": ticker, "industry": "Unknown", "description": "Data not available"}


def get_market_cap(
    ticker: str,
    end_date: str,
    api_key: str = None,
) -> float | None:
    """Fetch market cap from the API."""
    # Check if end_date is today
    if end_date == datetime.datetime.now().strftime("%Y-%m-%d"):
        # Get the market cap from company facts API
        headers = {}
        financial_api_key = api_key or os.environ.get("FINANCIAL_DATASETS_API_KEY")
        if financial_api_key:
            headers["X-API-KEY"] = financial_api_key

        url = f"https://api.financialdatasets.ai/company/facts/?ticker={ticker}"
        response = _make_api_request(url, headers)
        if response.status_code != 200:
            print(f"Error fetching company facts: {ticker} - {response.status_code}")
            return None

        data = response.json()
        response_model = CompanyFactsResponse(**data)
        return response_model.company_facts.market_cap

    financial_metrics = get_financial_metrics(ticker, end_date, api_key=api_key)
    if not financial_metrics:
        return None

    market_cap = financial_metrics[0].market_cap

    if not market_cap:
        return None

    return market_cap


def prices_to_df(prices: List[Price]) -> 'pd.DataFrame':
    """将 Price 列表转换为 DataFrame"""
    import pandas as pd
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


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str, api_key: str = None) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date, api_key=api_key)
    return prices_to_df(prices)
