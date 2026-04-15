import datetime
import logging
import os
import pandas as pd
import requests
import time

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


async def get_prices(ticker: str, start_date: str, end_date: str) -> List[Price]:
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

async def get_financial_metrics(ticker: str) -> FinancialMetrics:
    # 调用 Tushare 的 fina_indicator 接口获取主要财务指标
    df = pro.fina_indicator(ts_code=ticker)
    
    # 如果获取到的数据为空，返回一个空的 FinancialMetrics 对象
    if df.empty:
        return FinancialMetrics()
    
    # 取最新一期（第一行）的数据
    latest = df.iloc[0]
    metrics = FinancialMetrics(
        pe_ratio=latest.get('pe'),  # 市盈率
        pb_ratio=latest.get('pb'),  # 市净率
        roe=latest.get('roe'),      # 净资产收益率
        market_cap=latest.get('market_cap')  # 市值
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


async def get_insider_trades(ticker: str, start_date: str, end_date: str) -> List[InsiderTrade]:
    # 调用 Tushare 的 disclosure 接口获取股东增减持信息
    df = pro.disclosure(ts_code=ticker, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
    
    # 如果获取到的数据为空，直接返回空列表
    if df.empty:
        return []
    
    # 将 Tushare 返回的 DataFrame 数据，转换成项目需要的 InsiderTrade 对象列表
    trades = []
    for _, row in df.iterrows():
        trade = InsiderTrade(
            transaction_shares=row.get('shares_change'),  # 变动股数
            transaction_value=row.get('change_value'),   # 变动金额
            transaction_date=row.get('ann_date')         # 公告日期
        )
        trades.append(trade)
    
    return trades


async def get_company_news(ticker: str, start_date: str, end_date: str) -> List[CompanyNews]:
    # 调用 Tushare 的 news 接口获取公司新闻
    df = pro.news(ts_code=ticker, start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
    
    # 如果获取到的数据为空，直接返回空列表
    if df.empty:
        return []
    
    # 将 Tushare 返回的 DataFrame 数据，转换成项目需要的 CompanyNews 对象列表
    news_list = []
    for _, row in df.iterrows():
        news = CompanyNews(
            title=row['title'],
            content=row['content'],
            source=row['source'],
            datetime=row['datetime']
        )
        news_list.append(news)
    
    return news_list


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


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str, api_key: str = None) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date, api_key=api_key)
    return prices_to_df(prices)
