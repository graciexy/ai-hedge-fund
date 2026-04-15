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
        # 1. 调用 Tushare 接口获取数据
        # 2. 检查数据有效性
        # 3. 将 DataFrame 数据转换为项目要求的 Price 对象列表
        # 4. 按日期升序排序后返回
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
                # volume 字段必须是整数，从 Tushare 获取的 vol 可能是浮点数，需要转换
                volume=int(row['vol']) if not pd.isna(row['vol']) else 0,
                # time 字段是必须的，从 Tushare 获取的 trade_date 是字符串，需要直接赋值
                time=row['trade_date']
            )
            prices.append(price)
        prices.sort(key=lambda x: x.time)
        return prices
    except Exception as e:
        print(f"Error in get_prices for {ticker}: {e}")
        return []

# ==================== 财务指标 ====================
def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> FinancialMetrics:
    ticker = _normalize_ticker(ticker)
    try:
        # 1. 调用 Tushare 接口获取财务指标数据
        # 2. 如果数据为空，则返回一个所有字段都为 None 的 FinancialMetrics 对象
        # 3. 从最新一期数据中提取字段，并将其映射到 FinancialMetrics 模型所要求的字段名
        # 4. 对于 Tushare 没有提供的字段，保持为 None
        df = pro.fina_indicator(ts_code=ticker)
        if df.empty:
            # 返回一个完整的 FinancialMetrics 对象，所有字段初始化为 None
            return FinancialMetrics(
                ticker=ticker,
                report_period=None,
                period=None,
                currency="CNY",
                market_cap=None,
                enterprise_value=None,
                price_to_earnings_ratio=None,
                price_to_book_ratio=None,
                price_to_sales_ratio=None,
                enterprise_value_to_ebitda_ratio=None,
                enterprise_value_to_revenue_ratio=None,
                free_cash_flow_yield=None,
                peg_ratio=None,
                gross_margin=None,
                operating_margin=None,
                net_margin=None,
                return_on_equity=None,
                return_on_assets=None,
                return_on_invested_capital=None,
                asset_turnover=None,
                inventory_turnover=None,
                receivables_turnover=None,
                days_sales_outstanding=None,
                operating_cycle=None,
                working_capital_turnover=None,
                current_ratio=None,
                quick_ratio=None,
                cash_ratio=None,
                operating_cash_flow_ratio=None,
                debt_to_equity=None,
                debt_to_assets=None,
                interest_coverage=None,
                revenue_growth=None,
                earnings_growth=None,
                book_value_growth=None,
                earnings_per_share_growth=None,
                free_cash_flow_growth=None,
                operating_income_growth=None,
                ebitda_growth=None,
                payout_ratio=None,
                earnings_per_share=None,
                book_value_per_share=None,
                free_cash_flow_per_share=None
            )
        latest = df.iloc[0]
        # 将 Tushare 的字段名映射到 FinancialMetrics 的字段名
        metrics = FinancialMetrics(
            ticker=ticker,
            report_period=latest.get('end_date'),
            period=kwargs.get('period', 'annual'),
            currency="CNY",
            market_cap=latest.get('market_cap'),
            enterprise_value=None,
            price_to_earnings_ratio=latest.get('pe'),
            price_to_book_ratio=latest.get('pb'),
            price_to_sales_ratio=latest.get('ps'),
            enterprise_value_to_ebitda_ratio=None,
            enterprise_value_to_revenue_ratio=None,
            free_cash_flow_yield=None,
            peg_ratio=None,
            gross_margin=latest.get('gross_margin'),
            operating_margin=latest.get('operating_margin'),
            net_margin=latest.get('net_margin'),
            return_on_equity=latest.get('roe'),
            return_on_assets=latest.get('roa'),
            return_on_invested_capital=None,
            asset_turnover=latest.get('asset_turnover'),
            inventory_turnover=latest.get('inventory_turnover'),
            receivables_turnover=latest.get('receivables_turnover'),
            days_sales_outstanding=None,
            operating_cycle=None,
            working_capital_turnover=None,
            current_ratio=latest.get('current_ratio'),
            quick_ratio=latest.get('quick_ratio'),
            cash_ratio=None,
            operating_cash_flow_ratio=None,
            debt_to_equity=latest.get('debt_to_equity'),
            debt_to_assets=None,
            interest_coverage=latest.get('interest_coverage'),
            revenue_growth=latest.get('revenue_growth'),
            earnings_growth=latest.get('earnings_growth'),
            book_value_growth=None,
            earnings_per_share_growth=None,
            free_cash_flow_growth=None,
            operating_income_growth=None,
            ebitda_growth=None,
            payout_ratio=None,
            earnings_per_share=latest.get('eps'),
            book_value_per_share=latest.get('bps'),
            free_cash_flow_per_share=None
        )
        return metrics
    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
        # 发生异常时返回一个默认的 FinancialMetrics 对象
        return FinancialMetrics(
            ticker=ticker,
            report_period=None,
            period=None,
            currency="CNY",
            market_cap=None,
            enterprise_value=None,
            price_to_earnings_ratio=None,
            price_to_book_ratio=None,
            price_to_sales_ratio=None,
            enterprise_value_to_ebitda_ratio=None,
            enterprise_value_to_revenue_ratio=None,
            free_cash_flow_yield=None,
            peg_ratio=None,
            gross_margin=None,
            operating_margin=None,
            net_margin=None,
            return_on_equity=None,
            return_on_assets=None,
            return_on_invested_capital=None,
            asset_turnover=None,
            inventory_turnover=None,
            receivables_turnover=None,
            days_sales_outstanding=None,
            operating_cycle=None,
            working_capital_turnover=None,
            current_ratio=None,
            quick_ratio=None,
            cash_ratio=None,
            operating_cash_flow_ratio=None,
            debt_to_equity=None,
            debt_to_assets=None,
            interest_coverage=None,
            revenue_growth=None,
            earnings_growth=None,
            book_value_growth=None,
            earnings_per_share_growth=None,
            free_cash_flow_growth=None,
            operating_income_growth=None,
            ebitda_growth=None,
            payout_ratio=None,
            earnings_per_share=None,
            book_value_per_share=None,
            free_cash_flow_per_share=None
        )

# ==================== 公司新闻 ====================
def get_company_news(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[CompanyNews]:
    ticker = _normalize_ticker(ticker)
    try:
        # 1. 调用 Tushare 接口获取公司新闻数据
        # 2. 处理可能为空的数据
        # 3. 将数据转换为项目要求的 CompanyNews 对象列表
        df = pro.news(ts_code=ticker,
                      start_date=start_date.replace('-', '') if start_date else None,
                      end_date=end_date.replace('-', '') if end_date else None)
        if df.empty:
            return []
        news_list = []
        for _, row in df.iterrows():
            news = CompanyNews(
                ticker=ticker,
                title=row.get('title', ''),
                author=row.get('source', ''),
                source=row.get('source', ''),
                date=row.get('datetime', ''),
                url=row.get('url', ''),
                sentiment=None
            )
            news_list.append(news)
        return news_list
    except Exception:
        return []

# ==================== 内幕交易 ====================
def get_insider_trades(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[InsiderTrade]:
    ticker = _normalize_ticker(ticker)
    try:
        # 1. 调用 Tushare 接口获取股东增减持信息
        # 2. 处理可能为空的数据
        # 3. 将数据转换为项目要求的 InsiderTrade 对象列表
        df = pro.disclosure(ts_code=ticker,
                            start_date=start_date.replace('-', '') if start_date else None,
                            end_date=end_date.replace('-', '') if end_date else None)
        if df.empty:
            return []
        trades = []
        for _, row in df.iterrows():
            trade = InsiderTrade(
                ticker=ticker,
                issuer=None,
                name=row.get('holder_name'),
                title=None,
                is_board_director=None,
                transaction_date=row.get('ann_date'),
                transaction_shares=row.get('shares_change'),
                transaction_price_per_share=None,
                transaction_value=row.get('change_value'),
                shares_owned_before_transaction=None,
                shares_owned_after_transaction=None,
                security_title=None,
                filing_date=row.get('ann_date')
            )
            trades.append(trade)
        return trades
    except Exception:
        return []

# ==================== 公司概况 ====================
def get_company_facts(ticker: str) -> dict:
    ticker = _normalize_ticker(ticker)
    try:
        # 1. 调用 Tushare 接口获取公司基本信息
        # 2. 如果获取成功，返回一个包含公司信息的字典
        # 3. 如果失败，返回一个包含最小信息的字典，防止程序中断
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
        # 1. 调用 Tushare 接口获取市值数据
        # 2. 单位转换：Tushare 返回的 total_mv 单位是万元，需要转换为元
        # 3. 返回市值数据，如果获取失败则返回 0.0
        df = pro.daily_basic(ts_code=ticker, fields='total_mv')
        if not df.empty:
            return float(df.iloc[0]['total_mv']) * 10000
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
def search_line_items(*args, **kwargs) -> List[Dict]:
    """
    搜索财务报表行项目（占位实现）
    原项目用于从财务报表中提取特定指标，这里返回空列表避免报错
    """
    return []

# ==================== 辅助函数 ====================
def prices_to_df(prices: List[Price]) -> pd.DataFrame:
    data = {
        'time': [p.time for p in prices],
        'open': [p.open for p in prices],
        'high': [p.high for p in prices],
        'low': [p.low for p in prices],
        'close': [p.close for p in prices],
        'volume': [p.volume for p in prices],
    }
    df = pd.DataFrame(data)
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time')
    return df
