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
    # ... 代码规范化函数，此处省略，保持不变 ...
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
    # ... 保持不变 ...
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
                volume=int(row['vol']) if not pd.isna(row['vol']) else 0,
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
        # 1. 获取基础财务指标 (fina_indicator)
        df_indicator = pro.fina_indicator(ts_code=ticker)
        if df_indicator.empty:
            # 数据为空时返回一个全 None 的默认对象
            return FinancialMetrics(ticker=ticker, report_period=None, period=None, currency="CNY")
        
        latest_indicator = df_indicator.iloc[0]

        # 2. 获取每日基本面数据 (daily_basic)
        df_daily_basic = pro.daily_basic(ts_code=ticker)
        if df_daily_basic.empty:
            # 如果获取失败，创建一个空的 DataFrame，避免后续代码报错
            df_daily_basic = pd.DataFrame()
        
        # 3. 获取利润表 (income)
        df_income = pro.income(ts_code=ticker)
        if df_income.empty:
            df_income = pd.DataFrame()

        # 4. 获取资产负债表 (balancesheet)
        df_balance = pro.balancesheet(ts_code=ticker)
        if df_balance.empty:
            df_balance = pd.DataFrame()

        # 5. 获取现金流量表 (cashflow)
        df_cashflow = pro.cashflow(ts_code=ticker)
        if df_cashflow.empty:
            df_cashflow = pd.DataFrame()

        # --- 开始构建 FinancialMetrics 对象，从各个数据源中提取所需字段 ---
        metrics = FinancialMetrics(
            # 基础字段
            ticker=ticker,
            report_period=latest_indicator.get('end_date'),
            period=kwargs.get('period', 'annual'),
            currency="CNY",
            
            # 估值指标
            market_cap=float(df_daily_basic.iloc[0]['total_mv']) * 10000 if not df_daily_basic.empty else None,
            pe_ratio=latest_indicator.get('pe'),
            pb_ratio=latest_indicator.get('pb'),
            ps_ratio=latest_indicator.get('ps'),
            
            # 利润率指标
            gross_margin=latest_indicator.get('gross_margin'),
            operating_margin=latest_indicator.get('operating_margin'),
            net_margin=latest_indicator.get('net_margin'),
            
            # 回报率指标
            roe=latest_indicator.get('roe'),
            roa=latest_indicator.get('roa'),
            
            # 运营能力指标
            asset_turnover=latest_indicator.get('asset_turnover'),
            inventory_turnover=latest_indicator.get('inventory_turnover'),
            receivables_turnover=latest_indicator.get('receivables_turnover'),
            operating_cycle=latest_indicator.get('operating_cycle'),
            working_capital_turnover=latest_indicator.get('working_capital_turnover'),
            
            # 偿债能力指标
            current_ratio=latest_indicator.get('current_ratio'),
            quick_ratio=latest_indicator.get('quick_ratio'),
            debt_to_equity=latest_indicator.get('debt_to_equity'),
            interest_coverage=latest_indicator.get('interest_coverage'),
            
            # 增长指标
            revenue_growth=latest_indicator.get('revenue_growth'),
            earnings_growth=latest_indicator.get('earnings_growth'),
            operating_income_growth=latest_indicator.get('operating_income_growth'),
            
            # 每股指标
            eps=latest_indicator.get('eps'),
            bps=latest_indicator.get('bps'),
            
            # 高级估值与财务健康度指标（需要从三大报表计算）
            # 以下为需要计算的指标，这里先设为 None，后续可以完善
            enterprise_value=None,
            free_cash_flow=None,
            free_cash_flow_yield=None,
            peg_ratio=None,
            return_on_invested_capital=None,
            days_sales_outstanding=None,
            cash_ratio=None,
            operating_cash_flow_ratio=None,
            debt_to_assets=None,
            book_value_growth=None,
            earnings_per_share_growth=None,
            free_cash_flow_growth=None,
            ebitda_growth=None,
            payout_ratio=None,
            free_cash_flow_per_share=None
        )
        return metrics
    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
        # 发生异常时返回一个默认的 FinancialMetrics 对象
        return FinancialMetrics(ticker=ticker, report_period=None, period=None, currency="CNY")

# ==================== 公司新闻 ====================
def get_company_news(ticker: str, start_date: str = None, end_date: str = None, **kwargs) -> List[CompanyNews]:
    ticker = _normalize_ticker(ticker)
    try:
        # 调用 Tushare 接口获取公司新闻
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
        # 调用 Tushare 接口获取股东增减持信息
        df = pro.stk_holdertrade(ts_code=ticker,
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
                transaction_shares=row.get('change_vol'),
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
        # 调用 Tushare 接口获取公司基本信息
        df = pro.stock_company(ts_code=ticker)
        if not df.empty:
            row = df.iloc[0]
            return {
                "name": row.get('org_name', ticker),
                "industry": row.get('industry', 'Unknown'),
                "list_date": row.get('list_date', ''),
                "market": row.get('market', 'Unknown'),
                "description": row.get('introduction', f"{row.get('org_name', ticker)} is listed on China market.")
            }
    except Exception as e:
        print(f"Error fetching company facts for {ticker}: {e}")
    return {"name": ticker, "industry": "Unknown", "description": "Data not available"}

# ==================== 市值 ====================
def get_market_cap(ticker: str, *args, **kwargs) -> float:
    ticker = _normalize_ticker(ticker)
    try:
        # 调用 Tushare 接口获取市值数据
        df = pro.daily_basic(ts_code=ticker, fields='total_mv')
        if not df.empty:
            return float(df.iloc[0]['total_mv']) * 10000  # 万元转元
    except Exception as e:
        print(f"Error in get_market_cap for {ticker}: {e}")
    return 0.0

# ==================== 资产负债表 ====================
def get_balance_sheet(ticker: str, **kwargs) -> List[Dict]:
    # 返回空列表，避免程序崩溃；可根据需要扩展
    return []

# ==================== 现金流量表 ====================
def get_cash_flow(ticker: str, **kwargs) -> List[Dict]:
    return []

# ==================== 利润表 ====================
def get_income_statement(ticker: str, **kwargs) -> List[Dict]:
    return []

# ==================== 搜索财务报表行项目 ====================
def search_line_items(*args, **kwargs) -> List[Dict]:
    # 占位实现，返回空列表避免报错
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
