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
    """标准化股票代码格式"""
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
def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> List[FinancialMetrics]:
    """
    获取财务指标，返回列表（原项目期望列表，用于多期分析）
    """
    ticker = _normalize_ticker(ticker)
    # 准备一个包含所有字段的默认字典
    default_metrics_dict = {
        'ticker': ticker,
        'report_period': '',
        'period': kwargs.get('period', 'annual'),
        'currency': 'CNY',
        'market_cap': None,
        'enterprise_value': None,
        'price_to_earnings_ratio': None,
        'price_to_book_ratio': None,
        'price_to_sales_ratio': None,
        'enterprise_value_to_ebitda_ratio': None,
        'enterprise_value_to_revenue_ratio': None,
        'free_cash_flow_yield': None,
        'peg_ratio': None,
        'gross_margin': None,
        'operating_margin': None,
        'net_margin': None,
        'return_on_equity': None,
        'return_on_assets': None,
        'return_on_invested_capital': None,
        'asset_turnover': None,
        'inventory_turnover': None,
        'receivables_turnover': None,
        'days_sales_outstanding': None,
        'operating_cycle': None,
        'working_capital_turnover': None,
        'current_ratio': None,
        'quick_ratio': None,
        'cash_ratio': None,
        'operating_cash_flow_ratio': None,
        'debt_to_equity': None,
        'debt_to_assets': None,
        'interest_coverage': None,
        'revenue_growth': None,
        'earnings_growth': None,
        'book_value_growth': None,
        'earnings_per_share_growth': None,
        'free_cash_flow_growth': None,
        'operating_income_growth': None,
        'ebitda_growth': None,
        'payout_ratio': None,
        'earnings_per_share': None,
        'book_value_per_share': None,
        'free_cash_flow_per_share': None,
    }
    try:
        # 获取基础财务指标
        df_indicator = pro.fina_indicator(ts_code=ticker)
        if not df_indicator.empty:
            latest = df_indicator.iloc[0]
            default_metrics_dict.update({
                'report_period': latest.get('end_date', ''),
                'price_to_earnings_ratio': latest.get('pe'),
                'price_to_book_ratio': latest.get('pb'),
                'price_to_sales_ratio': latest.get('ps'),
                'gross_margin': latest.get('gross_margin'),
                'operating_margin': latest.get('operating_margin'),
                'net_margin': latest.get('net_margin'),
                'return_on_equity': latest.get('roe'),
                'return_on_assets': latest.get('roa'),
                'asset_turnover': latest.get('asset_turnover'),
                'inventory_turnover': latest.get('inventory_turnover'),
                'receivables_turnover': latest.get('receivables_turnover'),
                'operating_cycle': latest.get('operating_cycle'),
                'working_capital_turnover': latest.get('working_capital_turnover'),
                'current_ratio': latest.get('current_ratio'),
                'quick_ratio': latest.get('quick_ratio'),
                'debt_to_equity': latest.get('debt_to_equity'),
                'interest_coverage': latest.get('interest_coverage'),
                'revenue_growth': latest.get('revenue_growth'),
                'earnings_growth': latest.get('earnings_growth'),
                'operating_income_growth': latest.get('operating_income_growth'),
                'earnings_per_share': latest.get('eps'),
                'book_value_per_share': latest.get('bps'),
            })
        # 获取每日基本面
        df_daily = pro.daily_basic(ts_code=ticker)
        if not df_daily.empty:
            latest_daily = df_daily.iloc[0]
            if default_metrics_dict['market_cap'] is None:
                default_metrics_dict['market_cap'] = latest_daily.get('total_mv') * 10000 if latest_daily.get('total_mv') else None
            if default_metrics_dict['price_to_earnings_ratio'] is None:
                default_metrics_dict['price_to_earnings_ratio'] = latest_daily.get('pe')
            if default_metrics_dict['price_to_book_ratio'] is None:
                default_metrics_dict['price_to_book_ratio'] = latest_daily.get('pb')
            if default_metrics_dict['price_to_sales_ratio'] is None:
                default_metrics_dict['price_to_sales_ratio'] = latest_daily.get('ps')
    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
    # 确保 report_period 不为 None
    if default_metrics_dict['report_period'] is None:
        default_metrics_dict['report_period'] = ''
    # 确保 period 不为 None
    if default_metrics_dict['period'] is None:
        default_metrics_dict['period'] = 'annual'
    # 创建 FinancialMetrics 对象
    metrics_obj = FinancialMetrics(**default_metrics_dict)
    # 返回列表（只有一个元素）
    return [metrics_obj]

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
        df = pro.daily_basic(ts_code=ticker, fields='total_mv')
        if not df.empty:
            return float(df.iloc[0]['total_mv']) * 10000  # 万元转元
    except Exception as e:
        print(f"Error in get_market_cap for {ticker}: {e}")
    return 0.0

# ==================== 资产负债表 ====================
def get_balance_sheet(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.balancesheet(ts_code=ticker)
        if not df.empty:
            return df.to_dict('records')
    except Exception:
        pass
    return []

# ==================== 现金流量表 ====================
def get_cash_flow(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.cashflow(ts_code=ticker)
        if not df.empty:
            return df.to_dict('records')
    except Exception:
        pass
    return []

# ==================== 利润表 ====================
def get_income_statement(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.income(ts_code=ticker)
        if not df.empty:
            return df.to_dict('records')
    except Exception:
        pass
    return []

# ==================== 搜索财务报表行项目 ====================
def search_line_items(*args, **kwargs):
    """
    搜索特定财务报表项目 - A股Tushare适配版
    兼容所有调用方式：位置参数、关键字参数、混合参数
    """
    # 参数提取
    ticker = None
    line_items = None
    period = "ttm"
    limit = 10
    end_date = None
    api_key = None
    
    # 从位置参数提取
    if len(args) >= 1:
        ticker = args[0]
    if len(args) >= 2:
        line_items = args[1]
    if len(args) >= 3:
        period = args[2]
    if len(args) >= 4:
        limit = args[3]
    
    # 从关键字参数提取（覆盖位置参数）
    ticker = kwargs.get('ticker', ticker)
    line_items = kwargs.get('line_items', line_items)
    period = kwargs.get('period', period)
    limit = kwargs.get('limit', limit)
    end_date = kwargs.get('end_date', None)
    api_key = kwargs.get('api_key', None)
    
    if not ticker or not line_items:
        return []
    
    ticker = _normalize_ticker(ticker)
    
    # 字段映射表
    FIELD_MAP = {
        "revenue": "total_revenue",
        "operating_income": "operate_income",
        "net_income": "n_income",
        "total_assets": "total_assets",
        "shareholders_equity": "total_hldr_eqy_exc_min_int",
        "cash_and_equivalents": "money_cap",
        "total_debt": "total_liabilities",
        "inventory": "inventories",
        "accounts_receivable": "accounts_receiv",
        "operating_cash_flow": "n_cashflow_act",
    }
    
    results = []
    
    try:
        # 查询利润表
        income_fields = [(item, FIELD_MAP[item]) for item in line_items 
                        if item in FIELD_MAP and FIELD_MAP[item] in 
                        ["total_revenue", "operate_income", "n_income"]]
        
        if income_fields:
            df = pro.income(ts_code=ticker, limit=limit)
            if not df.empty:
                for std_name, ts_name in income_fields:
                    if ts_name in df.columns:
                        for _, row in df.iterrows():
                            if pd.notna(row[ts_name]):
                                results.append({
                                    'ticker': ticker,
                                    'line_item': std_name,
                                    'value': float(row[ts_name]),
                                    'report_period': str(row.get('end_date', '')),
                                    'period': period,
                                    'currency': 'CNY'
                                })
        
        # 查询资产负债表
        balance_fields = [(item, FIELD_MAP[item]) for item in line_items 
                         if item in FIELD_MAP and FIELD_MAP[item] in 
                         ["total_assets", "total_liabilities", "total_hldr_eqy_exc_min_int", 
                          "money_cap", "inventories", "accounts_receiv"]]
        
        if balance_fields:
            df = pro.balancesheet(ts_code=ticker, limit=limit)
            if not df.empty:
                for std_name, ts_name in balance_fields:
                    if ts_name in df.columns:
                        for _, row in df.iterrows():
                            if pd.notna(row[ts_name]):
                                dup = any(r for r in results 
                                         if r['line_item'] == std_name 
                                         and r['report_period'] == str(row.get('end_date', '')))
                                if not dup:
                                    results.append({
                                        'ticker': ticker,
                                        'line_item': std_name,
                                        'value': float(row[ts_name]),
                                        'report_period': str(row.get('end_date', '')),
                                        'period': period,
                                        'currency': 'CNY'
                                    })
        
        # 查询现金流量表
        if "free_cash_flow" in line_items or any("cash" in item for item in line_items):
            df = pro.cashflow(ts_code=ticker, limit=limit)
            if not df.empty:
                if "free_cash_flow" in line_items:
                    for _, row in df.iterrows():
                        ocf = row.get("n_cashflow_act")
                        if pd.notna(ocf):
                            capex = row.get("c_paid_for_assets")
                            fcf = float(ocf) - (abs(float(capex)) if pd.notna(capex) else 0)
                            results.append({
                                'ticker': ticker,
                                'line_item': "free_cash_flow",
                                'value': fcf,
                                'report_period': str(row.get('end_date', '')),
                                'period': period,
                                'currency': 'CNY'
                            })
        
        return results
        
    except Exception as e:
        print(f"Error in search_line_items for {ticker}: {e}")
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
