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
def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> FinancialMetrics:
    """
    获取财务指标 - 适配 A 股 Tushare 数据
    字段映射：Tushare 字段名 -> 模型标准字段名
    """
    ticker = _normalize_ticker(ticker)
    
    # 初始化所有字段为 None
    metrics_data = {
        # 基础字段
        "ticker": ticker,
        "report_period": None,
        "period": kwargs.get('period', 'ttm'),
        "currency": "CNY",
        
        # 估值指标
        "market_cap": None,
        "enterprise_value": None,
        "price_to_earnings_ratio": None,      # pe_ratio
        "price_to_book_ratio": None,            # pb_ratio
        "price_to_sales_ratio": None,           # ps_ratio
        "enterprise_value_to_ebitda_ratio": None,
        "enterprise_value_to_revenue_ratio": None,
        "free_cash_flow_yield": None,
        "peg_ratio": None,
        
        # 盈利能力
        "gross_margin": None,
        "operating_margin": None,
        "net_margin": None,
        "return_on_equity": None,               # roe
        "return_on_assets": None,               # roa
        "return_on_invested_capital": None,
        
        # 运营效率
        "asset_turnover": None,
        "inventory_turnover": None,
        "receivables_turnover": None,
        "days_sales_outstanding": None,
        "operating_cycle": None,
        "working_capital_turnover": None,
        
        # 偿债能力
        "current_ratio": None,
        "quick_ratio": None,
        "cash_ratio": None,
        "operating_cash_flow_ratio": None,
        "debt_to_equity": None,
        "debt_to_assets": None,
        "interest_coverage": None,
        
        # 成长性
        "revenue_growth": None,
        "earnings_growth": None,
        "book_value_growth": None,
        "earnings_per_share_growth": None,
        "free_cash_flow_growth": None,
        "operating_income_growth": None,
        "ebitda_growth": None,
        
        # 股东回报
        "payout_ratio": None,
        
        # 每股指标
        "earnings_per_share": None,             # eps
        "book_value_per_share": None,           # bps
        "free_cash_flow_per_share": None,
    }
    
    try:
        # 1. 获取基础财务指标 (fina_indicator)
        df_indicator = pro.fina_indicator(ts_code=ticker)
        if not df_indicator.empty:
            latest = df_indicator.iloc[0]
            
            # 字段映射：Tushare -> 标准字段
            field_mapping = {
                # 盈利能力
                'roe': 'return_on_equity',
                'roa': 'return_on_assets',
                'gross_margin': 'gross_margin',
                'operating_margin': 'operating_margin',
                'net_margin': 'net_margin',
                'asset_turnover': 'asset_turnover',
                'inventory_turnover': 'inventory_turnover',
                'receivables_turnover': 'receivables_turnover',
                'operating_cycle': 'operating_cycle',
                'working_capital_turnover': 'working_capital_turnover',
                'current_ratio': 'current_ratio',
                'quick_ratio': 'quick_ratio',
                'debt_to_equity': 'debt_to_equity',
                'interest_coverage': 'interest_coverage',
                'revenue_growth': 'revenue_growth',
                'operating_income_growth': 'operating_income_growth',
                'eps': 'earnings_per_share',
                'bps': 'book_value_per_share',
            }
            
            for tushare_field, standard_field in field_mapping.items():
                if tushare_field in latest and pd.notna(latest[tushare_field]):
                    metrics_data[standard_field] = float(latest[tushare_field])
            
            metrics_data['report_period'] = latest.get('end_date')
        
        # 2. 获取每日基本面数据 (daily_basic) - 包含估值指标
        df_daily = pro.daily_basic(ts_code=ticker)
        if not df_daily.empty:
            latest_daily = df_daily.iloc[0]
            
            # 估值指标映射
            valuation_mapping = {
                'pe': 'price_to_earnings_ratio',
                'pb': 'price_to_book_ratio',
                'ps': 'price_to_sales_ratio',
                'total_mv': 'market_cap',  # 万元
            }
            
            for tushare_field, standard_field in valuation_mapping.items():
                if tushare_field in latest_daily and pd.notna(latest_daily[tushare_field]):
                    value = float(latest_daily[tushare_field])
                    # 市值需要转换单位（万元 -> 元）
                    if tushare_field == 'total_mv':
                        value *= 10000
                    metrics_data[standard_field] = value
        
        # 3. 尝试计算 Enterprise Value (简版)
        if metrics_data['market_cap']:
            try:
                # 获取最新资产负债表
                df_balance = pro.balancesheet(ts_code=ticker, period=metrics_data['report_period'][:4]+'1231' if metrics_data['report_period'] else None)
                if not df_balance.empty:
                    latest_balance = df_balance.iloc[0]
                    total_debt = 0
                    cash = 0
                    if 'total_liabilities' in latest_balance and pd.notna(latest_balance['total_liabilities']):
                        total_debt = float(latest_balance['total_liabilities'])
                    if 'money_cap' in latest_balance and pd.notna(latest_balance['money_cap']):
                        cash = float(latest_balance['money_cap'])
                    
                    metrics_data['enterprise_value'] = metrics_data['market_cap'] + total_debt - cash
            except Exception:
                pass
        
        return FinancialMetrics(**metrics_data)
        
    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
        # 返回带有默认 None 值的对象，而不是抛出异常
        return FinancialMetrics(**metrics_data)

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
def search_line_items(
    ticker: str,
    line_items: List[str],
    period: str = "ttm",
    limit: int = 10,
    end_date: str = None,
    **kwargs
) -> List[Dict]:
    """
    搜索特定财务报表项目 - A股Tushare适配版
    
    Args:
        ticker: 股票代码，如 "000001.SZ"
        line_items: 财务项目列表，如 ["revenue", "net_income", "total_assets"]
        period: 报告周期，"ttm"|"annual"|"quarterly"（A股主要用annual）
        limit: 返回记录数量
        end_date: 结束日期，格式 "2024-12-31"
        **kwargs: 其他参数（兼容原API）
    
    Returns:
        List[Dict]: 财务数据列表，每项包含 ticker, line_item, value, report_period, period, currency
    """
    ticker = _normalize_ticker(ticker)
    results = []
    
    # Tushare 字段映射：标准字段名 -> Tushare字段名
    # 注意：Tushare不同接口字段名不同，需分别处理
    field_mapping = {
        # 利润表字段 (pro.income)
        "revenue": "total_revenue",           # 营业总收入
        "operating_income": "operate_income", # 营业收入
        "operating_expense": "operate_expense", # 营业支出
        "net_income": "n_income",             # 净利润（注意：不是net_profit）
        "total_profit": "total_profit",       # 利润总额
        "income_tax": "income_tax",          # 所得税
        "basic_eps": "basic_eps",            # 基本每股收益
        
        # 资产负债表字段 (pro.balancesheet)
        "total_assets": "total_assets",
        "total_liabilities": "total_liabilities",
        "shareholders_equity": "total_hldr_eqy_exc_min_int",  # 股东权益(不含少数股东)
        "cash_and_equivalents": "money_cap",   # 货币资金
        "trading_fin_assets": "trading_fin_assets", # 交易性金融资产
        "inventory": "inventories",           # 存货
        "accounts_receivable": "accounts_receiv",  # 应收账款
        "notes_receivable": "notes_receiv",    # 应收票据
        "fixed_assets": "fix_assets",          # 固定资产
        "goodwill": "goodwill",               # 商誉
        
        # 现金流量表字段 (pro.cashflow)
        "operating_cash_flow": "n_cashflow_act",  # 经营活动现金流净额
        "investing_cash_flow": "n_cashflow_inv_act", # 投资活动现金流净额
        "financing_cash_flow": "n_cashflow_fin_act",  # 筹资活动现金流净额
        "free_cash_flow": None,  # 需计算：经营现金流 - 购建固定资产现金
        "capex": "c_paid_for_assets",  # 购建固定资产、无形资产支付的现金
    }
    
    try:
        # 确定查询年份（A股年报）
        if end_date:
            year = end_date[:4]
        else:
            year = None
        
        # 1. 查询利润表数据
        income_fields = {k: v for k, v in field_mapping.items() if k in [
            "revenue", "operating_income", "operating_expense", "net_income", 
            "total_profit", "income_tax", "basic_eps"
        ] and v is not None}
        
        if any(item in income_fields for item in line_items):
            try:
                df_income = pro.income(ts_code=ticker, start_date=f"{int(year)-5}0101" if year else None, end_date=f"{year}1231" if year else None)
                if not df_income.empty:
                    for item in line_items:
                        tushare_field = income_fields.get(item)
                        if tushare_field and tushare_field in df_income.columns:
                            for idx, row in df_income.head(limit).iterrows():
                                if pd.notna(row.get(tushare_field)):
                                    results.append({
                                        'ticker': ticker,
                                        'line_item': item,
                                        'value': float(row[tushare_field]),
                                        'report_period': row.get('end_date'),
                                        'period': period,
                                        'currency': 'CNY'
                                    })
            except Exception as e:
                print(f"Error fetching income data for {ticker}: {e}")
        
        # 2. 查询资产负债表数据
        balance_fields = {k: v for k, v in field_mapping.items() if k in [
            "total_assets", "total_liabilities", "shareholders_equity",
            "cash_and_equivalents", "trading_fin_assets", "inventory",
            "accounts_receivable", "notes_receivable", "fixed_assets", "goodwill"
        ] and v is not None}
        
        if any(item in balance_fields for item in line_items):
            try:
                df_balance = pro.balancesheet(ts_code=ticker, start_date=f"{int(year)-5}0101" if year else None, end_date=f"{year}1231" if year else None)
                if not df_balance.empty:
                    for item in line_items:
                        tushare_field = balance_fields.get(item)
                        if tushare_field and tushare_field in df_balance.columns:
                            for idx, row in df_balance.head(limit).iterrows():
                                if pd.notna(row.get(tushare_field)):
                                    # 避免重复
                                    exists = any(r['line_item'] == item and r['report_period'] == row.get('end_date') for r in results)
                                    if not exists:
                                        results.append({
                                            'ticker': ticker,
                                            'line_item': item,
                                            'value': float(row[tushare_field]),
                                            'report_period': row.get('end_date'),
                                            'period': period,
                                            'currency': 'CNY'
                                        })
            except Exception as e:
                print(f"Error fetching balance sheet for {ticker}: {e}")
        
        # 3. 查询现金流量表数据
        cashflow_fields = {k: v for k, v in field_mapping.items() if k in [
            "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "capex"
        ] and v is not None}
        
        if any(item in list(cashflow_fields.keys()) + ["free_cash_flow"] for item in line_items):
            try:
                df_cashflow = pro.cashflow(ts_code=ticker, start_date=f"{int(year)-5}0101" if year else None, end_date=f"{year}1231" if year else None)
                if not df_cashflow.empty:
                    # 处理直接存在的字段
                    for item in line_items:
                        if item == "free_cash_flow":
                            # 计算自由现金流 = 经营现金流 - 资本支出
                            for idx, row in df_cashflow.head(limit).iterrows():
                                ocf = row.get("n_cashflow_act")
                                capex = row.get("c_paid_for_assets")
                                if pd.notna(ocf) and pd.notna(capex):
                                    fcf = float(ocf) - abs(float(capex))
                                    results.append({
                                        'ticker': ticker,
                                        'line_item': "free_cash_flow",
                                        'value': fcf,
                                        'report_period': row.get('end_date'),
                                        'period': period,
                                        'currency': 'CNY'
                                    })
                        else:
                            tushare_field = cashflow_fields.get(item)
                            if tushare_field and tushare_field in df_cashflow.columns:
                                for idx, row in df_cashflow.head(limit).iterrows():
                                    if pd.notna(row.get(tushare_field)):
                                        exists = any(r['line_item'] == item and r['report_period'] == row.get('end_date') for r in results)
                                        if not exists:
                                            results.append({
                                                'ticker': ticker,
                                                'line_item': item,
                                                'value': float(row[tushare_field]),
                                                'report_period': row.get('end_date'),
                                                'period': period,
                                                'currency': 'CNY'
                                            })
            except Exception as e:
                print(f"Error fetching cash flow for {ticker}: {e}")
        
        # 4. 处理特殊计算字段
        # 如 working_capital = 流动资产 - 流动负债
        if "working_capital" in line_items:
            try:
                df_balance = pro.balancesheet(ts_code=ticker, start_date=f"{int(year)-5}0101" if year else None, end_date=f"{year}1231" if year else None)
                if not df_balance.empty and 'total_cur_assets' in df_balance.columns and 'total_cur_liab' in df_balance.columns:
                    for idx, row in df_balance.head(limit).iterrows():
                        if pd.notna(row.get('total_cur_assets')) and pd.notna(row.get('total_cur_liab')):
                            wc = float(row['total_cur_assets']) - float(row['total_cur_liab'])
                            results.append({
                                'ticker': ticker,
                                'line_item': "working_capital",
                                'value': wc,
                                'report_period': row.get('end_date'),
                                'period': period,
                                'currency': 'CNY'
                            })
            except Exception as e:
                print(f"Error calculating working capital for {ticker}: {e}")
        
        # 5. 处理 total_debt（总负债）
        if "total_debt" in line_items:
            try:
                df_balance = pro.balancesheet(ts_code=ticker, start_date=f"{int(year)-5}0101" if year else None, end_date=f"{year}1231" if year else None)
                if not df_balance.empty and 'total_liabilities' in df_balance.columns:
                    for idx, row in df_balance.head(limit).iterrows():
                        if pd.notna(row.get('total_liabilities')):
                            exists = any(r['line_item'] == "total_debt" and r['report_period'] == row.get('end_date') for r in results)
                            if not exists:
                                results.append({
                                    'ticker': ticker,
                                    'line_item': "total_debt",
                                    'value': float(row['total_liabilities']),
                                    'report_period': row.get('end_date'),
                                    'period': period,
                                    'currency': 'CNY'
                                })
            except Exception as e:
                print(f"Error fetching total_debt for {ticker}: {e}")
        
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
