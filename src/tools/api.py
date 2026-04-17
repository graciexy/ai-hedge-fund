import os
import tushare as ts
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
import pandas as pd
from functools import lru_cache
from types import SimpleNamespace
from src.data.models import Price, FinancialMetrics, CompanyNews, InsiderTrade

load_dotenv()
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TUSHARE_TOKEN:
    raise ValueError("TUSHARE_TOKEN not found")
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def _normalize_ticker(ticker: str) -> str:
    if ' ' in ticker:
        ticker = ticker.split()[0]
    if '.' not in ticker:
        if ticker.startswith('6'):
            ticker = f"{ticker}.SH"
        else:
            ticker = f"{ticker}.SZ"
    return ticker

# ==================== 简单缓存装饰器 ====================
def cache_result(ttl=3600):
    """简单内存缓存，避免同一股票重复请求"""
    cache = {}
    def decorator(func):
        def wrapper(ticker, *args, **kwargs):
            key = f"{ticker}_{args}_{tuple(sorted(kwargs.items()))}"
            if key in cache:
                return cache[key]
            result = func(ticker, *args, **kwargs)
            cache[key] = result
            return result
        return wrapper
    return decorator

# ==================== 价格数据（带后复权） ====================
def get_prices(ticker: str, start_date: str, end_date: str, **kwargs) -> List[Price]:
    ticker = _normalize_ticker(ticker)
    try:
        # 使用 pro_bar 获取后复权价格（hfq），保证价格连续性
        df = ts.pro_bar(ts_code=ticker, start_date=start_date, end_date=end_date, adj='hfq')
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

# ==================== 财务指标（多期列表） ====================
@cache_result()
def get_financial_metrics(ticker: str, end_date: str = None, **kwargs) -> List[FinancialMetrics]:
    """
    返回多期财务数据列表（最近4期），满足原项目对 len(metrics) 的依赖
    """
    ticker = _normalize_ticker(ticker)
    metrics_list = []

    try:
        # 1. 获取基础财务指标（fina_indicator）
        df_ind = pro.fina_indicator(ts_code=ticker)
        if df_ind.empty:
            return []   # 无数据返回空列表

        # 按报告期排序，取最近4期
        df_ind = df_ind.sort_values('end_date', ascending=False).head(4)

        # 2. 获取每日基本面（用于估值指标）
        df_daily = pro.daily_basic(ts_code=ticker)
        latest_daily = df_daily.iloc[0] if not df_daily.empty else None

        # 3. 获取利润表、资产负债表、现金流量表（用于高级计算）
        df_income = pro.income(ts_code=ticker)
        df_balance = pro.balancesheet(ts_code=ticker)
        df_cashflow = pro.cashflow(ts_code=ticker)

        for _, row in df_ind.iterrows():
            report_period = row.get('end_date', '')

            # --- 基础估值数据（优先用 daily_basic 的最新值） ---
            market_cap = None
            pe = row.get('pe')
            pb = row.get('pb')
            ps = row.get('ps')
            if latest_daily is not None:
                market_cap = latest_daily.get('total_mv') * 10000 if latest_daily.get('total_mv') else None
                if pe is None:
                    pe = latest_daily.get('pe')
                if pb is None:
                    pb = latest_daily.get('pb')
                if ps is None:
                    ps = latest_daily.get('ps')

            # --- 计算 Enterprise Value (EV) = 市值 + 总负债 - 现金及等价物 ---
            enterprise_value = None
            if market_cap is not None and not df_balance.empty:
                balance_row = df_balance.iloc[0]  # 取最新一期
                total_liab = balance_row.get('total_liab')
                cash_eq = balance_row.get('money_cap')
                if total_liab is not None and cash_eq is not None:
                    enterprise_value = market_cap + total_liab - cash_eq

            # --- 计算 Free Cash Flow (FCF) = 经营现金流 - 资本支出 ---
            free_cash_flow = None
            free_cash_flow_yield = None
            if not df_cashflow.empty:
                cf_row = df_cashflow.iloc[0]
                ocf = cf_row.get('n_cashflow_act')      # 经营活动现金流净额
                capex = cf_row.get('c_pay_acq_const_fiolta')  # 购建固定资产支付的现金
                if ocf is not None and capex is not None:
                    free_cash_flow = ocf - capex
                    if market_cap and market_cap > 0:
                        free_cash_flow_yield = free_cash_flow / market_cap

            # --- 构建 FinancialMetrics 对象 ---
            metrics = FinancialMetrics(
                ticker=ticker,
                report_period=report_period,
                period=kwargs.get('period', 'annual'),
                currency='CNY',

                # 估值指标
                market_cap=market_cap,
                enterprise_value=enterprise_value,
                price_to_earnings_ratio=pe,
                price_to_book_ratio=pb,
                price_to_sales_ratio=ps,
                enterprise_value_to_ebitda_ratio=None,  # 需要 EBITDA，暂不计算
                enterprise_value_to_revenue_ratio=enterprise_value / row.get('revenue') if enterprise_value and row.get('revenue') else None,
                free_cash_flow_yield=free_cash_flow_yield,
                peg_ratio=None,  # 需要增长率，后续可算

                # 利润率
                gross_margin=row.get('gross_margin'),
                operating_margin=row.get('operating_margin'),
                net_margin=row.get('net_margin'),

                # 回报率
                return_on_equity=row.get('roe'),
                return_on_assets=row.get('roa'),
                return_on_invested_capital=None,

                # 营运能力
                asset_turnover=row.get('asset_turnover'),
                inventory_turnover=row.get('inventory_turnover'),
                receivables_turnover=row.get('receivables_turnover'),
                days_sales_outstanding=None,
                operating_cycle=row.get('operating_cycle'),
                working_capital_turnover=row.get('working_capital_turnover'),

                # 偿债能力
                current_ratio=row.get('current_ratio'),
                quick_ratio=row.get('quick_ratio'),
                cash_ratio=None,
                operating_cash_flow_ratio=None,
                debt_to_equity=row.get('debt_to_equity'),
                debt_to_assets=None,   # 可从资产负债表计算，暂略
                interest_coverage=row.get('interest_coverage'),

                # 增长指标
                revenue_growth=row.get('revenue_growth'),
                earnings_growth=row.get('earnings_growth'),
                book_value_growth=None,
                earnings_per_share_growth=None,
                free_cash_flow_growth=None,
                operating_income_growth=row.get('operating_income_growth'),
                ebitda_growth=None,
                payout_ratio=None,

                # 每股指标
                earnings_per_share=row.get('eps'),
                book_value_per_share=row.get('bps'),
                free_cash_flow_per_share=free_cash_flow / row.get('total_share') if free_cash_flow and row.get('total_share') else None,
            )
            metrics_list.append(metrics)

    except Exception as e:
        print(f"Error in get_financial_metrics for {ticker}: {e}")
        return []

    return metrics_list   # 返回列表，原项目可 len() 和遍历

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

# ==================== 内幕交易（股东增减持） ====================
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

# ==================== 市值（已整合到财务指标中，但保留独立函数） ====================
def get_market_cap(ticker: str, *args, **kwargs) -> float:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.daily_basic(ts_code=ticker, fields='total_mv')
        if not df.empty:
            return float(df.iloc[0]['total_mv']) * 10000
    except Exception as e:
        print(f"Error in get_market_cap for {ticker}: {e}")
    return 0.0

# ==================== 资产负债表（返回列表，供其他函数使用） ====================
def get_balance_sheet(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.balancesheet(ts_code=ticker)
        if df.empty:
            return []
        return df.to_dict(orient='records')
    except Exception:
        return []

# ==================== 现金流量表 ====================
def get_cash_flow(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.cashflow(ts_code=ticker)
        if df.empty:
            return []
        return df.to_dict(orient='records')
    except Exception:
        return []

# ==================== 利润表 ====================
def get_income_statement(ticker: str, **kwargs) -> List[Dict]:
    ticker = _normalize_ticker(ticker)
    try:
        df = pro.income(ts_code=ticker)
        if df.empty:
            return []
        return df.to_dict(orient='records')
    except Exception:
        return []

# ==================== 搜索财务报表行项目（返回真实对象列表） ====================
def search_line_items(*args, **kwargs):
    """
    搜索特定财务报表项目 - A股Tushare适配版
    返回 SimpleNamespace 对象列表，兼容原代码访问方式
    """
    from types import SimpleNamespace
    
    # 参数提取
    ticker = None
    line_items = None
    period = "ttm"
    limit = 10
    end_date = None
    api_key = None
    
    if len(args) >= 1:
        ticker = args[0]
    if len(args) >= 2:
        line_items = args[1]
    if len(args) >= 3:
        period = args[2]
    if len(args) >= 4:
        limit = args[3]
    
    ticker = kwargs.pop('ticker', ticker)
    line_items = kwargs.pop('line_items', line_items)
    period = kwargs.pop('period', period)
    limit = kwargs.pop('limit', limit)
    end_date = kwargs.pop('end_date', end_date)
    api_key = kwargs.pop('api_key', api_key)
    
    if not ticker or not line_items:
        return []
    
    ticker = _normalize_ticker(ticker)
    
    # 存储每个报告期的数据
    period_data = {}
    
    try:
        # 查询利润表
        income_fields = ["revenue", "operating_income", "net_income", "total_profit"]
        if any(item in income_fields for item in line_items):
            try:
                df = pro.income(ts_code=ticker, limit=limit)
                if not df.empty:
                    for _, row in df.iterrows():
                        end_date = str(row.get('end_date', ''))
                        if end_date not in period_data:
                            period_data[end_date] = {
                                'ticker': ticker,
                                'report_period': end_date,
                                'period': period,
                                'currency': 'CNY'
                            }
                        
                        if 'total_revenue' in df.columns and pd.notna(row.get('total_revenue')):
                            period_data[end_date]['revenue'] = float(row['total_revenue'])
                        if 'operate_income' in df.columns and pd.notna(row.get('operate_income')):
                            period_data[end_date]['operating_income'] = float(row['operate_income'])
                            # 计算经营利润率
                            if 'total_revenue' in df.columns and pd.notna(row.get('total_revenue')) and row['total_revenue'] != 0:
                                period_data[end_date]['operating_margin'] = float(row['operate_income']) / float(row['total_revenue'])
                        if 'n_income' in df.columns and pd.notna(row.get('n_income')):
                            period_data[end_date]['net_income'] = float(row['n_income'])
                            # 计算净利润率
                            if 'total_revenue' in df.columns and pd.notna(row.get('total_revenue')) and row['total_revenue'] != 0:
                                period_data[end_date]['net_margin'] = float(row['n_income']) / float(row['total_revenue'])
            except Exception as e:
                print(f"Income query error for {ticker}: {e}")
        
        # 查询资产负债表
        balance_fields = ["total_assets", "total_liabilities", "shareholders_equity", 
                         "cash_and_equivalents", "total_debt", "inventory", "accounts_receivable"]
        if any(item in balance_fields for item in line_items):
            try:
                df = pro.balancesheet(ts_code=ticker, limit=limit)
                if not df.empty:
                    for _, row in df.iterrows():
                        end_date = str(row.get('end_date', ''))
                        if end_date not in period_data:
                            period_data[end_date] = {
                                'ticker': ticker,
                                'report_period': end_date,
                                'period': period,
                                'currency': 'CNY'
                            }
                        
                        if 'total_assets' in df.columns and pd.notna(row.get('total_assets')):
                            period_data[end_date]['total_assets'] = float(row['total_assets'])
                        if 'total_liabilities' in df.columns and pd.notna(row.get('total_liabilities')):
                            period_data[end_date]['total_liabilities'] = float(row['total_liabilities'])
                            period_data[end_date]['total_debt'] = float(row['total_liabilities'])
                        if 'total_hldr_eqy_exc_min_int' in df.columns and pd.notna(row.get('total_hldr_eqy_exc_min_int')):
                            period_data[end_date]['shareholders_equity'] = float(row['total_hldr_eqy_exc_min_int'])
                            # 计算 ROE 和负债权益比
                            if 'n_income' in locals() and pd.notna(row.get('total_hldr_eqy_exc_min_int')) and row['total_hldr_eqy_exc_min_int'] != 0:
                                # 需要从利润表获取净利润，这里先占位
                                pass
                            if 'total_liabilities' in df.columns and pd.notna(row.get('total_liabilities')) and row['total_hldr_eqy_exc_min_int'] != 0:
                                period_data[end_date]['debt_to_equity'] = float(row['total_liabilities']) / float(row['total_hldr_eqy_exc_min_int'])
                        if 'money_cap' in df.columns and pd.notna(row.get('money_cap')):
                            period_data[end_date]['cash_and_equivalents'] = float(row['money_cap'])
                        if 'inventories' in df.columns and pd.notna(row.get('inventories')):
                            period_data[end_date]['inventory'] = float(row['inventories'])
                        if 'accounts_receiv' in df.columns and pd.notna(row.get('accounts_receiv')):
                            period_data[end_date]['accounts_receivable'] = float(row['accounts_receiv'])
            except Exception as e:
                print(f"Balance query error for {ticker}: {e}")
        
        # 查询现金流量表
        if any("cash" in item or item == "free_cash_flow" for item in line_items):
            try:
                df = pro.cashflow(ts_code=ticker, limit=limit)
                if not df.empty:
                    for _, row in df.iterrows():
                        end_date = str(row.get('end_date', ''))
                        if end_date not in period_data:
                            period_data[end_date] = {
                                'ticker': ticker,
                                'report_period': end_date,
                                'period': period,
                                'currency': 'CNY'
                            }
                        
                        if 'n_cashflow_act' in df.columns and pd.notna(row.get('n_cashflow_act')):
                            period_data[end_date]['operating_cash_flow'] = float(row['n_cashflow_act'])
                        
                        if "free_cash_flow" in line_items:
                            ocf = row.get("n_cashflow_act")
                            capex = row.get("c_paid_for_assets")
                            if pd.notna(ocf):
                                fcf = float(ocf) - (abs(float(capex)) if pd.notna(capex) else 0)
                                period_data[end_date]['free_cash_flow'] = fcf
            except Exception as e:
                print(f"Cashflow query error for {ticker}: {e}")
        
        # 获取财务指标（fina_indicator）中的增长率数据
        try:
            df_indicator = pro.fina_indicator(ts_code=ticker, limit=limit)
            if not df.empty:
                for _, row in df_indicator.iterrows():
                    end_date = str(row.get('end_date', ''))
                    if end_date in period_data:
                        if 'revenue_yoy' in df_indicator.columns and pd.notna(row.get('revenue_yoy')):
                            period_data[end_date]['revenue_growth'] = float(row['revenue_yoy'])
                        if 'profit_yoy' in df_indicator.columns and pd.notna(row.get('profit_yoy')):
                            period_data[end_date]['earnings_growth'] = float(row['profit_yoy'])
                        if 'roe' in df_indicator.columns and pd.notna(row.get('roe')):
                            period_data[end_date]['return_on_equity'] = float(row['roe'])
        except Exception as e:
            print(f"Indicator query error for {ticker}: {e}")
        
        # 转换为 SimpleNamespace 对象列表
        results = []
        for end_date, data in period_data.items():
            # 确保所有请求的 line_items 都存在（缺失的设为 None）
            for item in line_items:
                if item not in data:
                    data[item] = None
            
            # 确保常用的财务指标属性存在（代码可能访问）
            default_fields = [
                'operating_margin', 'revenue_growth', 'earnings_growth', 
                'return_on_equity', 'debt_to_equity', 'gross_margin', 
                'net_margin', 'free_cash_flow'
            ]
            for field in default_fields:
                if field not in data:
                    data[field] = None
            
            results.append(SimpleNamespace(**data))
        
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
