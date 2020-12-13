from sqlalchemy.orm import Session
from sqlalchemy import func, Date, cast
from . import models
import datetime

def get_symbols(db: Session):
    return db.query(models.vStockAndIndex).all()

def get_symbol_id(db: Session, symbol_name: str):
    return db.query(models.vStockAndIndex).filter(models.vStockAndIndex.Name == symbol_name).first()

def get_symbol_name(db: Session, symbol_id: int):
    return db.query(models.vStockAndIndex).filter(models.vStockAndIndex.ID == symbol_id).first()

def get_financial_by_date(db: Session, date: datetime.date):
    return db.query(models.vFinancial).filter(
            cast(models.vFinancial.ImportDate, Date) == date
        ).all()

def get_prices(db: Session, symbol_name: str):
    symbol_id_subquery = db.query(models.vStockAndIndex.ID).filter(models.vStockAndIndex.Name == symbol_name).subquery()
    
    results =  db.query(models.WatchOpenCloseSummary.OpenPrice,
        models.WatchOpenCloseSummary.HighestPrice,
        models.WatchOpenCloseSummary.LowestPrice,
        models.WatchOpenCloseSummary.LastSalePrice,
        models.WatchOpenCloseSummary.TotalSharesTraded,
        models.WatchOpenCloseSummary.TotalValueTradedin1000,
        models.WatchOpenCloseSummary.WatchOCS_Date,
        ).filter(models.WatchOpenCloseSummary.SecurityNumber.in_(symbol_id_subquery)).all()
    ohlc = [{'open': round(item[0], 2),
                'high': round(item[1], 2),
                'low': round(item[2], 2),
                'close': round(item[3], 2),
                'volume': round(item[4], 2),
                'value': round(item[5], 2) * 1000,
                'date': item[6].date()
                }
                for item in results]
    out = { 'symbol': symbol_name,
            'data': ohlc}
    return out

def get_prices_pct_change(db: Session, symbol_name: str):
    symbol_id_subquery = db.query(models.vStockAndIndex.ID).filter(models.vStockAndIndex.Name == symbol_name).subquery()
    
    results =  db.query(models.WatchOpenCloseSummary.OpenPrice,
        models.WatchOpenCloseSummary.HighestPrice,
        models.WatchOpenCloseSummary.LowestPrice,
        models.WatchOpenCloseSummary.LastSalePrice,
        models.WatchOpenCloseSummary.TotalSharesTraded,
        models.WatchOpenCloseSummary.TotalValueTradedin1000,
        models.WatchOpenCloseSummary.WatchOCS_Date,
        ).filter(models.WatchOpenCloseSummary.SecurityNumber.in_(symbol_id_subquery)).order_by(models.WatchOpenCloseSummary.WatchOCS_Date.desc()).limit(2).all()
    ohlc = [{'open': round(item[0], 2),
                'high': round(item[1], 2),
                'low': round(item[2], 2),
                'close': round(item[3], 2),
                'volume': round(item[4], 2),
                'value': round(item[5], 2) * 1000,
                'date': item[6].date()
                }
                for item in results]
    out = { **ohlc[0],
            'change': ohlc[0]['close']-ohlc[1]['close'],
            'pct_change': round((ohlc[0]['close']-ohlc[1]['close'])/ohlc[1]['close']*100, 2)}
    return out


def get_fundamentalbyquote(db: Session):
    return db.query(models.vStockFundamentalByQuote2).all()

def get_factsheet(symbol_name: str, db: Session):
    symbol = db.query(models.vStockAndIndex).filter(models.vStockAndIndex.Name == symbol_name.upper()).first()
    if symbol is None:
        return None
    resultproxy = db.get_bind().execute(f'SELECT * FROM fnStockfundamentalByFactsheetYOY({symbol.ID})')
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output

def get_factsheet_with_feature(symbol_name: str, feature_name: str, db: Session):
    symbol = db.query(models.vStockAndIndex).filter(models.vStockAndIndex.Name == symbol_name.upper()).first()
    if symbol is None:
        return None
    selected_feature = ['id', 'SecurityNumber', 'Fiscal', 'Quarter', 'FinanceDate', feature_name]
    resultproxy = db.get_bind().execute(f'SELECT * FROM fnStockfundamentalByFactsheetYOY({symbol.ID})')
    output = [{column: value for column, value in rowproxy.items() if column in selected_feature} for rowproxy in resultproxy]
    return output


def get_industries(db:Session):
    return db.query(models.IndustryNo).all()

def get_sectors(db:Session):
    return db.query(models.SectorNo).all()

def get_symbol_from_sector(sector_number: int, db:Session):
    resultproxy =  db.get_bind().execute(f"""SELECT * FROM DBMarketWatchMaster.dbo.d_Compsec
JOIN DBMarketWatchMaster.dbo.SectorNo  ON (DBMarketWatchMaster.dbo.d_Compsec.SectorNo = DBMarketWatchMaster.dbo.SectorNo.SectorNumber)
WHERE DBMarketWatchMaster.dbo.d_Compsec.SectorNo = {sector_number} AND DBMarketWatchMaster.dbo.d_Compsec.ListingStatus = 'L'""")
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output

def get_features(db: Session):
    resultproxy = db.get_bind().execute("""SELECT DISTINCT  AccountCode, AccountNameEN FROM DBMarketWatchMaster.dbo.d_Account """)
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output


def get_finance_by_sector(sector_id: int, feature_id: int, db: Session):
    resultproxy = db.get_bind().execute(f"""SELECT * FROM DBMarketWatchMaster.dbo.d_Finance as finance
    WHERE finance.SecurityID IN (
	SELECT SecurityID FROM DBMarketWatchMaster.dbo.d_Compsec
	JOIN DBMarketWatchMaster.dbo.SectorNo  ON (DBMarketWatchMaster.dbo.d_Compsec.SectorNo = DBMarketWatchMaster.dbo.SectorNo.SectorNumber)
	WHERE DBMarketWatchMaster.dbo.d_Compsec.SectorNo = '{sector_id}' AND DBMarketWatchMaster.dbo.d_Compsec.ListingStatus = 'L'
	)
	AND finance.Fiscal = '2020' 
	AND finance.Quarter = '3'
	AND finance.AccountID = '{feature_id}'
	AND finance.FinancialStatementType = 'U'""")
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output

def get_businessinfo(symbol_name: str, db: Session):
    resultproxy = db.get_bind().execute(f"""SELECT * FROM DBMarketWatchMaster.dbo.d_Business 
	WHERE SecuritySymbol = '{symbol_name}'""")
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output

def get_set_trade_summary(start: str, end: str, db: Session):
    query_string = f"""SELECT TOP 300 WatchOCS_Date AS date, 
                        ROUND(OpenPrice,2) AS SETopen,
                        ROUND(HighestPrice,2) AS SEThigh,
                        ROUND(LowestPrice,2) AS SETlow, 
                        ROUND(LastSalePrice,2) AS SETclose,
                        FundValBuy,FundValSell,
                        ForeignValBuy,ForeignValSell,
                        TradingValBuy,TradingValSell,
                        CustomerValBuy,CustomerValSell,
                        FundValBuy-FundValSell AS FundValNet,
                        ForeignValBuy-ForeignValSell AS ForeignValNet, 
                        TradingValBuy-TradingValSell AS TradingValNet, 
                        CustomerValBuy-CustomerValSell AS CustomerValNet
                        FROM DBMarketWatchMaster.dbo.WatchOpenCloseSummary LEFT JOIN DBMarketWatchMaster.dbo.d_CustomerHistory ON WatchOCS_Date = SeqDate 
                        WHERE DBMarketWatchMaster.dbo.WatchOpenCloseSummary.SecurityNumber = 1024 AND DBMarketWatchMaster.dbo.d_CustomerHistory.SecurityNumber = 1024
                        AND WatchOCS_Date >= '{start}' AND WatchOCS_Date <= '{end}'
                        ORDER BY WatchOCS_Date DESC"""
    resultproxy = db.get_bind().execute(query_string)
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output


def get_tfex_trade_summary(start: str, end: str, db: Session):
    query_string = f"""SELECT TOP 300 WatchOCS_Date AS date, 
                        ROUND(OpenPrice,2) AS SETopen,
                        ROUND(HighestPrice,2) AS SEThigh,
                        ROUND(LowestPrice,2) AS SETlow, 
                        ROUND(LastSalePrice,2) AS SETclose,
                        FundValBuy,FundValSell,
                        ForeignValBuy,ForeignValSell
                        CustomerValBuy,CustomerValSell,
                        FundValBuy-FundValSell AS FundValNet,
                        ForeignValBuy-ForeignValSell AS ForeignValNet,
                        CustomerValBuy-CustomerValSell AS CustomerValNet
                        FROM DBMarketWatchMaster.dbo.WatchOpenCloseSummary LEFT JOIN DBMarketWatchMaster.dbo.d_CustomerHistory ON WatchOCS_Date = SeqDate 
                        WHERE DBMarketWatchMaster.dbo.WatchOpenCloseSummary.SecurityNumber = 1025 AND DBMarketWatchMaster.dbo.d_CustomerHistory.SecurityNumber = 1025
                        AND WatchOCS_Date >= '{start}' AND WatchOCS_Date <= '{end}'
                        ORDER BY WatchOCS_Date DESC"""
    resultproxy = db.get_bind().execute(query_string)
    output = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]
    return output