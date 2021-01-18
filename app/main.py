from typing import List

from fastapi import Depends, FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware

from . import crud, models, schemas
from .database import SessionLocal, engine
from sqlalchemy.orm import Session

from azure.storage.blob import BlobClient

from bs4 import BeautifulSoup

import datetime
import json
import pandas
import requests
import urllib.request
import pathlib

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # allow_origin_regex=['*',"http://127.0.0.1:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/symbols/")
def read_symbols(db: Session = Depends(get_db)):
    result = crud.get_symbols(db)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/symbol/id/{symbol_id}")
def read_symbol_name(symbol_id: int = Path(..., title=" The ID of the symbol to get"), db: Session = Depends(get_db)):
    result = crud.get_symbol_name(db, symbol_id=symbol_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/symbol/name/{symbol_name}")
def read_symbol_id(symbol_name: str = Path(..., title=" The name of the symbol to get"), db: Session = Depends(get_db)):
    result = crud.get_symbol_id(db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result


@app.get("/prices/{symbol_name}")
def read_prices(symbol_name: str, db: Session = Depends(get_db)):
    result = crud.get_prices(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/ohlcvv/{symbol_name}/{length}")
def read_ohlcvv(symbol_name: str, length: int, db: Session = Depends(get_db)):
    result = crud.get_ohlcvv(db=db, symbol_name=symbol_name, length=length)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/prices/recent/{symbol_name}")
def read_prices_pct_change(symbol_name: str, db: Session = Depends(get_db)):
    result = crud.get_prices_pct_change(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/businessinfo/{symbol_name}")
def read_businessinfo(symbol_name: str = Path(..., title=" The name of the symbol to get"), db: Session = Depends(get_db)):
    result = crud.get_businessinfo(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/industry/")
def read_industries(db: Session = Depends(get_db)):
    result = crud.get_industries(db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/sector/")
def read_sectors(db: Session = Depends(get_db)):
    result = crud.get_sectors(db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/sector/{sector_number}")
def read_symbol_from_sector(sector_number: str,db: Session = Depends(get_db)):
    result = crud.get_symbol_from_sector(sector_number, db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/finance_by_sector")
def read_finance_by_sector(sector_id: int, feature_id: int, db: Session = Depends(get_db)):
    result = crud.get_finance_by_sector(sector_id, feature_id, db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/setmaiinfo")
def setmaiinfo():
    def set_info():
        page = urllib.request.urlopen('https://marketdata.set.or.th/mkt/marketsummary.do?market=SET&language=en&country=US')
        soup = BeautifulSoup(page, 'html.parser')
        table_rows = soup.findAll('div', attrs={'class': 'row info'})
        l = []
        for tr in table_rows:
            td = tr.find_all('div')
            row = [tr.text.replace(" ","").replace("*","").replace("\r","").replace("\n","") for tr in td]
            if len(row) > 0:
                l.append(row)
        df = pandas.DataFrame(l, columns=['name','value'])
        df = df.set_index('name').drop('IndexPerformance')
        return df.to_json().replace("\\","")
    
    def mai_info():
        page = urllib.request.urlopen('https://marketdata.set.or.th/mkt/marketsummary.do?market=mai&language=en&country=US')
        soup = BeautifulSoup(page, 'html.parser')
        table_rows = soup.findAll('div', attrs={'class': 'row info'})
        l = []
        for tr in table_rows:
            td = tr.find_all('div')
            row = [tr.text.replace(" ","").replace("*","").replace("\r","").replace("\n","") for tr in td]
            if len(row) > 0:
                l.append(row)
        df = pandas.DataFrame(l, columns=['name','value'])
        df = df.set_index('name').drop('IndexPerformance')
        return df.to_json().replace("\\","")
    try:
        data = { 'set' : json.loads(set_info())['value'], 'mai' : json.loads(mai_info())['value'] }
        result =  data
    except:
        result = {"status":"FAILURE","message":"Can't get data"}
        
    return result

@app.get("/recent_tradesum_tfex")
def recent_tradesum_tfex():
    try:
        page = urllib.request.urlopen('https://marketdata.set.or.th/tfx/tfexinvestortypetrading.do?locale=th_TH')
        soup = BeautifulSoup(page, 'html.parser')
        table = soup.find('tbody',)
        table_rows = table.findAll('tr')
        l = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.replace(" ","").replace("\r","").replace("\n","") for tr in td]
            if len(row) > 0:
                l.append(row)
        df = pandas.DataFrame(l, columns=["name", "i_buy", "i_sell", "i_net", "f_buy", "f_sell", "f_net", "l_buy", "l_sell", "l_net", "total"])
        result = {'date': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'FundValBuy':     float(df.at[1,'i_buy'].replace(',','')),
                    'FundValSell':    float(df.at[1,'i_sell'].replace(',','')),
                    'FundValNet':     float(df.at[1,'i_net'].replace('+','').replace(',','')),
                    'ForeignValBuy':  float(df.at[1,'f_buy'].replace(',','')),
                    'ForeignValSell': float(df.at[1,'f_sell'].replace(',','')),
                    'ForeignValNet':  float(df.at[1,'f_net'].replace('+','').replace(',','')),
                    'CustomerValBuy': float(df.at[1,'l_buy'].replace(',','')),
                    'CustomerValSell':float(df.at[1,'l_sell'].replace(',','')),
                    'CustomerValNet': float(df.at[1,'l_net'].replace('+','').replace(',',''))}
    except Exception as e:
        result = {"status":"FAILURE","message":f"{e}"}
    return result

@app.get("/tradesum_set/")
def tradesum_set(start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    output = crud.get_set_trade_summary(start, end, db)

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")

    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()

    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['TradingValNetSum'] = round(df['TradingValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)

    df = df.reset_index()
    df.date = df.date.astype(str)
    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result

@app.get("/tradesum_set/recent/{period}")
def tradesum_set_recent(period: str='RECENT', start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    output = crud.get_set_trade_summary(start, end, db)

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")

    date = datetime.datetime.now()
    first_date_Q = datetime.datetime(date.year,3*((date.month-1)//3)+1,1).date()
    first_date_M = datetime.datetime.today().replace(day=1).date()
    first_date_Y = datetime.datetime.today().replace(day=1,month=1).date()

    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()

    if period == 'MTD':
        df = df[str(first_date_M):]
    elif period == 'QTD':
        df = df[str(first_date_Q):]
    elif  period == 'YTD':
        df = df[str(first_date_Y):]
    else:
        df = df.tail(1)

    df['FundValBuySum']    = round(df['FundValBuy'].astype('float').cumsum(),2)
    df['ForeignValBuySum'] = round(df['ForeignValBuy'].astype('float').cumsum(),2)
    df['TradingValBuySum'] = round(df['TradingValBuy'].astype('float').cumsum(),2)
    df['CustomerValBuySum']   = round(df['CustomerValBuy'].astype('float').cumsum(),2)

    df['FundValSellSum']    = round(df['FundValSell'].astype('float').cumsum(),2)
    df['ForeignValSellSum'] = round(df['ForeignValSell'].astype('float').cumsum(),2)
    df['TradingValSellSum'] = round(df['TradingValSell'].astype('float').cumsum(),2)
    df['CustomerValSellSum']   = round(df['CustomerValSell'].astype('float').cumsum(),2)

    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['TradingValNetSum'] = round(df['TradingValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)
    
    df = df.tail(1)

    df = df.sort_index(ascending=False).reset_index()
    df.date = df.date.astype(str)

    df = df[['date','FundValBuySum','FundValSellSum','FundValNetSum','ForeignValBuySum','ForeignValSellSum','ForeignValNetSum','TradingValBuySum','TradingValSellSum','TradingValNetSum','CustomerValBuySum','CustomerValSellSum','CustomerValNetSum']]

    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result

@app.get("/tradesum_tfex_db/")
def tradesum_tfex_db(start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    output = crud.get_tfex_trade_summary(start, end, db)

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")

    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()

    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)

    df = df.reset_index()
    df.date = df.date.astype(str)
    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result


@app.get("/tradesum_tfex_db/recent/{period}")
def tradesum_tfex_db_recent(period: str='RECENT', start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    output = crud.get_tfex_trade_summary(start, end, db)

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")

    date = datetime.datetime.now()
    first_date_Q = datetime.datetime(date.year,3*((date.month-1)//3)+1,1).date()
    first_date_M = datetime.datetime.today().replace(day=1).date()
    first_date_Y = datetime.datetime.today().replace(day=1,month=1).date()

    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()

    if period == 'MTD':
        df = df[str(first_date_M):]
    elif period == 'QTD':
        df = df[str(first_date_Q):]
    elif  period == 'YTD':
        df = df[str(first_date_Y):]
    else:
        df = df.tail(1)

    df['FundValBuySum']    = round(df['FundValBuy'].astype('float').cumsum(),2)
    df['ForeignValBuySum'] = round(df['ForeignValBuy'].astype('float').cumsum(),2)
    df['CustomerValBuySum']   = round(df['CustomerValBuy'].astype('float').cumsum(),2)

    df['FundValSellSum']    = round(df['FundValSell'].astype('float').cumsum(),2)
    df['ForeignValSellSum'] = round(df['ForeignValSell'].astype('float').cumsum(),2)
    df['CustomerValSellSum']   = round(df['CustomerValSell'].astype('float').cumsum(),2)

    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)
    
    df = df.tail(1)

    df = df.sort_index(ascending=False).reset_index()
    df.date = df.date.astype(str)

    df = df[['date','FundValBuySum','FundValSellSum','FundValNetSum','ForeignValBuySum','ForeignValSellSum','ForeignValNetSum','CustomerValBuySum','CustomerValSellSum','CustomerValNetSum']]

    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result


@app.get("/tradesum_tfex/")
def tradesum_tfex(start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    def get_crawl():
        try:
            page = urllib.request.urlopen('https://marketdata.set.or.th/tfx/tfexinvestortypetrading.do?locale=th_TH')
            soup = BeautifulSoup(page, 'html.parser')
            table = soup.find('tbody',)
            table_rows = table.findAll('tr')
            l = []
            for tr in table_rows:
                td = tr.find_all('td')
                row = [tr.text.replace(" ","").replace("\r","").replace("\n","") for tr in td]
                if len(row) > 0:
                    l.append(row)
            df = pandas.DataFrame(l, columns=["name", "i_buy", "i_sell", "i_net", "f_buy", "f_sell", "f_net", "l_buy", "l_sell", "l_net", "total"])
            output = {'date': datetime.datetime.today().strftime('%Y-%m-%d'),
                        'FundValNet':     float(df.at[1,'i_net'].replace('+','').replace(',','')),
                        'ForeignValNet':  float(df.at[1,'f_net'].replace('+','').replace(',','')),
                        'CustomerValNet': float(df.at[1,'l_net'].replace('+','').replace(',',''))}
        except:
            return pandas.DataFrame()

        connection_string = "DefaultEndpointsProtocol=https;AccountName=alpharesearch;AccountKey=v1zCpiYiSgIzXgb58YI9tA3ebi1OtyoMeA6cu2vFzmk94zxC4DepNWlT8+dpsNELDFq+0owUrY1gehvCzSFZ6A==;EndpointSuffix=core.windows.net"
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="yongcontainer", blob_name="my_csv")
        with open("tfex-trade-history.csv", "wb") as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
        df = pandas.read_csv('tfex-trade-history.csv', thousands=',')
        df = df.append(output,ignore_index=True)
        df = df.set_index('date')
        df.index = pandas.to_datetime(df.index)
        df = df.apply(pandas.to_numeric)
        df = df[~df.index.duplicated(keep='last')]
        df.to_csv('tfex-trade-history.csv')
        with open("tfex-trade-history.csv", "rb") as data:
            blob.upload_blob(data, overwrite=True)
        return df

    output = crud.get_tfex_trade_summary(start, end, db)
    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()
    df = df.drop(['FundValBuy','FundValSell','FundValNet','ForeignValBuy','ForeignValSell','ForeignValNet','CustomerValBuy','CustomerValSell','CustomerValNet'],axis=1)
    upd_df = get_crawl()
    df = df.join(upd_df, how='left')
    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)
    df = df.sort_index(ascending=True).reset_index()
    df.date = df.date.astype(str)
    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result


@app.get("/tradesum_tfex/recent/{period}")
def tradesum_tfex_recent(period: str='RECENT', start: str='2015-01-01', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
    def get_crawl():
        try:
            page = urllib.request.urlopen('https://marketdata.set.or.th/tfx/tfexinvestortypetrading.do?locale=th_TH')
            soup = BeautifulSoup(page, 'html.parser')
            table = soup.find('tbody',)
            table_rows = table.findAll('tr')
            l = []
            for tr in table_rows:
                td = tr.find_all('td')
                row = [tr.text.replace(" ","").replace("\r","").replace("\n","") for tr in td]
                if len(row) > 0:
                    l.append(row)
            df = pandas.DataFrame(l, columns=["name", "i_buy", "i_sell", "i_net", "f_buy", "f_sell", "f_net", "l_buy", "l_sell", "l_net", "total"])
            output = {'date': datetime.datetime.today().strftime('%Y-%m-%d'),
                        'FundValNet':     float(df.at[1,'i_net'].replace('+','').replace(',','')),
                        'ForeignValNet':  float(df.at[1,'f_net'].replace('+','').replace(',','')),
                        'CustomerValNet': float(df.at[1,'l_net'].replace('+','').replace(',',''))}
        except:
            return pandas.DataFrame()

        connection_string = "DefaultEndpointsProtocol=https;AccountName=alpharesearch;AccountKey=v1zCpiYiSgIzXgb58YI9tA3ebi1OtyoMeA6cu2vFzmk94zxC4DepNWlT8+dpsNELDFq+0owUrY1gehvCzSFZ6A==;EndpointSuffix=core.windows.net"
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="yongcontainer", blob_name="my_csv")
        with open("tfex-trade-history.csv", "wb") as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
        df = pandas.read_csv('tfex-trade-history.csv', thousands=',')
        df = df.append(output,ignore_index=True)
        df = df.set_index('date')
        df.index = pandas.to_datetime(df.index)
        df = df.apply(pandas.to_numeric)
        df = df[~df.index.duplicated(keep='last')]
        df.to_csv('tfex-trade-history.csv')
        with open("tfex-trade-history.csv", "rb") as data:
            blob.upload_blob(data, overwrite=True)
        return df

    output = crud.get_tfex_trade_summary(start, end, db)
    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    df = pandas.DataFrame(output)
    df.date = pandas.to_datetime(df.date)
    df = df.set_index('date').sort_index()
    df = df.drop(['FundValBuy','FundValSell','FundValNet','ForeignValBuy','ForeignValSell','ForeignValNet','CustomerValBuy','CustomerValSell','CustomerValNet'],axis=1)
    upd_df = get_crawl()
    df = df.join(upd_df, how='left')

    date = datetime.datetime.now()
    first_date_Q = datetime.datetime(date.year,3*((date.month-1)//3)+1,1).date()
    first_date_M = datetime.datetime.today().replace(day=1).date()
    first_date_Y = datetime.datetime.today().replace(day=1,month=1).date()

    if period == 'MTD':
        df = df[str(first_date_M):]
    elif period == 'QTD':
        df = df[str(first_date_Q):]
    elif  period == 'YTD':
        df = df[str(first_date_Y):]
    else:
        df = df.tail(1)

    df['FundValNetSum']    = round(df['FundValNet'].astype('float').cumsum(),2)
    df['ForeignValNetSum'] = round(df['ForeignValNet'].astype('float').cumsum(),2)
    df['CustomerValNetSum']   = round(df['CustomerValNet'].astype('float').cumsum(),2)
    
    df = df.tail(1)

    df = df.sort_index(ascending=False).reset_index()
    df.date = df.date.astype(str)

    df = df[['date','FundValNetSum','ForeignValNetSum','CustomerValNetSum']]

    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result

@app.get("/marketbreadth/")
def marketbreadth():
    pandas.options.display.float_format = '{:,.2f}'.format
    def get_is_ath(series, period = 260):
        period = 260
        ath_threshold = series.shift(1).rolling(period).max()
        is_ath = (series >= ath_threshold).astype(int)
        return is_ath

    def get_is_atl(series, period = 260):
        atl_threshold = series.shift(1).rolling(period).min()
        is_atl = (series <= atl_threshold).astype(int)
        return is_atl

    def get_above_is_above_sma(series, period = 100):
        sma_threshold = series.shift(1).rolling(period).mean()
        is_above_sma = (series >= sma_threshold).astype(int)
        return is_above_sma

    def get_above_is_below_sma(series, period = 100):
        sma_threshold = series.shift(1).rolling(period).mean()
        is_below_sma = (series <  sma_threshold).astype(int)
        return is_below_sma

    SET    = ['7UP','A','AAV','ABPIF','ACC','ACE','ADVANC','AEC','AEONTS','AFC','AH','AHC','AI','AIMCG','AIMIRT','AIT','AJ','AJA','AKR','ALLA','ALT','ALUCON','AMANAH','AMARIN','AMATA','AMATAR','','AMC','ANAN','AOT','AP','APCO','APCS','APEX','APURE','AQ','AQUA','AS','ASAP','ASEFA','ASIA','ASIAN','ASIMAR','ASK','ASP','AWC','AYUD','B52','B','BA','BAFS','BAM','BANPU','BAT-3K','BAY','BBL','BCH','BCP','BCPG','BCT','BDMS','BEAUTY','BEC','BEM','BFIT','BGC','BGRIM','BH','BIG','BJC','BJCHI','BKD','BKER','BKI','BKKCP','BLA','BLAND','BLISS','BOFFICE','BPP','BR','BROCK','BRR','BRRGIF','BSBM','BTNC','BTS','BTSGIF','BUI','BWG','B-WORK','CBG','CCET','CCP','CEN','CENTEL','CFRESH','CGD','CGH','CHARAN','CHG','CHOTI','CI','CIMBT','CITY','CK','CKP','CM','CMAN','CMR','CNT','COL','COM7','COTTO','CPALL','CPF','CPH','CPI','CPL','CPN','CPNCG','CPNREIT','CPT','CPTGF','CPW','CRANE','CRC','CSC','CSP','CSR','CSS','CTARAF','CTW','CWT','DCC','DCON','DDD','DELTA','DEMCO','DIF','DOHOME','DREIT','DRT','DTAC','DTC','DTCI','EA','EASON','EASTW','ECL','EE','EGATIF','EGCO','EKH','EMC','EP','EPG','ERW','ERWPF','ESSO','ESTAR','EVER','F&D','FANCY','FE','FMT','FN','FNS','FORTH','FPT','FSS','FTE','FTREIT','FUTUREPF','GAHREIT','GBX','GC','GEL','GENCO','GFPT','GGC','GIFT','GJS','GL','GLAND','GLOBAL','GLOCON','GOLD','GOLDPF','GPI','GPSC','GRAMMY','GRAND','GREEN','GSTEEL','GULF','GUNKUL','GVREIT','GYT','HANA','HFT','HMPRO','HPF','HREIT','HTC','HTECH','HUMAN','ICC','ICHI','IFEC','IFS','IHL','III','ILINK','ILM','IMPACT','INET','INGRS','INOX','INSURE','INTUCH','IRC','IRPC','IT','ITD','IVL','J','JAS','JASIF','JCK','JCT','JMART','JMT','JTS','JUTHA','JWD','KAMART','KBANK','KBS','KC','KCAR','KCE','KDH','KGI','KKC','KKP','KPNPF','KSL','KTB','KTC','KTIS','KWC','KWG','KYE','L&E','LALIN','LANNA','LEE','LH','LHFG','LHHOTEL','LHK','LHPF','LHSC','LOXLEY','LPH','LPN','LRH','LST','LUXF','M','MACO','MAJOR','MAKRO','MALEE','MANRIN','MATCH','MATI','MAX','MBK','MBKET','MC','M-CHAI','MCOT','MCS','MDX','MEGA','METCO','MFC','MFEC','MIDA','M-II','MILL','MINT','MIPF','MIT','MJD','MJLF','MK','ML','MNIT','MNIT2','MNRF','MODERN','MONO','M-PAT','MPIC','MSC','M-STOR','MTC','MTI','NC','NCH','NEP','NER','NEW','NEX','NFC','NKI','NMG','NNCL','NOBLE','NOK','NSI','NTV','NVD','NUSA','NWR','NYT','OCC','OGC','OHTL','OISHI','ORI','OSP','PACE','PAE','PAF','PAP','PATO','PB','PCSGH','PDI','PDJ','PE','PERM','PF','PG','PK','PL','PLANB','PLAT','PLE','PM','PMTA','POLAR','POPF','PORT','POST','PPF','PPP','PPPM','PR9','PRAKIT','PREB','PRECHA','PRIME','PRG','PRIN','PRINC','PRM','PRO','PSH','PSL','PT','PTG','PTL','PTT','PTTEP','PTTGC','PYLON','Q-CON','QH','QHHR','QHOP','QHPF','RAM','RATCH','RBF','RCI','RCL','RICH','RICHY','RJH','RML','ROCK','ROH','ROJNA','RPC','RPH','RS','RSP','S','S & J','S11','SABINA','SAM','SAMART','SAMCO','SAMTEL','SAPPE','SAT','SAUCE','SAWAD','SAWANG','SBPF','SC','SCB','SCC','SCCC','SCG','SCI','SCN','SCP','SDC','SEAFCO','SE-ED','SEG','SENA','SF','SFLEX','SFP','SGP','SHANG','SHR','SHREIT','SIAM','SINGER','SIRI','SIRIP','SIS','SISB','SITHAI','SKE','SKN','SKR','SLP','SMIT','SMK','SMPC','SMT','SNC','SNP','SOLAR','SORKON','SPACK','SPALI','SPC','SPCG','SPF','SPG','SPI','SPRC','SPRIME','SQ','SRICHA','SRIPANWA','SSC','SSF','SSI','SSP','SSPF','SSSC','SST','SSTRT','STA','STANLY','STARK','STEC','STHAI','STPI','SUC','SUPER','SUPEREIF','SUSCO','SUTHA','SVH','SVI','SVOA','SYMC','SYNEX','SYNTEC','TAE','TASCO','TBSP','TC','TCAP','TCC','TCCC','TCJ','TCMC','TCOAT','TEAM','TEAMG','TFFIF','TFG','TFI','TFMAMA','TGPRO','TH','THAI','THANI','THCOM','THE','THG','THIP','THL','THRE','THREL','TIF1','TIP','TIPCO','TISCO','TIW','TK','TKN','TKS','TKT','TLGF','TLHPF','TMB','TMD','TMT','TNITY','TNL','TNPC','TNPF','TNR','TOA','TOG','TOP','TOPP','TPA','TPBI','TPCORP','TPIPL','TPIPP','TPOLY','TPP','TPRIME','TQM','TR','TRC','TRITN','TRU','TRUBB','TRUE','TSC','TSE','TSI','TSR','TSTE','TSTH','TTA','TTCL','TTI','TTLPF','TTT','TTW','TU','TU-PF','TVI','TVO','TWP','TWPC','TWZ','TYCN','U','UAC','UMI','UNIQ','UOBKH','UP','UPF','UPOIC','URBNPF','UT','UTP','UV','UVAN','VARO','VGI','VIBHA','VIH','VNG','VNT','VPO','VRANDA','W','WACOAL','WAVE','WG','WHA','WHABT','WHART','WHAUP','WICE','WIIK','WIN','WORK','WP','WPH','YCI','ZEN','ZMICO']
    SET100 = ["AAV","ACE","ADVANC","AEONTS","AMATA","AOT","AP","AWC","BANPU","BBL","BCH","BCP","BCPG","BDMS","BEC","BEM","BGRIM","BH","BJC","BPP","BTS","CBG","CENTEL","CHG","CK","CKP","COM7","CPALL","CPF","CPN","CRC","DOHOME","DTAC","EA","EGCO","EPG","ERW","ESSO","GFPT","GLOBAL","GPSC","GULF","GUNKUL","HANA","HMPRO","INTUCH","IRPC","IVL","JAS","JMT","KBANK","KCE","KKP","KTB","KTC","LH","MAJOR","MEGA","MINT","MTC","ORI","OSP","PLANB","PRM","PSH","PTG","PTT","PTTEP","PTTGC","QH","RATCH","RBF","RS","SAWAD","SCB","SCC","SGP","SIRI","SPALI","SPRC","STA","STEC","SUPER","TASCO","TCAP","THANI","TISCO","TKN","TMB","TOA","TOP","TPIPP","TQM","TRUE","TTW","TU","TVO","VGI","WHA","WHAUP"]
    SET50  = ["ADVANC","AOT","AWC","BBL","BDMS","BEM","BGRIM","BH","BJC","BPP","BTS","CBG","CPALL","CPF","CPN","CRC","DTAC","EA","EGCO","GLOBAL","GPSC","GULF","HMPRO","INTUCH","IRPC","IVL","KBANK","KTB","KTC","LH","MINT","MTC","OSP","PTT","PTTEP","PTTGC","RATCH","SAWAD","SCB","SCC","TCAP","TISCO","TMB","TOA","TOP","TRUE","TTW","TU","VGI","WHA"]
    MAI    = ["ABICO","AU","JCKH","KASET","MM","SUN","TACC","TMILL","XO","BGT","BIZ","DOD","ECF","HPT","IP","JUBILE","MOONG","NPK","OCEAN","TM","ACAP","AF","AIRA","ASN","BROOK","CHAYO","GCAP","LIT","MITSIB","SGF","2S","ADB","BM","CHO","CHOW","CIG","COLOR","CPR","FPI","GTB","KCM","KUMWEL","KWM","MBAX","MGT","NDR","PDG","PIMO","PJW","PPM","RWI","SALEE","SANKO","SELIC","SWC","TMC","TMI","TMW","TPAC","TPLAS","UBIS","UEC","UKEM","UREKA","YUASA","ZIGA","ALL","ARIN","ARROW","BC","BSM","BTW","CAZ","CHEWA","CMC","CRD","DIMET","FLOYD","HYDRO","JSP","K","KUN","META","PPS","PROUD","SMART","STAR","STC","STI","T","TAPAC","THANA","TIGER","TITLE","ABM","AGE","AIE","PSTC","QTC","SAAM","SEAOIL","SR","TAKUNI","TPCH","TRT","UMS","UPA","UWC","A5","AKP","AMA","ARIP","ATP30","AUCT","BOL","CMO","D","DCORP","EFORL","ETE","FSMART","FVC","GSC","HARN","IMH","JKN","KIAT","KOOL","LDC","MORE","MPG","MVP","NBC","NCL","NEWS","NINE","OTO","PHOL","PICO","QLT","RP","SE","SLM","SONIC","SPA","THMUI","TNDT","TNH","TNP","TSF","TVD","TVT","VL","WINNER","YGG","APP","COMAN","ICN","IIG","INSET","IRCP","ITEL","NETBAY","PLANET","SICT","SIMAT","SKY","SPVI","TPS","VCOM"]

    INDEX_df       = pandas.read_csv('https://alpharesearch.blob.core.windows.net/yongcontainer/INDEX.csv').set_index('DATE')
    INDEX_df.index = pandas.to_datetime(INDEX_df.index)

    SET_df         = INDEX_df[['SET_OPEN','SET_HIGH','SET_LOW','SET_CLOSE']]
    SET_df.columns = ['OPEN','HIGH','LOW','CLOSE']
    SET100_df      = INDEX_df[['SET100_OPEN','SET100_HIGH','SET100_LOW','SET100_CLOSE']]
    SET100_df.columns = ['OPEN','HIGH','LOW','CLOSE']
    SET50_df       = INDEX_df[['SET50_OPEN','SET50_HIGH','SET50_LOW','SET50_CLOSE']]
    SET50_df.columns = ['OPEN','HIGH','LOW','CLOSE']
    MAI_df         = INDEX_df[['MAI_OPEN','MAI_HIGH','MAI_LOW','MAI_CLOSE']]
    MAI_df.columns = ['OPEN','HIGH','LOW','CLOSE']
        
    prices_df = pandas.read_csv('https://alpharesearch.blob.core.windows.net/yongcontainer/STOCKS.csv').set_index('DATE')
    prices_df.index = pandas.to_datetime(prices_df.index)

    vol_df = pandas.read_csv('https://alpharesearch.blob.core.windows.net/yongcontainer/STOCKS_VOL.csv').set_index('DATE')
    vol_df.index = pandas.to_datetime(vol_df.index)

    val_df = pandas.read_csv('https://alpharesearch.blob.core.windows.net/yongcontainer/STOCKS_VAL.csv').set_index('DATE')
    val_df.index = pandas.to_datetime(val_df.index)

    stock_amount = len(prices_df.columns)

    is_ath_df = pandas.DataFrame(index=prices_df.index)
    is_atl_df = pandas.DataFrame(index=prices_df.index)
    above_sma_df = pandas.DataFrame(index=prices_df.index)
    below_sma_df = pandas.DataFrame(index=prices_df.index)

    for symbol in prices_df.columns:
        is_ath_df[symbol] = get_is_ath(prices_df[symbol])
        is_atl_df[symbol] = get_is_atl(prices_df[symbol])
        above_sma_df[symbol] = get_above_is_above_sma(prices_df[symbol])
        below_sma_df[symbol] = get_above_is_below_sma(prices_df[symbol])
        
    summary_df = pandas.DataFrame(index=prices_df.index)

    summary_df['number_high']  = is_ath_df.sum(axis=1)
    summary_df['number_low']   = is_atl_df.sum(axis=1)
    summary_df['percent_high'] = round(summary_df['number_high'] / stock_amount*100,2)
    summary_df['percent_low']  = round(summary_df['number_low']  / stock_amount*100,2)

    summary_df['number_above_sma']  = above_sma_df.sum(axis=1)
    summary_df['number_below_sma']  = stock_amount-summary_df['number_above_sma'] 
    summary_df['percent_above_sma'] = round(summary_df['number_above_sma'] / stock_amount*100,2)
    summary_df['percent_below_sma'] = round(summary_df['number_below_sma'] / stock_amount*100,2)

    df_result = SET_df.join(summary_df)

    current_above_sma = above_sma_df.tail(1)
    current_above_sma = current_above_sma[current_above_sma == 1].dropna(axis=1)
    current_above_sma_result = pandas.DataFrame()
    symbol_list = current_above_sma.columns
    current_above_sma_result['symbol'] = symbol_list

    prices  = prices_df.tail(2).reset_index()
    volumes = vol_df.tail(2).reset_index()
    values  = val_df.tail(2).reset_index()

    price_list  = ["{:.2f}".format(round(prices.at[1, s],2))  for s in symbol_list]
    chg_list    = ["{:.2f}".format(round(prices.at[1, s]-prices.at[0, s],2))  for s in symbol_list]
    precent_chg_list = ["{:.2f}".format(round((prices.at[1, s]-prices.at[0, s])/prices.at[0, s]*100,2)) for s in symbol_list]
    volume_list = [int(volumes.at[1, s]) for s in symbol_list]
    value_list  = [int(values.at[1, s])  for s in symbol_list]

    current_above_sma_result['last']       = price_list
    current_above_sma_result['change']     = chg_list
    current_above_sma_result['pct_change'] = precent_chg_list
    current_above_sma_result['volume']     = volume_list
    current_above_sma_result['value']      = value_list

    ath_atl_result = df_result.drop(['number_above_sma','number_below_sma','percent_above_sma','percent_below_sma'],axis=1).tail(250).sort_index(ascending=False).reset_index()
    ath_atl_result.DATE = ath_atl_result.DATE.astype(str)
    sma_result = df_result.drop(['number_high','number_low','percent_high','percent_low'],axis=1).tail(250).sort_index(ascending=False).reset_index()
    sma_result.DATE = sma_result.DATE.astype(str)
    stocks_above_sma = current_above_sma_result.sort_values('value',ascending=False)

    result = {'ath_atl_result':json.loads(ath_atl_result.to_json(orient='records',date_format ='ISO')),
          'sma_result':json.loads(sma_result.to_json(orient='records',date_format ='ISO')),
          'stocks_above_sma':json.loads(stocks_above_sma.to_json(orient='records',date_format ='ISO'))}
    return result

@app.get("/tech_screen_set/")
def tech_screen_set():
    output = requests.get('https://alpharesearch.blob.core.windows.net/yongcontainer/tech_screen_set.json').json()

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")

    result = output
    return result

@app.get("/tech_screen_mai/")
def tech_screen_mai():
    output = requests.get('https://alpharesearch.blob.core.windows.net/yongcontainer/tech_screen_mai.json').json()

    if output is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    
    result = output
    return result

@app.get("/relative/{market_group}")
def relative(market_group: str):
    set_industry = ['.AGRO','.CONSUMP','.FINCIAL','.INDUS','.PROPCON','.RESOURC','.SERVICE','.TECH']
    set_sector = ['.AGRI','.FOOD','.FASHION','.HOME','.PERSON','.BANK','.FIN','.INSUR','.AUTO','.IMM','.PAPER','.PETRO','.PKG','.STEEL','.CONMAT','.CONS','.PROP','.ENERG','.MINE','.COMM','.HELTH','.MEDIA','.PROF','.TOURISM','.TRANS','.ETRON','.ICT']
    mai_sector = ['.AGRO-ms','.CONSUMP-ms','.FINCIAL-ms','.INDUS-ms','.PROPCON-ms','.RESOURC-ms','.SERVICE-ms','.TECH-ms']

    period = 100
    # set_df = pandas.DataFrame(requests.get(f'https://yong.alpha.lab.ai/ohlcvv/SET/{period}').json()['data']).set_index('date').sort_index()
    result = read_ohlcvv('SET', period)
    set_df = pandas.DataFrame(result['data']).set_index('date').sort_index()
    set_df.index = pandas.to_datetime(set_df.index)

    # mai_df = pandas.DataFrame(requests.get(f'https://yong.alpha.lab.ai/ohlcvv/mai/{period}').json()['data']).set_index('date').sort_index()
    result = read_ohlcvv('mai', period)
    mai_df = pandas.DataFrame(result['data']).set_index('date').sort_index()
    mai_df.index = pandas.to_datetime(mai_df.index)

    t = market_group
    if (t == 'SETIndustry'):
        set_industry_df = set_df[['open','high','low','close']]
        for symbol in set_industry:
            # tmp_df = pandas.DataFrame(requests.get(f'https://yong.alpha.lab.ai/ohlcvv/{symbol}/{period}').json()['data']).set_index('date').sort_index()
            result = read_ohlcvv(symbol, period)
            tmp_df = pandas.DataFrame(result['data']).set_index('date').sort_index()
            set_industry_df[symbol.replace('.','')] = tmp_df['close'].divide(set_industry_df['close'])
        df = set_industry_df
    elif (t == 'SETSector'):
        set_sector_df = set_df[['open','high','low','close']]
        for symbol in set_sector:
            # tmp_df = pandas.DataFrame(requests.get(f'https://yong.alpha.lab.ai/ohlcvv/{symbol}/{period}').json()['data']).set_index('date').sort_index()
            result = read_ohlcvv(symbol, period)
            tmp_df = pandas.DataFrame(result['data']).set_index('date').sort_index()
            set_sector_df[symbol.replace('.','')] = tmp_df['close'].divide(set_sector_df['close'])     
        df = set_sector_df
    elif (t == 'MAISector'):
        mai_sector_df = mai_df[['open','high','low','close']]
        for symbol in mai_sector:
            # tmp_df = pandas.DataFrame(requests.get(f'https://yong.alpha.lab.ai/ohlcvv/{symbol}/{period}').json()['data']).set_index('date').sort_index()
            result = read_ohlcvv(symbol, period)
            tmp_df = pandas.DataFrame(result['data']).set_index('date').sort_index()
            mai_sector_df[symbol.replace('-ms','').replace('.','')] = tmp_df['close'].divide(mai_sector_df['close'])
        df = mai_sector_df

    df = df.reset_index()
    result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    return result