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

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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
    t = type(output)
    return {"type":f"{t}"}
    # df = pandas.DataFrame(json.dumps(output))
    # df['FundValNetSum']    = round(df['FundValNet'].cumsum(),2)
    # df['ForeignValNetSum'] = round(df['ForeignValNet'].cumsum(),2)
    # df['TradingValNetSum'] = round(df['TradingValNet'].cumsum(),2)
    # df['CustomerValSum']   = round(df['CustomerValNet'].cumsum(),2)
    # df.date = pandas.to_datetime(df.date)
    # df.date = df.date.astype(str)
    # result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    # return result

@app.get("/tradesum_tfex/")
def tradesum_tfex(start: str='None', end: str=datetime.datetime.today().strftime('%Y-%m-%d'),db: Session = Depends(get_db)):
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
    except Exception as e:
        output = None
        result = {"status":"FAILURE","message":f"{e}"}

    try:
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

        if start == 'None':
            df = df.sort_index(ascending=False)
            df = df.head(300)
        else:
            df = df[start:end]
            df = df.sort_index(ascending=False)

        df.index = df.index.astype(str)
        df = df.reset_index()
        result = json.loads(df.to_json(orient='records',date_format ='ISO'))
    except Exception as e:
        result = {"status":"FAILURE","message":f"{e}"}
    return result