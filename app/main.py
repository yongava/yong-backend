from typing import List

from fastapi import Depends, FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware

from . import crud, models, schemas
from .database import SessionLocal, engine
from sqlalchemy.orm import Session

from bs4 import BeautifulSoup

import datetime
import json
import pandas
import requests
import urllib.request

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