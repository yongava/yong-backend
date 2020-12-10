from typing import List

from fastapi import Depends, FastAPI, HTTPException, Path
from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import SessionLocal, engine
import datetime
from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/financial/")
def read_financial_by_date(date: datetime.date, db: Session = Depends(get_db)):
    result = crud.get_financial_by_date(db, date=date)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/fundamentalbyquote/")
def read_fundamentalbyquote(db: Session = Depends(get_db)):
    result = crud.get_fundamentalbyquote(db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result
    pass

@app.get("/prices/{symbol_name}")
def read_prices(symbol_name: str, db: Session = Depends(get_db)):
    result = crud.get_prices(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/prices/pct_change/{symbol_name}")
def read_prices_pct_change(symbol_name: str, db: Session = Depends(get_db)):
    result = crud.get_prices_pct_change(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result
    
@app.get("/factsheet/{symbol_name}")
def read_factsheet(symbol_name: str = Path(..., title=" The name of the symbol to get"), db: Session = Depends(get_db)):
    result = crud.get_factsheet(db=db, symbol_name=symbol_name)
    if result is None:
        raise HTTPException(status_code=404, detail="Symbol not found")
    return result

@app.get("/factsheet/{symbol_name}/{feature_name}")
def read_factsheet(symbol_name: str = Path(..., title=" The name of the symbol to get"), feature_name: str = Path(..., title=" The name of the symbol to get"), db: Session = Depends(get_db)):
    result = crud.get_factsheet_with_feature(db=db, symbol_name=symbol_name, feature_name=feature_name)
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

@app.get("/feature/")
def read_get_features(db: Session = Depends(get_db)):
    result = crud.get_features( db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/finance_by_sector")
def read_finance_by_sector(sector_id: int, feature_id: int, db: Session = Depends(get_db)):
    result = crud.get_finance_by_sector(sector_id, feature_id, db)
    if result is None:
        raise HTTPException(status_code=404, detail="not found")
    return result

@app.get("/finhub_to_unicorn")
def map_finhub_to_unicorn(symbol, exchange):
    country_code   = {
    'BK':'BK',
    'US':'US',
    'HK':'HK',
    'AX': 'AU',
    'L': 'LSE',
    'NS': 'NSE',
    'SI': 'SG',
    'VN': 'VN',
    'SS': 'SHG',
    'SZ': 'SHE',
    'T': 'TSE',
    'TO': 'TO'
    }
    exchange = country_code[exchange.upper()]

    if exchange == 'HK':
        symbol = str(symbol).zfill(4)
        
    if exchange == 'SZ':
        symbol = str(symbol).zfill(6)
        
    return f'{symbol}.{exchange}'
    
