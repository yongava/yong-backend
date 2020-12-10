from sqlalchemy import Table
from .database import Base, engine

class vStockFundamentalByQuote2(Base):
    table = Table('vStockFundamentalByQuote2', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table
    __mapper_args__ = {
        'primary_key':[table.c.SecurityNumber, table.c.Fiscal, table.c.Quarter]
    }

class vFinancial(Base):
    table = Table('vFinancial', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table
    __mapper_args__ = {
        'primary_key':[table.c.SecurityNumber, table.c.Fiscal, table.c.Quarter, table.c.AccountID, table.c.AccountFrom, table.c.ImportDate]
    }

class vStockAndIndex(Base):
    table = Table('vStockAndIndex', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table
    __mapper_args__ = {
        'primary_key':[table.c.ID]
    }

class WatchOpenCloseSummary(Base):
    table = Table('WatchOpenCloseSummary', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table


class IndustryNo(Base):
    table = Table('IndustryNo', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table

class SectorNo(Base):
    table = Table('SectorNo', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table

class d_Compsec(Base):
    table = Table('d_Compsec', Base.metadata,
                    autoload=True, autoload_with=engine)
    __table__ = table