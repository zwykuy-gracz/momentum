import logging
from datetime import date
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    Date,
    Boolean,
)
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)

    def __repr__(self):
        return f"<StockData(date='{self.date}')>"


class MarketBreadth(Base):
    __tablename__ = "market_breadth"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)

    ma50_number_of_stocks_above = Column(Integer, nullable=True)
    ma50_number_of_stocks_below = Column(Integer, nullable=True)
    ma50_pct_of_stocks_above = Column(Float, nullable=True)

    ma100_number_of_stocks_above = Column(Integer, nullable=True)
    ma100_number_of_stocks_below = Column(Integer, nullable=True)
    ma100_pct_of_stocks_above = Column(Float, nullable=True)

    ma200_number_of_stocks_above = Column(Integer, nullable=True)
    ma100_number_of_stocks_below = Column(Integer, nullable=True)
    ma200_pct_of_stocks_above = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(date='{self.date}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def get_change(above, number_of_tickers):
    if above == number_of_tickers:
        return 100.0
    try:
        return (above / number_of_tickers) * 100.0
    except ZeroDivisionError:
        return 0


specific_date = date(2025, 1, 17)
query_ma = session.query(StockData).filter(StockData.date == specific_date).all()
print("------ 50")
above = 0
below = 0
for ticker in query_ma:
    if ticker.ma50_above == True:
        above += 1
    else:
        below += 1
print(len(query_ma))
print("above 50", above)
print("below 50", below)

market_breadth = get_change(above, len(query_ma))
print(round(market_breadth, 2))
# ------ 100
print("------ 100")
above = 0
below = 0
for ticker in query_ma:
    if ticker.ma100_above == True:
        above += 1
    else:
        below += 1
print(len(query_ma))
print("above 100", above)
print("below 100", below)

market_breadth = get_change(above, len(query_ma))
print(round(market_breadth, 2))
# ------ 200
print("------ 200")
above = 0
below = 0
for ticker in query_ma:
    if ticker.ma200_above == True:
        above += 1
    else:
        below += 1
print(len(query_ma))
print("above 200", above)
print("below 200", below)

market_breadth = get_change(above, len(query_ma))
print(round(market_breadth, 2))


# TODO
"""
spr jeszcze raz obliczenia
Stworzyć tabelę w DB market breadth
zrobić z tego wykresy
dodać tę fukcję w daily_update
"""

session.close()
