from datetime import date, timedelta
from sqlalchemy import create_engine, Column, Integer, String, select
from sqlalchemy import Float, Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import and_
import os
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

# logging.basicConfig(
#     filename=os.getenv("LOG_FILE"),
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )

# logging.info(f"Starting weekly change populating")

Base = declarative_base()


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)


class CorrectionsWeakest(Base):
    __tablename__ = "corrections_weakest"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    last_closing_price = Column(Float, nullable=False)
    highest_price = Column(Float, nullable=False)
    pct_change = Column(Float, nullable=False)
    highest_price_date = Column(Date, nullable=False)

    def __repr__(self):
        return f"<CorrectionsWeakest(ticker='{self.ticker}', date='{self.date}')>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


with session:
    stmt = (
        select(StockData.date, StockData.high, StockData.low)
        .where(and_(StockData.ticker == "AMZN", StockData.date >= date(2024, 12, 1)))
        .order_by(StockData.date)
    )

    results = session.execute(stmt).all()
    max_date = None
    if results:
        max_price = max(result[1] for result in results)
        for i, result in enumerate(results):
            if result[1] == max_price:
                max_index_date, max_date = i, result[0]
                break

        min_price = min(result[2] for result in results[max_index_date:])
        for i, result in enumerate(results):
            if result[2] == min_price:
                min_index_date, min_date = i, result[0]
                break

        print(
            f"Max date: {max_date}, max date index: {max_index_date}, max price: {max_price}"
        )
        print(
            f"Min date: {min_date}, min date index: {min_index_date}, min price: {min_price}"
        )
    else:
        print("\nNo data found for AAPL in the last 3 months")

print("-------")
pct_change = (min_price - max_price) / max_price * 100
print(pct_change)

# TODO: create func, iterate through all tickers, add to DB, TG bot
# list_of_tickers = [t.ticker for t in session.query(TickersList5B).all()]
