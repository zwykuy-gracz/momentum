from datetime import date, timedelta
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy import Float, Date, Boolean, union_all
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="/home/frog/momentum_tg/momentum/watchdog_daily_routine.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting weekly change populating")

Base = declarative_base()

print("Starting best/worst weekly DB populating")


class ListOfTickers(Base):
    __tablename__ = "list_of_tickers_lt_1B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Float, nullable=False)
    nasdaq_tickers = Column(String, nullable=False)
    nyse_tickers = Column(String, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}')>"


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}')>"


class Weekly20Best(Base):
    __tablename__ = "weekly_change_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


class Weekly20Worst(Base):
    __tablename__ = "weekly_change_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


#engine = create_engine(os.getenv("DB_STOCK_DATA"))
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def weekly_change(tickers):
    dates = [date.today() - timedelta(days=1), date.today() - timedelta(days=8)]
    for ticker in tickers:
        try:
            previous_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == dates[1],
                )
                .first()
            )
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == dates[0],
                )
                .first()
            )

            weekly_returns = (
                (last_friday_data.close - previous_friday_data.close)
                / previous_friday_data.close
            ) * 100

            session.query(SourceData).filter_by(ticker=ticker, date=dates[0]).update(
                {"weekly_change": weekly_returns}
            )

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)


previous_day = date.today() - timedelta(days=1)
list_of_tickers = [
    t[0]
    for t in session.query(ListOfTickers.ticker)
    .filter(ListOfTickers.market_cap > 10_000_000_000)
    .all()
]
weekly_change(list_of_tickers)

query_result = (
    session.query(SourceData.date, SourceData.ticker, SourceData.weekly_change)
    .filter(SourceData.date == previous_day)
    .all()
)

df = pd.DataFrame(query_result, columns=["Date", "Ticker", "Weekly_change"])
df["Date"] = pd.to_datetime(df["Date"]).dt.date
df = df.dropna()

df_weekly_sorted = df.sort_values(by="Weekly_change", ascending=False)

weekly_top20 = df_weekly_sorted.head(20)
weekly_worst20 = df_weekly_sorted.tail(20)

for _, row in weekly_top20.iterrows():
    stock_data = Weekly20Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

for _, row in weekly_worst20.iterrows():
    stock_data = Weekly20Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

session.commit()

session.close()

logging.info(f"Finished best/worst weekly DB populating")

import time
import runpy

print("5 seconds sleep after weekly is done")
time.sleep(5)

runpy.run_path(path_name="/home/frog/momentum_tg/momentum/tg_bot/tg_main.py")
