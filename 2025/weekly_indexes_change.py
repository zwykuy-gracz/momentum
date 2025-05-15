from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy import Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import and_
import os
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting indexes weekly change populating")

Base = declarative_base()

print("Starting indexes weekly DB populating")


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}')>"


class IndexesWeeklyChange(Base):
    __tablename__ = "indexes_weekly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_week_pct_change = Column(Float, nullable=False)
    four_week_pct_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def weekly_index_change(tickers, last_friday, previous_friday):
    for ticker in tickers:
        try:
            last_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == last_friday,
                )
                .first()
            )
            one_week_before_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == previous_friday,
                )
                .first()
            )

            four_weeks_before_friday_data = (
                session.query(SourceData)
                .filter(
                    SourceData.ticker == ticker,
                    SourceData.date == previous_friday,
                )
                .first()
            )

            weekly_returns = (
                (last_friday_data.close - four_weeks_before_friday_data.close)
                / four_weeks_before_friday_data.close
            ) * 100

            session.query(SourceData).filter_by(ticker=ticker, date=last_friday).update(
                {"weekly_change": weekly_returns}
            )

            session.commit()

        except AttributeError:
            print("Bad ticker:", ticker)
            logging.error(f"Bad ticker: {ticker} in counting weekly index change")
    logging.info(f"Finished weekly index change populating")


last_friday = date.today() - timedelta(days=1)
previous_friday = date.today() - timedelta(days=8)
four_weeks_ago_friday = date.today() - timedelta(days=29)

list_of_indexes = ["QQQ", "SPY", "DIA", "IWM"]
weekly_index_change(list_of_indexes, last_friday, previous_friday)

query = session.query(
    SourceData.date, SourceData.ticker, SourceData.weekly_change
).filter(and_(SourceData.ticker.in_(list_of_indexes), SourceData.date == last_friday))

results = query.all()

df = pd.DataFrame(results, columns=["Date", "Ticker", "Weekly_change"])
df["Date"] = pd.to_datetime(df["Date"]).dt.date
df = df.dropna()

df_weekly_indexes_sorted = df.sort_values(by="Weekly_change", ascending=False)

for _, row in df_weekly_indexes_sorted.iterrows():
    stock_data = IndexesWeeklyChange(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["Weekly_change"],
    )
    session.add(stock_data)

session.commit()

session.close()

logging.info(f"Finished indexes weekly DB populating")

import time
import runpy

print("5 seconds sleep after indexes weekly is done")
time.sleep(5)
try:
    runpy.run_path(path_name=os.getenv("TG_BOT_PATH"))
except Exception as e:
    logging.error(f"Error in running tg_bot.py: {e}")
