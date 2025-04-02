from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import date, timedelta, datetime
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting Best/worst YTD, 0511, 3103 DB populating")

Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    ytd = Column(Float, nullable=True)
    november05 = Column(Float, nullable=True)
    march31 = Column(Float, nullable=True)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class YTD20Best(Base):
    __tablename__ = "ytd_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class YTD20Worst(Base):
    __tablename__ = "ytd_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class November05Best(Base):
    __tablename__ = "november05_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class November05Worst(Base):
    __tablename__ = "november05_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class March31Best(Base):
    __tablename__ = "march31_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


class March31Worst(Base):
    __tablename__ = "march31_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}')>"


engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)
query_result = (
    session.query(
        StockData.date,
        StockData.ticker,
        StockData.ytd,
        StockData.november05,
        StockData.march31,
    )
    .filter(StockData.date == previous_day)
    .all()
)
df = pd.DataFrame(
    query_result, columns=["Date", "Ticker", "YTD", "November05", "March31"]
)
df["Date"] = pd.to_datetime(df["Date"]).dt.date
df = df.dropna()

df_ytd = df[["Date", "Ticker", "YTD"]]
df_november05 = df[["Date", "Ticker", "November05"]]
df_march31 = df[["Date", "Ticker", "March31"]]

df_ytd_sorted = df_ytd.sort_values(by="YTD", ascending=False)
df_november05_sorted = df_november05.sort_values(by="November05", ascending=False)
df_march31_sorted = df_march31.sort_values(by="March31", ascending=False)

ytd_top20 = df_ytd_sorted.head(20)
ytd_worst20 = df_ytd_sorted.tail(20)
ytd_worst20 = ytd_worst20.sort_values(by="YTD", ascending=True)

november05_top20 = df_november05_sorted.head(20)
november05_worst20 = df_november05_sorted.tail(20)
november05_worst20 = november05_worst20.sort_values(by="November05", ascending=True)

march31_top20 = df_march31_sorted.head(20)
march31_worst20 = df_march31_sorted.tail(20)
march31_worst20 = march31_worst20.sort_values(by="March31", ascending=True)


for _, row in ytd_top20.iterrows():
    stock_data = YTD20Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["YTD"],
    )
    session.add(stock_data)

for _, row in ytd_worst20.iterrows():
    stock_data = YTD20Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["YTD"],
    )
    session.add(stock_data)

for _, row in november05_top20.iterrows():
    stock_data = November05Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["November05"],
    )
    session.add(stock_data)

for _, row in november05_worst20.iterrows():
    stock_data = November05Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["November05"],
    )
    session.add(stock_data)

for _, row in march31_top20.iterrows():
    stock_data = March31Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["March31"],
    )
    session.add(stock_data)

for _, row in march31_worst20.iterrows():
    stock_data = March31Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["March31"],
    )
    session.add(stock_data)

session.commit()
session.close()

logging.info(f"Finished Best/worst YTD, 0511, 3103 DB populating")

import time
import runpy

print("YTD finished - 5 seconds sleepipng")
time.sleep(5)
today = datetime.today().strftime("%A")
try:
    if today == "Saturday":
        runpy.run_path(path_name=os.getenv("WEEKLY_CHANGE_PATH"))
    else:
        runpy.run_path(path_name=os.getenv("TG_BOT_PATH"))
except Exception as e:
    logging.error(f"Error running weekly_change.py or tg_main.py: {e}")
