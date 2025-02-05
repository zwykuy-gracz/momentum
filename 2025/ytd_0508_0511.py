from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import date, timedelta, datetime
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


class StockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    ytd = Column(Float, nullable=True)
    august05 = Column(Float, nullable=True)
    november05 = Column(Float, nullable=True)
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
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.ytd_best})>"


class YTD20Worst(Base):
    __tablename__ = "ytd_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.ytd_worst})>"


class August05Best(Base):
    __tablename__ = "august05_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.august05_best})>"


class August05Worst(Base):
    __tablename__ = "august05_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.august05_worst})>"


class November05Best(Base):
    __tablename__ = "november05_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_best})>"


class November05Worst(Base):
    __tablename__ = "november05_worst"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.november05_worst})>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
# engine = create_engine(os.getenv("DB_STOCK_DATA_BACKUP"))
# Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)
query_result = (
    session.query(
        StockData.date,
        StockData.ticker,
        StockData.ytd,
        StockData.august05,
        StockData.november05,
    )
    .filter(StockData.date == previous_day)
    .all()
)
df = pd.DataFrame(
    query_result, columns=["Date", "Ticker", "YTD", "August05", "November05"]
)
df["Date"] = pd.to_datetime(df["Date"]).dt.date
df = df.dropna()

df_ytd = df[["Date", "Ticker", "YTD"]]
df_august05 = df[["Date", "Ticker", "August05"]]
df_november05 = df[["Date", "Ticker", "November05"]]

df_ytd_sorted = df_ytd.sort_values(by="YTD", ascending=False)
df_august05_sorted = df_august05.sort_values(by="August05", ascending=False)
df_november05_sorted = df_november05.sort_values(by="November05", ascending=False)

ytd_top20 = df_ytd_sorted.head(20)
ytd_worst20 = df_ytd_sorted.tail(20)
ytd_worst20 = ytd_worst20.sort_values(by="YTD", ascending=True)

august05_top20 = df_august05_sorted.head(20)
august05_worst20 = df_august05_sorted.tail(20)
august05_worst20 = august05_worst20.sort_values(by="August05", ascending=True)

november05_top20 = df_november05_sorted.head(20)
november05_worst20 = df_november05_sorted.tail(20)
november05_worst20 = november05_worst20.sort_values(by="November05", ascending=True)


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

for _, row in august05_top20.iterrows():
    stock_data = August05Best(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["August05"],
    )
    session.add(stock_data)

for _, row in august05_worst20.iterrows():
    stock_data = August05Worst(
        date=row["Date"],
        ticker=row["Ticker"],
        pct_change=row["August05"],
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

session.commit()

import time
import runpy

print("YTD finished - 5 seconds sleepipng")
time.sleep(5)
today = datetime.today().strftime("%A")
if today == "Saturday":
    runpy.run_path(path_name="weekly_change.py")
else:
    runpy.run_path(path_name="../tg_bot/tg_main.py")
