from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy import Boolean, Table, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import os
from sqlalchemy import and_
from dotenv import load_dotenv

load_dotenv()


Base = declarative_base()
# metadata = MetaData()


class TickersList2B(Base):
    __tablename__ = "list_of_tickers_lt_2B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    market_cap = Column(Float, nullable=False)
    nasdaq_tickers = Column(String, nullable=False)
    nyse_tickers = Column(String, nullable=False)

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
    open = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

"""
Populating DB with tickers step by step
0. Download data for all Nasdaq and Nyse tickers from https://www.nasdaq.com/market-activity/stocks/screener
1. Create DF with tickers and Market Cap
2. Populate DB with tickers and Market Cap. Set nasdaq_tickers and nyse_tickers to False
3. Update nasdaq_tickers and nyse_tickers to True for Nasdaq and Nyse tickers
4. Delete tickers that are not Nasdaq or Nyse
"""


# 1. Create DF with tickers and Market Cap
def create_tickers_file(filename):
    df_csv = pd.read_csv(filename)
    df = df_csv.dropna(subset=["Market Cap"], inplace=False)
    df_two_bills = df[df["Market Cap"] >= 2_000_000_000]
    df_ticker_MC = df_two_bills[["Symbol", "Market Cap"]]
    tickers = list(df_two_bills["Symbol"])
    print("Step 1 done")
    return df_ticker_MC


# 2. Populate DB with tickers and Market Cap. Set nasdaq_tickers and nyse_tickers to False
def populate_db_with_tickers_MC(df):

    for _, row in df.iterrows():
        stock_price = TickersList2B(
            ticker=row["Symbol"],
            market_cap=row["Market Cap"],
            nasdaq_tickers=False,
            nyse_tickers=False,
        )
        session.add(stock_price)

    session.commit()
    print("Step 2 done")


# df_2B = create_tickers_file("nasdaq_screener_1750065150281.csv")
# populate_db_with_tickers_MC(df_2B)

# 3. Update nasdaq_tickers and nyse_tickers to True for Nasdaq and Nyse tickers
df_nasdaq = pd.read_csv("nasdaq_lt_2B.csv")
for ticker in list(df_nasdaq["Symbol"]):
    session.query(TickersList2B).filter(TickersList2B.ticker == ticker).update(
        {"nasdaq_tickers": True}
    )
# session.commit()
df_nyse = pd.read_csv("nyse_lt_2B.csv")
for ticker in list(df_nyse["Symbol"]):
    session.query(TickersList2B).filter(TickersList2B.ticker == ticker).update(
        {"nyse_tickers": True}
    )
# session.commit()
print("Step 3 done")

# 4. Delete tickers that are not Nasdaq or Nyse
results = (
    session.query(TickersList2B)
    .filter(
        and_(TickersList2B.nasdaq_tickers == False, TickersList2B.nyse_tickers == False)
    )
    .delete()
)
# session.commit()
print("Step 4 done")
