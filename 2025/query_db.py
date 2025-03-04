from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy import Date, Boolean, Table, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()
metadata = MetaData()


class HistoricalStockData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    ytd = Column(Integer, nullable=True)
    august05 = Column(Float, nullable=True)
    november05 = Column(Float, nullable=True)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class MonthlyChange(Base):
    __tablename__ = "monthly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_month_change = Column(Float, nullable=True)
    three_months_change = Column(Float, nullable=True)
    six_months_change = Column(Float, nullable=True)
    twelve_months_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<MonthlyChange(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


# engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
engine = create_engine(os.getenv("DB_STOCK_DATA"))
Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)

# Delete records
# session.query(MonthlyChange).filter(MonthlyChange.date == date(2025, 2, 28)).delete()
# session.commit()

# specific_date = previous_day
specific_date = date(2025, 2, 28)
query_result = (
    session.query(HistoricalStockData)
    .filter(HistoricalStockData.date == specific_date)
    .all()
)
# print(f"DB records for {specific_date}: {len(query_result)}")
query_result_10B = session.query(TickersList5B).all()
nasdaq_list_of_tickers = [
    t.ticker
    for t in session.query(TickersList5B)
    .filter(TickersList5B.nasdaq_tickers == True)
    .all()
]
# print(nasdaq_list_of_tickers[:5])
# print(f"DB records for {specific_date}: {len(query_result)}")
# print(f"Number of tickers lt 10B: {len(query_result_10B)}")

# DROP Table
# table_to_drop = Table("market_breadth", metadata, autoload_with=engine)
# table_to_drop.drop(engine)

# Delete records
# session.query(HistoricalStockData).filter(
#     HistoricalStockData.date == date(2025, 2, 3)
# ).delete()
# session.commit()

import pandas as pd


def create_df_lt_1B(filename):
    df_csv = pd.read_csv(filename)
    df = df_csv.dropna(subset=["Market Cap"], inplace=False)
    df_one_bill = df[df["Market Cap"] >= 5_000_000_000]
    df_ticker_MC = df_one_bill[["Symbol", "Market Cap"]]
    return df_ticker_MC


# df_1B_ticker_MC = create_df_lt_1B("nasdaq_screener_1740347841032.csv")
# print(len(df_1B_ticker_MC))


import calendar


def last_business_day_in_month(year: int, month: int) -> int:
    return max(calendar.monthcalendar(year, month)[-1][:5])


print(1, last_business_day_in_month(2025, 1))
