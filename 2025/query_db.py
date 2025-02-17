from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy import Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


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


class TickersList10B(Base):
    __tablename__ = "list_of_tickers_lt_10B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(String, nullable=False)
    nyse_tickers = Column(String, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


engine = create_engine(os.getenv("DB_STOCK_DATA"))
Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)

# Delete records
# session.query(HistoricalStockData).filter(
#    HistoricalStockData.date == date(2025, 2, 14)
# ).delete()
# session.commit()

# specific_date = date(2025, 2, 11)
specific_date = previous_day
query_result = (
    session.query(HistoricalStockData)
    .filter(HistoricalStockData.date == specific_date)
    .all()
)
query_result_10B = session.query(TickersList10B).all()
nasdaq_list_of_tickers = [
    t.ticker
    for t in session.query(TickersList10B)
    .filter(TickersList10B.nasdaq_tickers == True)
    .all()
]
print(nasdaq_list_of_tickers[:5])
# print(f"DB records for {specific_date}: {len(query_result)}")
# print(f"Number of tickers lt 10B: {len(query_result_10B)}")
