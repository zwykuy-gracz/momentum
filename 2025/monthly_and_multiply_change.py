from datetime import date, timedelta
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
)
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
from datetime import datetime, timedelta

Base = declarative_base()


class SourceData(Base):
    __tablename__ = "stock_data"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    close = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class StockData(Base):
    __tablename__ = "monthly_change"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    one_month_change = Column(Float, nullable=True)
    three_months_change = Column(Float, nullable=True)
    six_months_change = Column(Float, nullable=True)
    twelve_months_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.weekly_change})>"


# engine = create_engine(os.getenv("DB_STOCK_DATA"))
# engine = create_engine(os.getenv("DB_STOCK_DATA_BACKUP"))
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# ----------------------------


def searching_for_last_working_day(no_months):
    format_string = "%Y-%m-%d %H:%M:%S"
    # today = datetime.today()  # tutaj podać 0101
    today = datetime(2025, 1, 1)
    X_months_ago = today - pd.DateOffset(months=no_months)

    last_day_of_X_months_ago = X_months_ago.date().replace(day=1) - timedelta(days=1)

    week_no = last_day_of_X_months_ago.isoweekday()
    if week_no == 7:
        last_working_day = last_day_of_X_months_ago - pd.DateOffset(days=2)
    elif week_no == 6:
        last_working_day = last_day_of_X_months_ago - pd.DateOffset(days=1)
        # timestamp_string = last_day_of_X_months_ago - pd.DateOffset(days=1)
        # last_working_day = datetime.strptime(timestamp_string, format_string)
        # print("aj", type(last_working_day))
    else:
        last_working_day = last_day_of_X_months_ago
        # print(type(last_working_day))

    # print("Last working day of prev month:", last_working_day)
    return last_working_day


def defining_last_working_days():

    last_working_day_one_month_ago = searching_for_last_working_day(1)
    last_working_day_three_months_ago = searching_for_last_working_day(3)
    last_working_day_six_months_ago = searching_for_last_working_day(6)
    last_working_day_twelve_months_ago = searching_for_last_working_day(11)
    return [
        last_working_day_one_month_ago,
        last_working_day_three_months_ago,
        last_working_day_six_months_ago,
        last_working_day_twelve_months_ago,
    ]


# today = datetime.today()  # spr czy dzien > 1. np 30
# last_traiding_day_of_previous_month = today.date().replace(day=1) - timedelta(
#     days=1
# )
# TODO check for november returns
def monthly_change(tickers, list_of_last_working_days):
    calculated_rows = []
    last_date = date(2025, 1, 17)
    for i, day in enumerate(list_of_last_working_days):
        print(day)
        for j, ticker in enumerate(tickers):
            try:
                if j % 100 == 0:
                    print(i, j)
                last_day_data = (
                    session.query(SourceData)
                    .filter(
                        SourceData.ticker == ticker,
                        SourceData.date == last_date,
                    )
                    .first()
                )
                # print(day, last_day_data.ticker, last_day_data.close)
                x_monts_ago_data = (
                    session.query(SourceData)
                    .filter(
                        SourceData.ticker == ticker,
                        SourceData.date == day,
                    )
                    .first()
                )

                x_months_returns = (
                    (last_day_data.close - x_monts_ago_data.close)
                    / x_monts_ago_data.close
                ) * 100
                if i == 0:
                    calculated_rows.append(
                        StockData(
                            ticker=ticker,
                            date=last_date,
                            one_month_change=x_months_returns,
                            three_months_change=None,
                            six_months_change=None,
                            twelve_months_change=None,
                        )
                    )
                elif i == 1:
                    three_months_return = x_months_returns
                    session.query(StockData).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"three_months_change": three_months_return})
                    session.commit()
                elif i == 2:
                    six_months_return = x_months_returns
                    session.query(StockData).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"six_months_change": six_months_return})
                    session.commit()
                elif i == 3:
                    twelve_months_return = x_months_returns
                    session.query(StockData).filter_by(
                        ticker=ticker, date=last_date
                    ).update({"twelve_months_change": twelve_months_return})
                    session.commit()
                else:
                    print("something went wrong...")

            except AttributeError:
                print("Bad ticker:", ticker)
        if i == 0:
            session.bulk_save_objects(calculated_rows)
            session.commit()


# list_of_tickers =
# list_of_last_working_days = defining_last_working_days()
# monthly_change(list_of_tickers, list_of_last_working_days)

# today = datetime.today()
# six_months_ago = today - pd.DateOffset(months=1)

# last_day_of_prev_month = six_months_ago.date().replace(day=1) - timedelta(days=1)

# print("Last day of prev month:", last_day_of_prev_month)
# week_no = last_day_of_prev_month.isoweekday()
# if week_no == 7:
#     last_working_day = last_day_of_prev_month - pd.DateOffset(days=2)
# elif week_no == 6:
#     last_working_day = last_day_of_prev_month - pd.DateOffset(days=1)
# print(f"last working day {last_working_day.date()}")
