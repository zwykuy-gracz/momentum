import pandas as pd
import numpy as np
import yfinance as yf
import logging
import runpy
from datetime import date, timedelta
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    Boolean,
    case,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from tradingview_ta import TA_Handler, Interval, get_multiple_analysis
from sqlalchemy.sql import and_
import os
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

YTD_DATE = date(2025, 1, 2)
PREVIOUS_CORRECTION_DATE = date(2024, 11, 5)
LAST_CORRECTION_DATE = date(2025, 4, 7)

"""
DAILY BOT's Check LIST
Creates list of tickers create_lst_of_tickers(filename)
Downloads data for last day from YF download_tickers_from_yf(tickers, today) and write it to csv....
Creating session
Read DF from CSV and populate DB read_df_from_csv_and_populate_db(date)
Count and populate DB with YTD returns counting_and_populating_ytd_return(tickers, date)
Create Top20 YTD creating_df_best_ytd(last_date)
"""


Base = declarative_base()


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
    ytd = Column(Integer, nullable=True)
    previous_correction = Column(Float, nullable=True)
    last_correction = Column(Float, nullable=True)
    ma50 = Column(Float, nullable=True)
    ma50_above = Column(Boolean, nullable=True)
    ma100 = Column(Float, nullable=True)
    ma100_above = Column(Boolean, nullable=True)
    ma200 = Column(Float, nullable=True)
    ma200_above = Column(Boolean, nullable=True)
    weekly_change = Column(Float, nullable=False)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.close})>"


class TickersList2B(Base):
    __tablename__ = "list_of_tickers_lt_2B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


class TickersList5B(Base):
    __tablename__ = "list_of_tickers_lt_5B"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False, index=True)
    nasdaq_tickers = Column(Boolean, nullable=False)
    nyse_tickers = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"<StockPrice(ticker='{self.ticker}')>"


def creating_list_of_tickers_2B():
    list_of_tickers = [t.ticker for t in session.query(TickersList2B).all()]
    list_of_indexes = [
        "QQQ",
        "SPY",
        "DIA",
        "IWM",
        "DAX",
        "EWQ",
        "EWU",
        "EWC",
        "EWZ",
        "ARGT",
        "EWW",
        "EWA",
        "MCHI",
        "KWEB",
        "EWJ",
        "EPI",
        "EWY",
        "EWT",
        "EWH",
        "EWS",
    ]
    list_of_commodities = ["GLD", "SLV", "COPX", "USO"]
    list_of_tickers.extend(list_of_indexes)
    list_of_tickers.extend(list_of_commodities)
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


def creating_list_of_tickers_5B():
    list_of_tickers = [t.ticker for t in session.query(TickersList5B).all()]
    logging.info(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    print(f"Created list of tickers from DB with length: {len(list_of_tickers)}")
    return list_of_tickers


# downloads from YF and write DFs to files
def download_tickers_from_yf(tickers, last_date):
    try:
        fifth_length_of_tickers = len(tickers) // 5
        df = yf.download(
            tickers[:fifth_length_of_tickers],
            group_by="Ticker",
            start=last_date,
            end=date.today(),
        )
        df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
        df = df.reset_index()
        df = df.dropna(axis=1, how="all")
        df.to_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
            index=False,
        )

        print("-------------------------------------")
        print("One minute sleep during downloading from YF")
        time.sleep(30)
        print("30 more seconds")
        time.sleep(30)
        print("-------------------------------------")

        for i in range(1, 5):
            beginning = fifth_length_of_tickers * i
            end = fifth_length_of_tickers * (i + 1)
            df = yf.download(
                tickers[beginning:end],
                group_by="Ticker",
                start=last_date,
                end=date.today(),
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
                mode="a",
                index=False,
                header=False,
            )

            print("-------------------------------------")
            print("One minute sleep during downloading from YF")
            time.sleep(30)
            print("30 more seconds")
            time.sleep(30)
            print("-------------------------------------")

        if len(tickers) > fifth_length_of_tickers * 5:
            print("last part")
            df = yf.download(
                tickers[fifth_length_of_tickers * 5 :],
                group_by="Ticker",
                start=last_date,
                end=date.today(),
            )
            df = df.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)
            df = df.reset_index()
            df = df.dropna(axis=1, how="all")
            df.to_csv(
                f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
                mode="a",
                index=False,
                header=False,
            )

        print("YF tickers downloaded")
        logging.info("YF API connection successful. Data downloaded.")
    except Exception as e:
        logging.error(f"YF API connection failed: {e}", exc_info=True)


def read_df_from_csv_and_populate_db(last_date):
    try:
        df = pd.read_csv(
            f"{os.getenv('CSV_FOLDER_PATH')}/{str(last_date).replace('-', '')}.csv",
            engine="python",
        )
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        logging.info(f"DF len: {len(df)}")

        for _, row in df.iterrows():
            stock_price = StockData(
                date=row["Date"],
                close=row["Close"],
                high=row["High"],
                low=row["Low"],
                open=row["Open"],
                volume=row["Volume"],
                ticker=row["Ticker"],
            )
            session.add(stock_price)

        session.commit()
        print("DB Populated")
        logging.info("Database successfully populated.")
    except Exception as e:
        logging.error(f"Database population failed: {e}", exc_info=True)


def daily_count_new_records(last_date):
    query_result = session.query(StockData).filter(StockData.date == last_date).all()
    logging.info(f"Number of new records in DB as of {last_date}: {len(query_result)}")
    return len(query_result)


def counting_and_populating_ytd_corrections_return(tickers, last_date):
    logging.info("YTD, corrections calculations started.")
    for ticker in tickers:
        last_day_closing_price = (
            session.query(StockData)
            .filter(
                StockData.ticker == ticker,
                StockData.date == last_date,
            )
            .first()
        )
        if last_day_closing_price:
            try:
                year_opening_price = (
                    session.query(StockData)
                    .filter(
                        StockData.ticker == ticker,
                        StockData.date == YTD_DATE,
                    )
                    .first()
                )

                ytd_return = (
                    (last_day_closing_price.close - year_opening_price.open)
                    / year_opening_price.open
                ) * 100
                session.query(StockData).filter_by(
                    ticker=ticker, date=last_date
                ).update({"ytd": ytd_return})

                session.commit()
            except AttributeError as e:
                logging.error(
                    f"Error with {ticker} in YTD calculations: {e}", exc_info=True
                )

            try:
                previous_correction_opening_price = (
                    session.query(StockData)
                    .filter(
                        StockData.ticker == ticker,
                        StockData.date == PREVIOUS_CORRECTION_DATE,
                    )
                    .first()
                )

                previous_correction_return = (
                    (
                        last_day_closing_price.close
                        - previous_correction_opening_price.open
                    )
                    / previous_correction_opening_price.open
                ) * 100
                session.query(StockData).filter_by(
                    ticker=ticker, date=last_date
                ).update({"previous_correction": previous_correction_return})

                session.commit()
            except AttributeError as e:
                logging.error(
                    f"Error with {ticker} in Previous correction calculations: {e}",
                    exc_info=True,
                )

            try:
                last_correction_opening_price = (
                    session.query(StockData)
                    .filter(
                        StockData.ticker == ticker,
                        StockData.date == LAST_CORRECTION_DATE,
                    )
                    .first()
                )

                last_correction_return = (
                    (last_day_closing_price.close - last_correction_opening_price.open)
                    / last_correction_opening_price.open
                ) * 100
                session.query(StockData).filter_by(
                    ticker=ticker, date=last_date
                ).update({"last_correction": last_correction_return})

                session.commit()
            except AttributeError as e:
                logging.error(
                    f"Error with {ticker} in Last correction calculations: {e}",
                    exc_info=True,
                )
        else:
            logging.error(
                f"Couldn't find last price {ticker}",
                exc_info=True,
            )

    logging.info("YTD, corrections calculations completed successfully.")
    print("ytd_corrections_return counted")


def nasdaq_counting_and_populating_DB_with_SMAs(last_date):
    logging.info("Nasdaq SMAa calculations started.")
    nasdaq_ta_symbols = []
    nasdaq_string_ticker = "NASDAQ:"
    nasdaq_list_of_tickers = [
        t.ticker
        for t in session.query(TickersList5B)
        .filter(TickersList5B.nasdaq_tickers == True)
        .all()
    ]

    for ticker in nasdaq_list_of_tickers:
        nasdaq_ta_symbols.append(nasdaq_string_ticker + ticker)

    nasdaq_analysis = get_multiple_analysis(
        screener="america", interval=Interval.INTERVAL_1_DAY, symbols=nasdaq_ta_symbols
    )

    for ticker, indicator in nasdaq_analysis.items():
        try:
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma50": indicator.indicators["SMA50"]})
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma100": indicator.indicators["SMA100"]})
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma200": indicator.indicators["SMA200"]})
            session.commit()
        except AttributeError as e:
            logging.error(f"Error with {ticker} in SMAs: {e}", exc_info=True)

    logging.info("Nasdaq SMAa populated successfully.")
    print("Nasdaq SMAa populated")


def nyse_counting_and_populating_DB_with_SMAs(last_date):
    logging.info("Nyse SMAa calculations started.")
    nyse_ta_symbols = []
    nyse_string_ticker = "NYSE:"
    nyse_list_of_tickers = [
        t.ticker
        for t in session.query(TickersList5B)
        .filter(TickersList5B.nyse_tickers == True)
        .all()
    ]

    for ticker in nyse_list_of_tickers:
        nyse_ta_symbols.append(nyse_string_ticker + ticker)

    nyse_analysis = get_multiple_analysis(
        screener="america", interval=Interval.INTERVAL_1_DAY, symbols=nyse_ta_symbols
    )

    for ticker, indicator in nyse_analysis.items():
        try:
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma50": indicator.indicators["SMA50"]})
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma100": indicator.indicators["SMA100"]})
            session.query(StockData).filter_by(
                ticker=ticker.split(":")[1], date=last_date
            ).update({"ma200": indicator.indicators["SMA200"]})
            session.commit()
        except AttributeError as e:
            logging.error(f"Error with {ticker} in SMAs: {e}", exc_info=True)

    logging.info("Nyse SMAa populated successfully.")
    print("Nyse SMAa populated")


def check_above_below_sma(tickers, last_date):
    logging.info("Above/below SMAs counting started.")
    for ticker in tickers:
        try:
            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma50_above": case(
                        (
                            and_(
                                StockData.ma50.isnot(None),
                                StockData.close > StockData.ma50,
                            ),
                            True,
                        ),
                        (StockData.ma50.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma100_above": case(
                        (
                            and_(
                                StockData.ma100.isnot(None),
                                StockData.close > StockData.ma100,
                            ),
                            True,
                        ),
                        (StockData.ma100.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.query(StockData).filter_by(ticker=ticker, date=last_date).update(
                {
                    "ma200_above": case(
                        (
                            and_(
                                StockData.ma200.isnot(None),
                                StockData.close > StockData.ma200,
                            ),
                            True,
                        ),
                        (StockData.ma200.is_(None), False),
                        else_=False,
                    )
                },
                synchronize_session=False,
            )

            session.commit()

        except Exception as e:
            logging.error(
                f"Error in counting above/below SMAs/Bad ticker {e}", exc_info=True
            )
    logging.info("Above/below SMAs counted.")
    print("Above/below SMAs counted")


def main():
    try:
        previous_day = date.today() - timedelta(days=1)
        print(f"Working on date: {previous_day}")
        logging.info(f"Working on date: {previous_day}")
        list_of_tickers_2B = creating_list_of_tickers_2B()
        list_of_tickers_5B = creating_list_of_tickers_5B()

        download_tickers_from_yf(list_of_tickers_2B, previous_day)
        read_df_from_csv_and_populate_db(previous_day)
        number_of_new_records_in_DB = daily_count_new_records(previous_day)
        if number_of_new_records_in_DB > 0:
            counting_and_populating_ytd_corrections_return(
                list_of_tickers_5B, previous_day
            )

            nasdaq_counting_and_populating_DB_with_SMAs(previous_day)
            nyse_counting_and_populating_DB_with_SMAs(previous_day)

            check_above_below_sma(list_of_tickers_5B, previous_day)

            logging.info("All steps completed successfully.")

            logging.info("5 seconds sleep before counting market breadth")
            time.sleep(5)
            try:
                runpy.run_path(path_name=os.getenv("MARKET_BREADTH_PATH"))
            except Exception as e:
                logging.error(
                    f"Error in reaching MARKET_BREADTH_PATH script: {e}", exc_info=True
                )

    except Exception as e:
        logging.critical(f"Critical error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
    # Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    main()

    session.close()
