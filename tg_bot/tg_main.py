#!/usr/bin/python3
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import logging
import time
from datetime import date, timedelta, datetime
import pandas as pd
import logging

from telegram import Update
from telegram.ext import ContextTypes, Application, CommandHandler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="/home/frog/momentum_tg/momentum/watchdog_daily_routine.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"Starting telegram bot")

print("TG bot started")

#logging.basicConfig(
#    filename="momentum.log",
#    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#    level=logging.INFO,
#)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

Base = declarative_base()


class YTD20Best(Base):
    __tablename__ = "ytd_best"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    ticker = Column(String, nullable=False, index=True)
    pct_change = Column(Float, nullable=True)

    def __repr__(self):
        return f"<StockData(ticker='{self.ticker}', date='{self.date}', close={self.pct_change})>"


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


#engine = create_engine(os.getenv("DB_STOCK_DATA_TG"))
engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
#engine = create_engine("sqlite:///momentum_tg/momentum/2025/historical_stock_data.db")
Session = sessionmaker(bind=engine)
session = Session()

previous_day = date.today() - timedelta(days=1)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("User %s started the conversation.", update)
    await update.message.reply_text("info")


async def start_msg(context: ContextTypes.DEFAULT_TYPE):
    msg = "/start@cjt_ticker_bot"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=msg,
    )
    # context.application.stop_running()


async def ytd_top20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                YTD20Best.date,
                YTD20Best.ticker,
                YTD20Best.pct_change,
            )
            .filter(YTD20Best.date == previous_day)
            .all()
        )
        ytd_best_msg = f"Best performing stocks YTD as of {previous_day}\n\n"
        for q in query_result:
            ytd_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=ytd_best_msg,
        )
        logging.info("ytd_top20 successly sent")
    except Exception as e:
        logger.error("ytd_top20 Error: %s", e)


async def ytd_bottom20(context: ContextTypes.DEFAULT_TYPE):
    try:
        query_result = (
            session.query(
                YTD20Worst.date,
                YTD20Worst.ticker,
                YTD20Worst.pct_change,
            )
            .filter(YTD20Worst.date == previous_day)
            .all()
        )
        ytd_worst_msg = f"Worst performing stocks YTD as of {previous_day}\n\n"
        for q in query_result:
            ytd_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
        await context.bot.send_message(
            chat_id=os.getenv("CJT_GROUP_ID"),
            message_thread_id=os.getenv("TICKER_BOT_ROOM"),
            text=ytd_worst_msg,
        )
        logging.info("ytd_bottom20 successly sent")
    except Exception as e:
        logger.error("ytd_bottom20 Error: %s", e)
    #context.application.stop_running()



async def august05_top20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            August05Best.date,
            August05Best.ticker,
            August05Best.pct_change,
        )
        .filter(August05Best.date == previous_day)
        .all()
    )
    august05_best_msg = (
        f"Best performing stocks since August 5th as of {previous_day}\n\n"
    )
    for q in query_result:
        august05_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=august05_best_msg,
    )


async def august05_bottom20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            August05Worst.date,
            August05Worst.ticker,
            August05Worst.pct_change,
        )
        .filter(August05Worst.date == previous_day)
        .all()
    )
    august05_worst_msg = (
        f"Wrost performing stocks since August 5th as of {previous_day}\n\n"
    )
    for q in query_result:
        august05_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=august05_worst_msg,
    )


async def november05_top20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            November05Best.date,
            November05Best.ticker,
            November05Best.pct_change,
        )
        .filter(November05Best.date == previous_day)
        .all()
    )
    november05_best_msg = (
        f"Best performing stocks since November 5th as of {previous_day}\n\n"
    )
    for q in query_result:
        november05_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=november05_best_msg,
    )


async def november05_bottom20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            November05Worst.date,
            November05Worst.ticker,
            November05Worst.pct_change,
        )
        .filter(November05Worst.date == previous_day)
        .all()
    )
    november05_worst_msg = (
        f"Worst performing stocks since November 5th as of {previous_day}\n\n"
    )
    for q in query_result:
        november05_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=november05_worst_msg,
    )
    context.application.stop_running()


async def weekly_top20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            Weekly20Best.date,
            Weekly20Best.ticker,
            Weekly20Best.pct_change,
        )
        .filter(Weekly20Best.date == previous_day)
        .all()
    )
    weekly_best_msg = "This week best performing stocks:\n\n"
    for q in query_result:
        weekly_best_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=weekly_best_msg,
    )


async def weekly_bottom20(context: ContextTypes.DEFAULT_TYPE):
    query_result = (
        session.query(
            Weekly20Worst.date,
            Weekly20Worst.ticker,
            Weekly20Worst.pct_change,
        )
        .filter(Weekly20Worst.date == previous_day)
        .all()
    )
    weekly_worst_msg = "This week worst performing stocks\n\n"
    for q in query_result:
        weekly_worst_msg += f"{q.ticker}: {round(q.pct_change,2)}%\n"
    await context.bot.send_message(
        chat_id=os.getenv("CJT_GROUP_ID"),
        message_thread_id=os.getenv("TICKER_BOT_ROOM"),
        text=weekly_worst_msg,
    )
    # context.application.stop_running()


application = Application.builder().token(os.getenv("TG_TOKEN")).build()

application.add_handler(CommandHandler("info", start))
# job_queue.run_once(start_msg, 2)
logging.info("starting job queue")
job_queue = application.job_queue
today = datetime.today().strftime("%A")
if today == "Saturday":
    job_queue.run_once(weekly_top20, 2)
    job_queue.run_once(weekly_bottom20, 4)
job_queue.run_once(ytd_top20, 7)
job_queue.run_once(ytd_bottom20, 10)
job_queue.run_once(august05_top20, 13)
job_queue.run_once(august05_bottom20, 17)
job_queue.run_once(november05_top20, 20)
job_queue.run_once(november05_bottom20, 23)
logging.info("job queue ended")

application.run_polling(allowed_updates=Update.ALL_TYPES)

logging.info(f"Finished TG bot")

