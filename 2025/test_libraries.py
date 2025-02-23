import pandas as pd
import numpy as np
import yfinance as yf
import logging
import runpy
from datetime import date, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Boolean, case
from sqlalchemy.orm import sessionmaker, declarative_base
from tradingview_ta import TA_Handler, Interval
from sqlalchemy.sql import and_
import os
import time
from dotenv import load_dotenv


print('jap')

load_dotenv()

logging.basicConfig(
    filename=os.getenv("LOG_FILE"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.info(f"TEST libraries")
try:
    engine = create_engine(os.getenv("DB_ABSOLUTE_PATH"))
    logging.info(f"Engin created")
except Exception as e:
    logging.error(f"Engin creation failed: {e}", exc_info=True)

    
# Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
 
print('after testing')


