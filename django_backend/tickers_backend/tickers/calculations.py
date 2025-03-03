from .models import YtdBest, YtdWorst, StockData
from datetime import date, datetime, timedelta


def get_best_ytd():
    stocks = YtdBest.objects.filter(date=date(2025, 2, 28)).order_by("-pct_change")[:10]
    ytd_best = []
    for stock in stocks:
        ytd_best.append(
            {"date": stock.date, "ticker": stock.ticker, "pct_change": stock.pct_change}
        )

    return ytd_best


def get_worst_ytd():
    stocks = YtdWorst.objects.filter(date=date(2025, 2, 28)).order_by("pct_change")[:10]
    ytd_worst = []
    for stock in stocks:
        ytd_worst.append(
            {"date": stock.date, "ticker": stock.ticker, "pct_change": stock.pct_change}
        )

    return ytd_worst


working_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
weekend_days = ["Saturday", "Sunday"]
holidays = [
    "2025-01-01",
    "2025-01-20",
    "2025-02-17",
    "2025-04-18",
    "2025-05-26",
    "2025-07-04",
    "2025-09-01",
    "2025-11-27",
    "2025-12-25",
]
from datetime import datetime, timedelta


def last_working_day():
    previous_day = (datetime.today() - timedelta(days=1)).strftime("%A")
    i = 1
    while True:
        if previous_day in working_days:
            print(1, previous_day)
            date = datetime.today() - timedelta(days=i)
            ytd_stock = StockData.objects.filter(date=date, ticker="NVDA").first()
            break
        else:
            i += 1
            print(2, previous_day)
            previous_day = (datetime.today() - timedelta(days=i)).strftime("%A")

    year_ago = date - timedelta(days=365)
    i = 1
    while True:
        if year_ago in working_days:
            print(3, year_ago)
            date = datetime.today() - timedelta(days=i)
            ytd_stock = StockData.objects.filter(date=date, ticker="NVDA").first()
            break
        else:
            i += 1
            print(2, previous_day)
            previous_day = (datetime.today() - timedelta(days=i)).strftime("%A")

    print(year_ago)
    return year_ago
