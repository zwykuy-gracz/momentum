from django.shortcuts import render
from .calculations import get_best_ytd, get_worst_ytd, last_working_day


def home(request):
    ytd_best = get_best_ytd
    ytd_worst = get_worst_ytd
    context = {"ytd_best": ytd_best, "ytd_worst": ytd_worst}
    return render(request, "tickers/index.html", context=context)


def single_ticker(request):
    return render(
        request, "tickers/single.html", {"last_working_day": last_working_day}
    )
