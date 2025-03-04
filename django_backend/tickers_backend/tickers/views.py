from django.http import HttpResponse
from django.shortcuts import render
from .calculations import (
    get_best_ytd,
    get_worst_ytd,
    last_working_day,
    year_ago_data,
    get_momentum_12_3,
    get_momentum_6_2,
    last_working_day_previous_month,
)


def home(request):
    ytd_best = get_best_ytd
    ytd_worst = get_worst_ytd
    momentum_12_3 = get_momentum_12_3
    momentum_6_2 = get_momentum_6_2
    context = {
        "ytd_best": ytd_best,
        "ytd_worst": ytd_worst,
        "momentum_12_3": momentum_12_3,
        "momentum_6_2": momentum_6_2,
        "last_working_day_previous_month": last_working_day_previous_month,
    }
    return render(request, "tickers/index.html", context=context)


import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from io import StringIO, BytesIO


def chart_view(request):
    plt.style.use("dark_background")
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6], label="Sample Data", color="cyan")
    ax.set_title("Dynamic Chart")
    ax.set_xlabel("X Axis")
    ax.set_ylabel("Y Axis")
    ax.legend()

    # Save the plot to an in-memory buffer
    buf = BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)

    # Return the image as an HTTP response
    return HttpResponse(buf.getvalue(), content_type="image/png")


def index_view(request):
    # Simple HTML template to display the chart
    return HttpResponse(
        """
    <h1>Dynamic Matplotlib Chart</h1>
    <img src="/chart/" alt="Dynamic Chart">
    """
    )


def single_ticker(request):
    context = {
        "last_working_day": last_working_day,
        "year_ago_data": year_ago_data,
    }
    return render(request, "tickers/single.html", context=context)
