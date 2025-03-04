from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("single/", views.single_ticker, name="single_ticker"),
    path("jap/", views.index_view, name="index"),
    path("chart/", views.chart_view, name="chart"),
]
