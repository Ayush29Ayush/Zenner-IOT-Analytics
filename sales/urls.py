from django.urls import path
from .views import (
    IngestView, TopProductsView, MonthlyRevenueView, AvgByCategoryView,
    AnnualGrowthView, LogsView, RunAllAsyncView
)

urlpatterns = [
    path("ingest/", IngestView.as_view()),
    path("top-products/", TopProductsView.as_view()),
    path("monthly-revenue/", MonthlyRevenueView.as_view()),
    path("avg-by-category/", AvgByCategoryView.as_view()),
    path("annual-growth/", AnnualGrowthView.as_view()),
    path("run-all-async/", RunAllAsyncView.as_view()),
    path("logs/", LogsView.as_view()),
]
