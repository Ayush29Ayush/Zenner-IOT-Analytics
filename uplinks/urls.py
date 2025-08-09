from django.urls import path
from .views import (
    IngestView, TopUplinksView, AvgRssiSnrView, 
    AvgWeatherView, DuplicatesView, ExportHotView, LogsView, RunAllAsyncView
)

urlpatterns = [
    path("ingest/", IngestView.as_view()),
    path("top/", TopUplinksView.as_view()),
    path("avg-rssi-snr/", AvgRssiSnrView.as_view()),
    path("avg-weather/", AvgWeatherView.as_view()),
    path("duplicates/", DuplicatesView.as_view()),
    path("export-hot/", ExportHotView.as_view()),
    path("run-all-async/", RunAllAsyncView.as_view()),
    path("logs/", LogsView.as_view()),
]
