from celery import shared_task
from django.conf import settings
from .utils import (
    ingest_data, highest_uplinks, avg_rssi_snr, avg_weather, get_duplicates, export_hot_temps
)
import os

@shared_task
def run_uplinks_ingestion_and_analysis():
    csv_path = os.path.join(settings.MEDIA_ROOT, "lorawan_uplink_devices.csv")
    out_json = os.path.join(settings.MEDIA_ROOT, "temp_detail.json")
    results = {}
    results["ingest"] = ingest_data(csv_path)
    results["top10"] = highest_uplinks(10)
    results["avg_rssi_snr_top10"] = avg_rssi_snr()[:10]
    results["avg_weather_top10"] = avg_weather()[:10]
    results["duplicates_top10"] = get_duplicates()[:10]
    results["export"] = export_hot_temps(out_json)
    return results
