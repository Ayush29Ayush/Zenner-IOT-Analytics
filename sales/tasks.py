from celery import shared_task
from django.conf import settings
import os
from .utils import ingest_data, top_five, monthly_revenue, avg_sales, annual_growth

@shared_task
def run_sales_ingestion_and_analysis():
    csv_path = os.path.join(settings.MEDIA_ROOT, "orders.csv")
    results = {}
    results["ingest"] = ingest_data(csv_path)
    results["top5"] = top_five()
    results["monthly_revenue"] = monthly_revenue()[:10]
    results["avg_sales"] = avg_sales()
    results["annual_growth"] = annual_growth()
    return results
