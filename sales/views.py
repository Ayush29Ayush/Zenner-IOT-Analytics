from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
import os
from .utils import ingest_data, top_five, monthly_revenue, avg_sales, annual_growth
from .tasks import run_sales_ingestion_and_analysis

class IngestView(APIView):
    def post(self, request):
        csv_path = os.path.join(settings.MEDIA_ROOT, "orders.csv")
        res = ingest_data(csv_path)
        return Response(res)

class TopProductsView(APIView):
    def get(self, request):
        return Response(top_five())

class MonthlyRevenueView(APIView):
    def get(self, request):
        return Response(monthly_revenue())

class AvgByCategoryView(APIView):
    def get(self, request):
        return Response(avg_sales())

class AnnualGrowthView(APIView):
    def get(self, request):
        return Response(annual_growth())

class RunAllAsyncView(APIView):
    def post(self, request):
        task = run_sales_ingestion_and_analysis.delay()
        return Response({"task_id": task.id}, status=status.HTTP_202_ACCEPTED)

class LogsView(APIView):
    def get(self, request):
        log_path = os.path.join(settings.MEDIA_ROOT, "logs", "sales_analysis.log")
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                return Response({"log": f.read()})
        return Response({"detail": "log not found"}, status=404)
