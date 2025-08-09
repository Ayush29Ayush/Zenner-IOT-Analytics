from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
import os
from .utils import ingest_data, highest_uplinks, avg_rssi_snr, avg_weather, get_duplicates, export_hot_temps
from .tasks import run_uplinks_ingestion_and_analysis

class IngestView(APIView):
    def post(self, request):
        csv_path = os.path.join(settings.MEDIA_ROOT, "lorawan_uplink_devices.csv")
        res = ingest_data(csv_path)
        return Response(res)

class TopUplinksView(APIView):
    def get(self, request):
        n = int(request.GET.get("n", 10))
        data = highest_uplinks(n)
        return Response(data)

class AvgRssiSnrView(APIView):
    def get(self, request):
        data = avg_rssi_snr()
        return Response(data)

class AvgWeatherView(APIView):
    def get(self, request):
        data = avg_weather()
        return Response(data)

class DuplicatesView(APIView):
    def get(self, request):
        data = get_duplicates()
        return Response(data)

class ExportHotView(APIView):
    def post(self, request):
        out = os.path.join(settings.MEDIA_ROOT, "temp_detail.json")
        res = export_hot_temps(out)
        return Response(res)

class RunAllAsyncView(APIView):
    def post(self, request):
        task = run_uplinks_ingestion_and_analysis.delay()
        return Response({"task_id": task.id}, status=status.HTTP_202_ACCEPTED)

class LogsView(APIView):
    def get(self, request):
        log_path = os.path.join(settings.MEDIA_ROOT, "logs", "uplinks_analysis.log")
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                return Response({"log": f.read()})
        return Response({"detail": "log not found"}, status=404)
