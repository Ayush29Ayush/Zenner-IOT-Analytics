from django.contrib import admin
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("api/uplinks/", include("uplinks.urls")),
    path("api/sales/", include("sales.urls")),
    path("health/", include("health_check.urls"))
]

admin.site.site_header = "IoT Analytics Admin"
admin.site.site_title = "IoT Analytics Admin Portal"
admin.site.index_title = "Welcome to IoT Analytics Admin Portal"
