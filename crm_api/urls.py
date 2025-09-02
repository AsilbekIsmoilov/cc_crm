from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import *

router = DefaultRouter()
router.register(r"fixeds", FixedsViewSet, basename="fixeds")
router.register(r'actives', ActivesViewSet, basename='actives')
router.register(r'suspends', SuspendsViewSet, basename='suspends')
router.register(r'excel-uploads', ExcelUploadViewSet, basename='excel-uploads')
router.register(r'operators', OperatorsViewSet, basename='operators')

urlpatterns = [
    path("", include(router.urls)),
    path('search-all/', SearchSuspendsFixeds.as_view()),
    path("export/suspends/", export_all_suspends, name="export_all_suspends"),
    path("export/actives/", export_all_actives, name="export_all_actives"),
    path("export/suspends/phones.csv", export_suspends_phones_csv,name="export_suspends_phones_csv"),
    path("imports/upload/", ImportUploadView.as_view(), name="imports-upload"),
    path("imports/status/<int:job_id>/", ImportStatusView.as_view(), name="imports-status"),
    path("export/fixeds/", export_all_fixeds, name="export_all_fixeds"),
    path("export/fixeds/daily/", export_fixeds_daily, name="export_fixeds_daily"),
    path("export/fixeds/monthly/", export_fixeds_monthly, name="export_fixeds_monthly"),
    path("maintenance/move-suspends/", MoveSuspendsToFixedsAPIView.as_view(), name="maintenance-move-suspends"),

]
