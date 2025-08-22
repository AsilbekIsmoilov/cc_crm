from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import *
from .auth import (
    MyTokenObtainPairView,
    MyTokenRefreshView,
    MyTokenVerifyView,
    LogoutView,
    MeView,
)

router = DefaultRouter()
router.register(r'actives', ActivesViewSet, basename='actives')
router.register(r'suspends', SuspendsViewSet, basename='suspends')
router.register(r'fixations', FixationsViewSet, basename='fixations')
router.register(r'sbms-accounts', SBMSAccountViewSet, basename='sbms-accounts')
router.register(r'google-accounts', GoogleAccountViewSet, basename='google-accounts')
router.register(r'excel-uploads', ExcelUploadViewSet, basename='excel-uploads')
router.register(r'operators', OperatorsViewSet, basename='operators')

urlpatterns = [
    path("", include(router.urls)),
    path("export/all_suspends/", export_all_suspends, name="export_all_suspends"),
    path("export/all_actives/", export_all_actives, name="export_all_actives"),
    path("run-export-excel/", run_export_excel, name="run_export_excel"),
    path("run-recheck-suspends/", run_recheck_suspends, name="run_recheck_suspends"),
    path("export/suspends/phones.csv", export_suspends_phones_csv,name="export_suspends_phones_csv"),
]
