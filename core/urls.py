from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from django.contrib import admin
from crm_api.views import *
urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("crm_api.urls")),
    path("api/auth/token/", TokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("api/auth/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("api/auth/me/", MeView.as_view(), name="auth_me"),
    path("api/auth/logout/", LogoutView.as_view(), name="auth_logout"),
    path("api/auth/token/verify/", MyTokenVerifyView.as_view(), name="my_token_verify"),
              ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
