import io
import threading

from django.contrib.admin.views.decorators import staff_member_required
from django.conf import settings
from django.utils import timezone
from django.db.models import Q
import sys
from rest_framework import viewsets, permissions, status
from rest_framework.decorators import action
from rest_framework.exceptions import PermissionDenied
from rest_framework.pagination import PageNumberPagination
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.permissions import DjangoModelPermissions, IsAuthenticated, AllowAny, IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView
import csv
from rest_framework_simplejwt.tokens import RefreshToken, UntypedToken
from rest_framework_simplejwt.views import TokenVerifyView
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError
from rest_framework_simplejwt.backends import TokenBackend
from .services import *
from .models import *
from .serializers import *
import openpyxl
from django.http import HttpResponse
from django.utils import timezone

from .services.excel_importer import run_import

ORDERABLE = {
    "id", "created_at", "updated_at", "msisdn", "client", "rate_plan",
    "branches", "status", "subscription_fee", "balance", "status_call",
    "call_result", "abonent_answer", "tech", "fixed_at", "account", "phone",
}

ALLOWED_SEARCH_FIELDS = {
    "msisdn": "msisdn__icontains",
    "phone": "phone__icontains",
    "client": "client__icontains",
    "account": "account__icontains",
}


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 500


def _change_perm_code():
    return f"{Actives._meta.app_label}.change_{Actives._meta.model_name}"


def _parse_search_fields(request) -> list[str]:
    raw = request.query_params.getlist("fields") or request.query_params.get("fields")
    if isinstance(raw, str):
        fields = [p.strip() for p in raw.split(",")]
    else:
        fields = list(raw or [])
    seen = set()
    cleaned = []
    for f in fields:
        if f in ALLOWED_SEARCH_FIELDS and f not in seen:
            cleaned.append(f)
            seen.add(f)
    return cleaned


def _apply_filters(request, qs):
    q = (request.query_params.get("q") or "").strip()
    fields = _parse_search_fields(request)

    if q:
        if fields:
            cond = Q()
            for f in fields:
                cond |= Q(**{ALLOWED_SEARCH_FIELDS[f]: q})
            qs = qs.filter(cond)
        else:
            cond = Q()
            for lookup in ALLOWED_SEARCH_FIELDS.values():
                cond |= Q(**{lookup: q})
            qs = qs.filter(cond)

    ordering = request.query_params.get("ordering")
    if ordering:
        fld = ordering.lstrip("-")
        if fld in ORDERABLE:
            qs = qs.order_by(ordering)
    return qs


# -------------------- ViewSets --------------------

class ActivesViewSet(viewsets.ModelViewSet):
    queryset = Actives.objects.all().order_by("-created_at")
    serializer_class = ActivesSerializer
    permission_classes = [permissions.IsAuthenticated, DjangoModelPermissions]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        qs = super().get_queryset()
        status_filter = self.request.query_params.get("status")
        if status_filter:
            qs = qs.filter(status__icontains=status_filter)
        return _apply_filters(self.request, qs)

    @action(detail=True, methods=["get", "patch"], url_path="fixation")
    def fixation(self, request, pk=None):
        abonent = self.get_object()

        if request.method == "GET":
            return Response(ActivesSerializer(abonent).data, status=status.HTTP_200_OK)

        perm_code = _change_perm_code()
        if not (request.user.is_superuser or request.user.has_perm(perm_code)):
            raise PermissionDenied(f"Недостаточно прав: требуется '{perm_code}'.")

        ser = ActivesFixationWriteSerializer(abonent, data=request.data, partial=True)
        ser.is_valid(raise_exception=True)
        ser.save()

        abonent.fixed_by = request.user
        abonent.fixed_at = timezone.now()
        abonent.save(update_fields=["fixed_by", "fixed_at", "updated_at"])
        return Response(ActivesSerializer(abonent).data, status=status.HTTP_200_OK)


class SuspendsViewSet(viewsets.ModelViewSet):
    queryset = Suspends.objects.all().order_by("-created_at")
    serializer_class = ActivesSerializer  # если у Suspends свой сериалайзер — поменяй здесь
    permission_classes = [permissions.IsAuthenticated, DjangoModelPermissions]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        qs = super().get_queryset()
        return _apply_filters(self.request, qs)

    @action(detail=True, methods=["get", "patch"], url_path="fixation")
    def fixation(self, request, pk=None):
        abonent = self.get_object()

        if request.method == "GET":
            return Response(ActivesSerializer(abonent).data, status=status.HTTP_200_OK)

        perm_code = _change_perm_code()
        if not (request.user.is_superuser or request.user.has_perm(perm_code)):
            raise PermissionDenied(f"Недостаточно прав: требуется '{perm_code}'.")

        ser = ActivesFixationWriteSerializer(abonent, data=request.data, partial=True)
        ser.is_valid(raise_exception=True)
        ser.save()

        abonent.fixed_by = request.user
        abonent.fixed_at = timezone.now()
        abonent.save(update_fields=["fixed_by", "fixed_at", "updated_at"])
        return Response(ActivesSerializer(abonent).data, status=status.HTTP_200_OK)


class FixationsViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = ActivesSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = StandardResultsSetPagination

    def get_queryset(self):
        qs = Actives.objects.filter(fixed_by__isnull=False).order_by("-fixed_at")
        qs = _apply_filters(self.request, qs)
        operator_id = self.request.query_params.get("operator")
        if operator_id:
            qs = qs.filter(fixed_by_id=operator_id)
        return qs


class SBMSAccountViewSet(viewsets.ModelViewSet):
    queryset = SBMSAccount.objects.filter(is_active=True).order_by("label")
    serializer_class = SBMSAccountSerializer
    permission_classes = [permissions.IsAuthenticated, DjangoModelPermissions]


class GoogleAccountViewSet(viewsets.ModelViewSet):
    queryset = GoogleAccount.objects.filter(is_active=True).select_related("sbms_account").order_by("label")
    serializer_class = GoogleAccountSerializer
    permission_classes = [permissions.IsAuthenticated, DjangoModelPermissions]


class ExcelUploadViewSet(viewsets.ModelViewSet):
    queryset = ExcelUpload.objects.select_related("uploaded_by").order_by("-uploaded_at")
    serializer_class = ExcelUploadSerializer
    permission_classes = [permissions.IsAuthenticated, DjangoModelPermissions]
    pagination_class = StandardResultsSetPagination


class OperatorsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = User.objects.filter(role=User.ROLE_OPERATOR).order_by("fio", "username")
    serializer_class = OperatorSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = None


# -------------------- Auth helpers --------------------

class MeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        return Response(MeSerializer(request.user).data, status=status.HTTP_200_OK)


class LogoutView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        refresh = request.data.get("refresh")
        if not refresh:
            return Response({"detail": "Field 'refresh' is required."}, status=status.HTTP_400_BAD_REQUEST)
        try:
            token = RefreshToken(refresh)
            token.blacklist()
        except Exception as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        return Response({"detail": "Logged out."}, status=status.HTTP_205_RESET_CONTENT)


class MyTokenVerifyView(TokenVerifyView):
    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):
        token_str = request.data.get("token")
        if not token_str:
            return Response({"detail": "Field 'token' is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            UntypedToken(token_str)
        except (InvalidToken, TokenError):
            return Response({"detail": "Token is invalid or expired.", "code": "token_not_valid"}, status=401)

        backend = TokenBackend(
            algorithm=getattr(settings, "SIMPLE_JWT", {}).get("ALGORITHM", "HS256"),
            signing_key=getattr(settings, "SIMPLE_JWT", {}).get("SIGNING_KEY", settings.SECRET_KEY),
            verifying_key=getattr(settings, "SIMPLE_JWT", {}).get("VERIFYING_KEY", None),
            audience=getattr(settings, "SIMPLE_JWT", {}).get("AUDIENCE", None),
            issuer=getattr(settings, "SIMPLE_JWT", {}).get("ISSUER", None),
            jwk_url=getattr(settings, "SIMPLE_JWT", {}).get("JWK_URL", None),
            leeway=getattr(settings, "SIMPLE_JWT", {}).get("LEEWAY", 0),
        )
        payload = backend.decode(token_str, verify=True)

        user_info = None
        uid = payload.get("user_id") or payload.get("user") or payload.get("sub")
        if uid:
            try:
                u = User.objects.only("id", "username", "first_name", "last_name", "email").get(pk=uid)
                user_info = MeSerializer(u).data
            except User.DoesNotExist:
                user_info = None

        return Response({"valid": True, "payload": payload, "user": user_info}, status=status.HTTP_200_OK)


def _fmt_local(dt, fmt="%Y-%m-%d %H:%M:%S") -> str:
    if not dt:
        return ""
    try:
        # If dt is naïve, attach default timezone
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_default_timezone())
        # Convert to localtime and format
        return timezone.localtime(dt).strftime(fmt)
    except Exception:
        # Fallback: best-effort formatting without tz ops
        try:
            return dt.strftime(fmt)
        except Exception:
            return ""


def export_all_suspends(request):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Suspends"

    ws.append([
        "DEPARTMENTS", "MSISDN", "Статус","CLIENT", "PHONE", "BRANCHES","Дата с которой Статус","[Дней в статусе]","Дата Списания АП","RATE_PLAN","Баланс","Абон плата","ACCOUNT",
        "Статус звонка", "Результат обзвона", "Ответ абонента",
        "Дата обзвона"
    ])

    for obj in Actives.objects.all():
        created_at_str = _fmt_local(obj.created_at)
        fixed_at_str   = _fmt_local(obj.fixed_at)

        ws.append([
            obj.departments,
            obj.msisdn,
            obj.status,
            obj.client,
            obj.phone,
            obj.branches,
            obj.status_from,
            obj.days_in_status,
            obj.write_offs_date,
            obj.rate_plan,
            obj.balance,
            obj.subscription_fee,
            obj.account,
            obj.status_call,
            obj.call_result,
            obj.abonent_answer,
            fixed_at_str
        ])

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="all_suspends.xlsx"'
    wb.save(response)
    return response


def export_all_actives(request):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Actives"

    ws.append([
        "DEPARTMENTS", "MSISDN", "Статус","CLIENT", "PHONE", "BRANCHES","Дата с которой Статус","[Дней в статусе]","Дата Списания АП","RATE_PLAN","Баланс","Абон плата","ACCOUNT",
        "Статус звонка", "Результат обзвона", "Ответ абонента",
        "Дата обзвона"
    ])

    for obj in Actives.objects.all():
        created_at_str = _fmt_local(obj.created_at)
        fixed_at_str   = _fmt_local(obj.fixed_at)

        ws.append([
            obj.departments,
            obj.msisdn,
            obj.status,
            obj.client,
            obj.phone,
            obj.branches,
            obj.status_from,
            obj.days_in_status,
            obj.write_offs_date,
            obj.rate_plan,
            obj.balance,
            obj.subscription_fee,
            obj.account,
            obj.status_call,
            obj.call_result,
            obj.abonent_answer,
            fixed_at_str
        ])

    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = 'attachment; filename="all_actives.xlsx"'
    wb.save(response)
    return response


def _capture_run(func):
    buffer = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = sys.stderr = buffer
    try:
        result = func()
    except Exception as e:
        print("❌ Ошибка:", e)
        result = None
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr

    log_text = buffer.getvalue()
    return log_text, result


@staff_member_required
def export_suspends_phones_csv(request):
    qs = (Suspends.objects
          .values_list("phone", flat=True)
          .exclude(phone__isnull=True)
          .exclude(phone__exact=""))

    if request.GET.get("distinct"):
        qs = qs.distinct()

    now = timezone.localtime()
    filename = f"suspends_phones_{now:%Y-%m-%d_%H-%M-%S}.csv"

    resp = HttpResponse(content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    resp.write("\ufeff")

    writer = csv.writer(resp, lineterminator="\n")
    writer.writerow(["PHONE"])

    for phone in qs.iterator(chunk_size=5000):
        p = (phone or "").strip()
        if p:
            writer.writerow([p])

    return resp


class ImportUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    permission_classes = [IsAdminUser]

    def post(self, request, *args, **kwargs):
        f = request.FILES.get("file")
        if not f:
            return Response({"detail": "Приложите файл в поле 'file'."},
                            status=status.HTTP_400_BAD_REQUEST)

        job = UploadJob.objects.create(
            created_by=request.user if request.user.is_authenticated else None,
            excel_file=f,
            status="pending",
        )
        t = threading.Thread(target=run_import, args=(job.id,), daemon=True)
        t.start()

        return Response({"job_id": job.id}, status=status.HTTP_201_CREATED)


class ImportStatusView(APIView):
    permission_classes = [IsAdminUser]

    def get(self, request, job_id: int, *args, **kwargs):
        try:
            job = UploadJob.objects.get(id=job_id)
        except UploadJob.DoesNotExist:
            return Response({"detail": "Job not found."}, status=status.HTTP_404_NOT_FOUND)

        data = UploadJobSerializer(job).data
        return Response(data, status=status.HTTP_200_OK)