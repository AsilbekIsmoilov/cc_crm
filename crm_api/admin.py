from datetime import datetime
from io import StringIO
import threading

from django import forms
from django.contrib import admin, messages
from django.contrib.auth import get_user_model
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse
from django.shortcuts import render
from django.urls import path
from django.utils import timezone
from django.utils.html import format_html


from .models import (
    Actives,
    Suspends,
    SBMSAccount,
    GoogleAccount,
    ExcelUpload,
    SbmsAudit,
    RecheckRun,
    UploadJob,
)
from .services.excel_importer import run_import
from crm_api.services.users import bulk_create_operators, build_csv_from_results


BASE_LIST_DISPLAY = (
    "msisdn", "client", "status", "branches", "rate_plan",
    "subscription_fee", "balance",
    "status_call", "call_result", "abonent_answer", "tech",
    "updated_at", 'fixed_by'
)

BASE_FIELDSETS = (
    ("Идентификация", {
        "fields": ("msisdn", "phone", "client", "account"),
    }),
    ("Организационное", {
        "fields": ("departments", "branches"),
    }),
    ("Тариф/баланс", {
        "fields": ("rate_plan", "subscription_fee", "balance"),
    }),
    ("Статус абонента", {
        "fields": ("status", "status_from", "days_in_status", "write_offs_date"),
    }),
    ("Фиксация", {
        "fields": ("status_call", "call_result", "abonent_answer", "note", "tech"),
    }),
    ("Служебные", {
        "fields": ("created_at", "updated_at"),
    }),
)


@admin.register(Actives)
class ActivesAdmin(admin.ModelAdmin):
    list_display = BASE_LIST_DISPLAY
    list_display_links = ("msisdn", "client")
    list_editable = ("status_call", "call_result", "abonent_answer", "tech")
    search_fields = ("msisdn", "phone", "client", "account", "note")
    list_filter = (
        ("created_at", admin.DateFieldListFilter),
        ("updated_at", admin.DateFieldListFilter),
        "branches", "rate_plan", "status",
        "status_call", "call_result", "abonent_answer", "tech",
        "fixed_by",
    )
    readonly_fields = ("created_at", "updated_at")
    fieldsets = BASE_FIELDSETS
    ordering = ("-updated_at",)
    list_per_page = 50
    date_hierarchy = "created_at"
    save_on_top = True
    empty_value_display = "—"


@admin.register(Suspends)
class SuspendsAdmin(ActivesAdmin):
    pass


class BulkOperatorsForm(forms.Form):
    count = forms.IntegerField(min_value=1, max_value=5000, initial=50, label="Сколько создать")
    prefix = forms.CharField(max_length=50, initial="operator", label="Префикс логина")
    start = forms.IntegerField(min_value=0, initial=0, label="Начать с индекса")
    reset_existing = forms.BooleanField(required=False, initial=False, label="Сбросить пароли существующим")


User = get_user_model()


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    change_list_template = "admin/crm_api/user/change_list.html"

    def get_urls(self):
        urls = super().get_urls()
        my = [
            path(
                "bulk-create-operators/",
                self.admin_site.admin_view(self.bulk_create_operators_view),
                name="crm_api_user_bulk_create_operators",
            ),
        ]
        return my + urls

    def bulk_create_operators_view(self, request):
        initial = {"count": 50, "prefix": "operator", "start": 0, "reset_existing": False}
        form = BulkOperatorsForm(request.POST or None, initial=initial)

        if request.method == "POST":
            if not self.has_change_permission(request):
                raise PermissionDenied

            data = form.cleaned_data if form.is_valid() else initial
            rows = bulk_create_operators(
                count=data["count"],
                prefix=data["prefix"],
                start=data["start"],
                reset_existing=data["reset_existing"],
            )
            csv_io: StringIO = build_csv_from_results(rows)
            filename = f"operators_{data['prefix']}_{int(timezone.now().timestamp())}.csv"
            resp = HttpResponse(csv_io.getvalue(), content_type="text/csv; charset=utf-8")
            resp["Content-Disposition"] = f'attachment; filename="{filename}"'
            return resp

        context = {
            **self.admin_site.each_context(request),
            "title": "Bulk create operators (download CSV)",
            "opts": User._meta,
            "form": form,
        }
        return render(request, "admin/crm_api/user/bulk_create_operators.html", context)


@admin.register(SBMSAccount)
class SBMSAccountAdmin(admin.ModelAdmin):
    list_display = ("label", "username", "is_active", "max_google_accounts", "google_accounts_count")
    list_filter = ("is_active",)
    search_fields = ("label", "username")


@admin.register(GoogleAccount)
class GoogleAccountAdmin(admin.ModelAdmin):
    list_display = ("label", "sbms_account", "google_email", "is_active")
    list_filter = ("is_active", "sbms_account")
    search_fields = ("label", "google_email", "user_data_dir")


admin.site.register(ExcelUpload)
admin.site.register(SbmsAudit)
admin.site.register(RecheckRun)


@admin.register(UploadJob)
class UploadJobAdmin(admin.ModelAdmin):

    list_display = (
        "id",
        "status",
        "progress",
        "succeeded_rows",
        "failed_rows",
        "total_rows",
        "excel_file_link",
        "created_by",
        "created_at",
    )
    list_filter = (
        "status",
        ("created_at", admin.DateFieldListFilter),
        "created_by",
    )
    search_fields = ("id", "excel_file")
    readonly_fields = (
        "status",
        "total_rows",
        "processed_rows",
        "succeeded_rows",
        "failed_rows",
        "last_error",
        "created_by",
        "created_at",
        "excel_file",
        "target_table",
        "progress",
        "duration",
    )
    ordering = ("-id",)
    actions = ("restart_import",)
    save_on_top = True

    def has_add_permission(self, request):
        return False

    def progress(self, obj: UploadJob):
        if obj.total_rows and obj.processed_rows is not None:
            p = int(round((obj.processed_rows / max(1, obj.total_rows)) * 100))
            return f"{p}%"
        return "—"
    progress.short_description = "Прогресс"

    def excel_file_link(self, obj: UploadJob):
        if obj.excel_file:
            return format_html('<a href="{}" target="_blank">скачать</a>', obj.excel_file.url)
        return "—"
    excel_file_link.short_description = "Файл"

    def duration(self, obj: UploadJob):
        return "—"
    duration.short_description = "Длительность (оценка)"

    def restart_import(self, request, queryset):
        restarted = 0
        skipped = 0
        for job in queryset:
            if job.status in ("pending", "failed"):
                UploadJob.objects.filter(id=job.id).update(status="pending", processed_rows=0, succeeded_rows=0, failed_rows=0, last_error="")
                t = threading.Thread(target=run_import, args=(job.id,), daemon=True)
                t.start()
                restarted += 1
            else:
                skipped += 1
        if restarted:
            self.message_user(request, f"Перезапущено: {restarted}", level=messages.SUCCESS)
        if skipped:
            self.message_user(request, f"Пропущено (уже running/done): {skipped}", level=messages.WARNING)
    restart_import.short_description = "Перезапустить импорт (pending/failed)"
