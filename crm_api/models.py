from django.db import models
from django.db.models import Q
from django.contrib.auth.models import AbstractUser
from django.conf import settings
from django.utils import timezone


class User(AbstractUser):
    ROLE_OPERATOR = "operator"
    ROLE_ADMIN = "admin"
    ROLE_CHOICES = [
        (ROLE_OPERATOR, "Operator"),
        (ROLE_ADMIN, "Administrator"),
    ]
    role = models.CharField(max_length=10, choices=ROLE_CHOICES, default=ROLE_OPERATOR, db_index=True)
    fio = models.CharField(max_length=300,null=True,blank=True)

    class Meta:
        verbose_name = "User"
        verbose_name_plural = "Users"
        indexes = [models.Index(fields=["username"]), models.Index(fields=["role"]), models.Index(fields=["fio"])]

    def __str__(self):
        return f"{self.username} ({self.get_role_display()})"





STATUS_CALL_CHOICES = [
    ("Дозвонился", "Дозвонился"),
    ("Не дозвонился", "Не дозвонился"),
    ("Не звонили", "Не звонили"),
]
CALL_RESULT_CHOICES = [
    ("Нет ответа","Нет ответа"),
    ("Ответили на вопрос", "Ответили на вопрос"),
    ("Отказ от разговора", "Отказ от разговора"),
    ("Другой владелец номера", "Другой владелец номера"),
    ("Нет ответа", "Нет ответа"),
    ("Аппарат выкл", "Аппарат выкл"),
    ("Номер не существует", "Номер не существует"),
    ("Дубликат номера", "Дубликат номера"),
    ("Статус активный", "Статус активный"),
]
ABONENT_ANSWER_CHOICES = [
    ("Нет ответа","Нет ответа"),
    ("Временно нет потребности в интернете", "Временно нет потребности в интернете"),
    ("Финансовые трудности", "Финансовые трудности"),
    ("Забыли внести платеж", "Забыли внести платеж"),
    ("Высокая стоимость ТП", "Высокая стоимость ТП"),
    ("Не устраивает качество сети", "Не устраивает качество сети"),
    ("Плохое обслуживание", "Плохое обслуживание"),
    ("Переезд", "Переезд"),
    ("Смена технологии", "Смена технологии"),
    ("Смена провайдера", "Смена провайдера"),
    ("Абонент активный", "Абонент активный"),
    ("Оплатит в скором времени", "Оплатит в скором времени"),
    ("Другой","Другой")
]
TECH_CHOICES = [("pon", "pon"), ("vdsl", "vdsl"), ("adsl", "adsl"), ("ethernet", "ethernet")]

class Actives(models.Model):
    msisdn = models.CharField(max_length=250, null=True, blank=True)
    departments = models.CharField(max_length=300, null=True, blank=True)
    status_from = models.CharField(max_length=100, null=True, blank=True)
    days_in_status = models.PositiveIntegerField(default=0)
    write_offs_date = models.CharField(max_length=100, null=True, blank=True)
    client = models.CharField(max_length=300, null=True, blank=True)
    rate_plan = models.CharField(max_length=200, null=True, blank=True)
    balance = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    subscription_fee = models.IntegerField(default=0)
    account = models.CharField(max_length=32, null=True, blank=True)
    branches = models.CharField(max_length=300, null=True, blank=True)
    status = models.CharField(max_length=150, null=True, blank=True)
    phone = models.CharField(max_length=15, null=True, blank=True)

    status_call = models.CharField(max_length=20, choices=STATUS_CALL_CHOICES, null=True, blank=True)
    call_result = models.CharField(max_length=32, choices=CALL_RESULT_CHOICES, null=True, blank=True)
    abonent_answer = models.CharField(max_length=255, choices=ABONENT_ANSWER_CHOICES, null=True, blank=True)
    note = models.TextField(null=True, blank=True)
    tech = models.CharField(max_length=16, choices=TECH_CHOICES, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    fixed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="calls_made",
        verbose_name="Кто звонил",
    )
    fixed_at = models.DateTimeField(null=True, blank=True, verbose_name="Когда звонил")

    class Meta:
        verbose_name = "Abonent"
        verbose_name_plural = "Abonents"
        indexes = [
            models.Index(fields=["msisdn"]),
            models.Index(fields=["phone"]),
            models.Index(fields=["branches"]),
            models.Index(fields=["rate_plan"]),
            models.Index(fields=["status"]),
            models.Index(fields=["status_call"]),
            models.Index(fields=["call_result"]),
            models.Index(fields=["abonent_answer"]),
            models.Index(fields=["tech"]),
            models.Index(fields=["fixed_at"]),
            models.Index(fields=["fixed_by"]),
        ]

    def __str__(self):
        return f"{self.msisdn or '-'} — {self.client or '-'}"

    @property
    def who_called(self) -> str:
        if not self.fixed_by:
            return ""
        return (getattr(self.fixed_by, "fio", None) or self.fixed_by.get_full_name() or self.fixed_by.username)

class SuspendsManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(Q(status__icontains="suspend"))

class Suspends(Actives):
    objects = SuspendsManager()
    class Meta:
        proxy = True
        verbose_name = "Suspend"
        verbose_name_plural = "Suspends"

class SbmsAudit(models.Model):
    abonent = models.ForeignKey(Actives, on_delete=models.CASCADE, related_name="sbms_audit")
    msisdn = models.CharField(max_length=250)
    old_client = models.CharField(max_length=300, blank=True, default="")
    new_client = models.CharField(max_length=300, blank=True, default="")
    old_status = models.CharField(max_length=150, blank=True, default="")
    new_status = models.CharField(max_length=150, blank=True, default="")
    note = models.TextField(blank=True, default="")
    ok = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["msisdn"]),
            models.Index(fields=["ok"]),
        ]

    def __str__(self) -> str:  # NEW
        flag = "OK" if self.ok else "ERR"
        return f"[{flag}] {self.msisdn} {self.old_status} → {self.new_status}"

class SBMSAccount(models.Model):
    label = models.CharField(max_length=100)
    username = models.CharField(max_length=150)
    password = models.CharField(max_length=200)
    is_active = models.BooleanField(default=True)
    max_google_accounts = models.PositiveSmallIntegerField(default=5, help_text="Ожидаемое число привязанных Google-аккаунтов")

    class Meta:
        verbose_name = "SBMS account"
        verbose_name_plural = "SBMS accounts"
        indexes = [models.Index(fields=["is_active"]), models.Index(fields=["username"])]

    def __str__(self):
        return f"{self.label} ({self.username})"

    @property
    def google_accounts_count(self) -> int:
        return self.google_accounts.filter(is_active=True).count()

class GoogleAccount(models.Model):
    sbms_account = models.ForeignKey(SBMSAccount, on_delete=models.PROTECT, related_name="google_accounts")
    label = models.CharField(max_length=100)
    google_email = models.CharField(max_length=255, blank=True, default="")
    user_data_dir = models.CharField(max_length=500, help_text="Путь к папке профиля Chrome")
    profile_directory = models.CharField(max_length=100, default="Default", help_text="Имя профиля внутри user-data-dir",null=True,blank=True)
    chrome_binary = models.CharField(max_length=500, blank=True, default="", help_text="Путь к chrome, если нестандартный")
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Google account (Chrome profile)"
        verbose_name_plural = "Google accounts (Chrome profiles)"
        indexes = [models.Index(fields=["is_active"]), models.Index(fields=["sbms_account"])]

    def __str__(self):
        return f"{self.label} ({self.google_email or 'no-google'})"

class ExcelUpload(models.Model):
    file = models.FileField(upload_to="uploads/%Y/%m/%d")
    original_name = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)


class RecheckRun(models.Model):
    run_id = models.CharField(max_length=64)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True, blank=True)
    checked = models.IntegerField(default=0)
    activated = models.IntegerField(default=0)
    still_suspend = models.IntegerField(default=0)
    updated = models.IntegerField(default=0)
    errors = models.IntegerField(default=0)
    log_path = models.CharField(max_length=512, blank=True, default="")
    activated_path = models.CharField(max_length=512, blank=True, default="")
    log_file = models.FileField(upload_to="runlogs/%Y/%m/%d", blank=True, null=True)


class UploadJob(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("running", "Running"),
        ("done", "Done"),
        ("failed", "Failed"),
    ]

    created_by = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)

    excel_file = models.FileField(upload_to="imports/%Y/%m/%d/")
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="pending")

    total_rows = models.IntegerField(default=0)
    processed_rows = models.IntegerField(default=0)
    succeeded_rows = models.IntegerField(default=0)
    failed_rows = models.IntegerField(default=0)

    last_error = models.TextField(blank=True, default="")
    target_table = models.CharField(max_length=128, default="actives")  # по умолчанию

    def __str__(self):
        return f"UploadJob#{self.id} {self.status}"