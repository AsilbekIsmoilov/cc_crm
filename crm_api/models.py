from django.db.models import Q
from django.contrib.auth.models import AbstractUser
from django.conf import settings
from django.db import models, transaction

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

from django.conf import settings
from django.db import models, transaction
from django.db.models import Q

from django.conf import settings
from django.db import models, transaction
from django.db.models import Q

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
        verbose_name = "Active"
        verbose_name_plural = "Actives"
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


class Fixeds(models.Model):
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

    fixed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="fixed_calls_moved",
        verbose_name="Кто звонил",
    )
    fixed_at = models.DateTimeField(null=True, blank=True, verbose_name="Когда звонил")

    created_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(null=True, blank=True)
    moved_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    class Meta:
        verbose_name = "Fixed"
        verbose_name_plural = "Fixeds"
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
            models.Index(fields=["created_at"]),
            models.Index(fields=["updated_at"]),
            models.Index(fields=["moved_at"]),
        ]

    def __str__(self):
        return f"{self.msisdn or '-'} — {self.client or '-'} (fixed)"


def move_suspends_with_status_call_to_fixeds(
    chunk_size: int = 2000,
    order_by: str = "pk",
    delete_after_copy: bool = True,
    ignore_duplicates: bool = False,
    dry_run: bool = False,
) -> int:
    qs = Suspends.objects.exclude(status_call__isnull=True).exclude(status_call="").order_by(order_by)

    fields_to_copy = [
        "msisdn", "departments", "status_from", "days_in_status", "write_offs_date",
        "client", "rate_plan", "balance", "subscription_fee", "account", "branches",
        "status", "phone", "status_call", "call_result", "abonent_answer", "note",
        "tech", "fixed_by", "fixed_at",'created_at',"updated_at"
    ]

    moved = 0
    to_create = []
    ids_to_delete = []
    for obj in qs.only(*fields_to_copy).iterator(chunk_size=chunk_size):
        data = {f: getattr(obj, f) for f in fields_to_copy}
        to_create.append(Fixeds(**data))
        ids_to_delete.append(obj.pk)

        if len(to_create) >= chunk_size:
            if not dry_run:
                with transaction.atomic():
                    Fixeds.objects.bulk_create(
                        to_create,
                        batch_size=chunk_size,
                        ignore_conflicts=ignore_duplicates,
                    )
                    if delete_after_copy:
                        Actives.objects.filter(pk__in=ids_to_delete).delete()
            moved += len(to_create)

            to_create.clear()
            ids_to_delete.clear()

    if to_create:
        if not dry_run:
            with transaction.atomic():
                Fixeds.objects.bulk_create(
                    to_create,
                    batch_size=chunk_size,
                    ignore_conflicts=ignore_duplicates,
                )
                if delete_after_copy:
                    Actives.objects.filter(pk__in=ids_to_delete).delete()
        moved += len(to_create)

    return moved

    @property
    def who_called(self) -> str:
        if not self.fixed_by:
            return ""
        return (getattr(self.fixed_by, "fio", None) or self.fixed_by.get_full_name() or self.fixed_by.username)




class ExcelUpload(models.Model):
    file = models.FileField(upload_to="uploads/%Y/%m/%d")
    original_name = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)

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