# crm_api/services/excel_importer.py
import pandas as pd
from sqlalchemy import create_engine, text
from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import Q
from datetime import date, datetime as dt
from urllib.parse import quote_plus
from crm_api.models import UploadJob, Actives

BATCH = 1000

# Используем безопасный upsert без дублей в БД
USE_DJANGO_ORM_UPSERT = True

TARGET_TABLE = Actives._meta.db_table

COLUMNS = [
    "msisdn",
    "departments",
    "status_from",
    "days_in_status",
    "write_offs_date",
    "client",
    "rate_plan",
    "balance",
    "subscription_fee",
    "account",
    "branches",
    "status",
    "phone",
]

UNIQ_FIELDS = {"msisdn", "account", "phone"}

COLUMN_ALIASES = {
    "MSISDN": "msisdn",
    "DEPARTMENTS": "departments",
    "Дата с которой Статус": "status_from",
    "[Дней в статусе]": "days_in_status",
    "Дата Списания АП": "write_offs_date",
    "CLIENT": "client",
    "RATE_PLAN": "rate_plan",
    "Баланс": "balance",
    "Абон плата": "subscription_fee",
    "ACCOUNT": "account",
    "BRANCHES": "branches",
    "Статус": "status",
    "PHONE": "phone",
}

UPSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} ({", ".join(COLUMNS)})
VALUES ({", ".join(":" + c for c in COLUMNS)})
ON DUPLICATE KEY UPDATE
  departments=VALUES(departments),
  status_from=VALUES(status_from),
  days_in_status=VALUES(days_in_status),
  write_offs_date=VALUES(write_offs_date),
  client=VALUES(client),
  rate_plan=VALUES(rate_plan),
  balance=VALUES(balance),
  subscription_fee=VALUES(subscription_fee),
  account=VALUES(account),
  branches=VALUES(branches),
  status=VALUES(status),
  phone=VALUES(phone)
"""

NUMERIC_INT_FIELDS = {"days_in_status", "subscription_fee"}
NUMERIC_DEC_FIELDS = {"balance"}

# ---------- НОРМАЛИЗАЦИЯ СТАТУСА ----------
def _normalize_status(value) -> str:
    """Любые 'suspend*' -> 'suspend 1 month'; любые 'active/актив*' -> 'active'."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    # CHANGED: всё, что содержит 'susp' / 'suspend' / 'приостан' -> 'suspend 1 month'
    if "susp" in s or "suspend" in s or "приостан" in s:
        return "suspend 1 month"
    # CHANGED: любые 'active', 'activ', 'актив', 'включ' -> 'active'
    if "activ" in s or "active" in s or "актив" in s or "включ" in s:
        return "active"
    # иначе оставляем как есть (в нижнем регистре)
    return s
# -----------------------------------------

def _coerce(val, field):
    """Числа; даты -> 'YYYY-MM-DD'; пустые для UNIQ -> None; статус -> нормализуем; остальное -> str/''."""
    if pd.isna(val) or (isinstance(val, str) and not val.strip()):
        if field in UNIQ_FIELDS:
            return None
        if field in NUMERIC_INT_FIELDS:
            return 0
        if field in NUMERIC_DEC_FIELDS:
            return 0
        return ""
    if field in NUMERIC_INT_FIELDS:
        try:
            return int(str(val).replace(" ", "").replace(",", ".").split(".")[0])
        except Exception:
            return 0
    if field in NUMERIC_DEC_FIELDS:
        try:
            s = str(val).replace(" ", "").replace(",", ".")
            return float(s)
        except Exception:
            return 0
    if isinstance(val, str):
        v = val.strip()
        if field == "status":                # CHANGED
            return _normalize_status(v)      # CHANGED
        return v if v else (None if field in UNIQ_FIELDS else "")
    if isinstance(val, (pd.Timestamp, dt, date)):
        if field == "status":                # CHANGED
            return _normalize_status(val)    # CHANGED
        return str(val)[:10]
    return _normalize_status(val) if field == "status" else str(val)  # CHANGED

def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not COLUMN_ALIASES:
        return df
    ren = {src: dst for src, dst in COLUMN_ALIASES.items() if src in df.columns}
    if ren:
        df = df.rename(columns=ren)
    return df

# TCP engine (используется только если USE_DJANGO_ORM_UPSERT=False)
def _make_engine():
    default = settings.DATABASES["default"]
    user = default["USER"]
    pwd = default["PASSWORD"]
    name = default["NAME"]
    host = default.get("HOST") or "127.0.0.1"
    if host == "localhost":  # Ubuntu: гарантируем TCP
        host = "127.0.0.1"
    port = int(default.get("PORT") or 3306)
    dsn = (
        f"mysql+mysqldb://{quote_plus(user)}:{quote_plus(pwd)}@"
        f"{host}:{port}/{name}?charset=utf8mb4"
    )
    return create_engine(dsn, pool_pre_ping=True, pool_recycle=3600)

# -------- ORM upsert helpers --------
def _choose_lookup(row: dict) -> tuple[str | None, str | None]:
    """Приоритет ключа: msisdn -> account -> phone."""
    for f in ("msisdn", "account", "phone"):
        v = row.get(f)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return f, s
    return None, None

def _safe_update_fields(obj: Actives, defaults: dict, key_field: str) -> list[str]:
    """
    Обновляем поля безопасно:
    - key_field не трогаем;
    - другие уникальные поля апдейтим только если у obj пусто или значение свободно в БД;
    - обычные поля (включая status) апдейтим всегда.
    """
    changed = []
    for field, val in defaults.items():
        if field == key_field:
            continue
        if field in UNIQ_FIELDS:
            current = getattr(obj, field)
            new_has = (val is not None and str(val).strip() != "")
            cur_empty = (current is None or str(current).strip() == "")
            if new_has and cur_empty:
                if not Actives.objects.filter(**{field: val}).exclude(pk=obj.pk).exists():
                    setattr(obj, field, val)
                    changed.append(field)
            continue
        if getattr(obj, field) != val:
            setattr(obj, field, val)
            changed.append(field)
    return changed

def _orm_upsert_rows(rows: list[dict]) -> tuple[int, int, str]:
    ok = err = 0
    last_err = ""
    for r in rows:
        key_field, key_value = _choose_lookup(r)
        if not key_field:
            err += 1
            last_err = "No unique identifier (msisdn/account/phone) provided"
            continue
        defaults = {k: r.get(k) for k in COLUMNS if k != key_field}
        try:
            obj, created = Actives.objects.update_or_create(
                **{key_field: key_value},
                defaults=defaults,
            )
            ok += 1
        except IntegrityError as e:
            last_err = str(e)
            try:
                with transaction.atomic():
                    query = Q()
                    for f in ("msisdn", "account", "phone"):
                        v = r.get(f)
                        if v is not None and str(v).strip():
                            query |= Q(**{f: str(v).strip()})
                    obj = Actives.objects.select_for_update().filter(query).first()
                    if obj:
                        changed = _safe_update_fields(obj, defaults | {key_field: key_value}, key_field)
                        if changed:
                            obj.save(update_fields=list(set(changed)))
                        ok += 1
                    else:
                        obj = Actives(**({key_field: key_value} | defaults))
                        obj.save()
                        ok += 1
            except Exception as e2:
                err += 1
                last_err = str(e2)
    return ok, err, last_err

# -------- MAIN --------
def run_import(job_id: int):
    job = UploadJob.objects.get(id=job_id)
    job.status = "running"
    job.save(update_fields=["status"])

    try:
        df = pd.read_excel(
            job.excel_file.path,
            dtype={"msisdn": str, "phone": str, "account": str},
        )
        df = _rename_columns(df)

        missing = [c for c in COLUMNS if c not in df.columns]
        if missing:
            for m in missing:
                df[m] = None

        df = df[COLUMNS].copy()
        for c in COLUMNS:
            df[c] = df[c].map(lambda v: _coerce(v, c))

        # отбрасываем строки без всех трёх ключей
        keymask = (
            df["msisdn"].fillna("").astype(str).str.strip().astype(bool)
            | df["account"].fillna("").astype(str).str.strip().astype(bool)
            | df["phone"].fillna("").astype(str).str.strip().astype(bool)
        )
        df = df.loc[keymask].reset_index(drop=True)

        total = len(df)
        job.total_rows = total
        job.save(update_fields=["total_rows"])

        processed = succeeded = failed = 0
        last_err = ""

        if USE_DJANGO_ORM_UPSERT:
            for start in range(0, total, BATCH):
                chunk_df = df.iloc[start:start + BATCH]
                rows = chunk_df.to_dict(orient="records")
                ok, err, last_err = _orm_upsert_rows(rows)
                processed += len(rows)
                succeeded += ok
                failed += err
                UploadJob.objects.filter(id=job.id).update(
                    processed_rows=processed,
                    succeeded_rows=succeeded,
                    failed_rows=failed,
                    last_error=last_err,
                )
        else:
            engine = _make_engine()
            with engine.begin() as conn:
                for start in range(0, total, BATCH):
                    chunk_df = df.iloc[start:start + BATCH]
                    rows = chunk_df.to_dict(orient="records")
                    try:
                        conn.execute(text(UPSERT_SQL), rows)
                        ok, err, last_err = len(rows), 0, ""
                    except Exception as e:
                        ok, err, last_err = 0, 0, str(e)
                        for r in rows:
                            try:
                                conn.execute(text(UPSERT_SQL), r)
                                ok += 1
                            except Exception as e2:
                                err += 1
                                last_err = str(e2)
                    processed += len(rows)
                    succeeded += ok
                    failed += err
                    UploadJob.objects.filter(id=job.id).update(
                        processed_rows=processed,
                        succeeded_rows=succeeded,
                        failed_rows=failed,
                        last_error=last_err,
                    )

        job.status = "done"
        job.last_error = last_err
        job.save(update_fields=["status", "last_error"])

    except Exception as e:
        job.status = "failed"
        job.last_error = str(e)
        job.save(update_fields=["status", "last_error"])
