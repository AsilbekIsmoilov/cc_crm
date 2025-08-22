import logging
import os
import io
import re
import time
import signal
from datetime import timedelta

import unicodedata
from decimal import Decimal
from typing import Dict, List, Tuple, Iterable, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
import django
django.setup()

from django.conf import settings
from django.core.files.base import File
from django.db import transaction
from django.utils import timezone

from crm_api.models import Actives, ExcelUpload, GoogleAccount
from crm_api.services.filter_sbms import (
    build_driver,
    login,
    open_combined_form,
    perform_search_fast,
    read_name_and_status,
    switch_to_any_window_and_frame_with,
    HEADLESS_DEFAULT,
)
from crm_api.logfilters import SafeWorkerFormatter, EnsureWorkerFilter

from crm_api.services.filter_sbms import (
    ensure_chrome_profiles_dirs,
    BASE_CHROME_PROFILES_DIR,
    GA_FOLDERS_COUNT,
)

BATCH_SAVE = int(os.getenv("SBMS_BATCH_SAVE", "100"))
LOGIN_USER_ENV_FALLBACK = os.getenv("SBMS_USER", "")
LOGIN_PASS_ENV_FALLBACK = os.getenv("SBMS_PASS", "")

IMPORT_GA_LABELS = [s.strip() for s in os.getenv("IMPORT_GA_LABELS", "").split(",") if s.strip()]
IMPORT_MAX_WORKERS = int(os.getenv("IMPORT_MAX_WORKERS", "6") or "6")

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def _attach_run_file_logger(run_id: str, suffix: str = "main") -> tuple[logging.Logger, logging.Handler, str]:
    logs_dir = Path(getattr(settings, "RUNLOGS_DIR", Path(settings.BASE_DIR) / "runlogs"))
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{run_id}-{suffix}.log"

    logger = logging.getLogger("crm")
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fmt = SafeWorkerFormatter("%(asctime)s | %(levelname)s | %(worker)s | %(message)s", "%H:%M:%S")
    handler.setFormatter(fmt)
    handler.addFilter(EnsureWorkerFilter())
    logger.addHandler(handler)
    return logger, handler, str(log_path)

def _detach_file_logger(handler: logging.Handler):
    try:
        logger = logging.getLogger("crm")
        logger.removeHandler(handler)
        handler.close()
    except Exception:
        pass

def _log(tag: str, msg: str, level: int = logging.INFO) -> None:
    line = f"{_ts()} | {tag:<10} | pid={os.getpid()} | {msg}"
    print(line, flush=True)
    try:
        logging.getLogger("crm").log(level, msg, extra={"worker": tag})
    except Exception:
        pass

SPACE_RE = re.compile(r"[\s\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000]+")
DASH_RANGE = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
PLACEHOLDER_RE = re.compile(rf"^\s*[-{DASH_RANGE}\.]*\s*$")
PLACEHOLDER_WORDS = {"", "null", "none", "n/a", "nan"}

def _collapse(s: str | None) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = SPACE_RE.sub(" ", s)
    return s.strip()

def _is_placeholder(v) -> bool:
    s = _collapse(v).lower()
    return s in PLACEHOLDER_WORDS or PLACEHOLDER_RE.fullmatch(s) is not None

def _to_decimal(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    s = _collapse(v).replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return None

def _to_int_smart(v) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    s = _collapse(v).replace(",", ".")
    if s == "":
        return 0
    try:
        return int(Decimal(s))
    except Exception:
        m = re.search(r"-?\d+", s)
        return int(m.group(0)) if m else 0

COLMAP = {
    "MSISDN": "msisdn",
    "MSISDN/Login": "msisdn",
    "MSISDN / Login": "msisdn",
    "Логин": "msisdn",

    "DEPARTMENTS": "departments",
    "Дата с которой Статус": "status_from",
    "Дата Списания АП": "write_offs_date",

    "Дней в статусе": "days_in_status",
    "[Дней в статусе]": "days_in_status",

    "CLIENT": "client",
    "RATE_PLAN": "rate_plan",
    "Баланс": "balance",
    "Абон плата": "subscription_fee",
    "ACCOUNT": "account",
    "BRANCHES": "branches",
    "Статус": "status",
    "PHONE": "phone",
}
MSISDN_COL = "MSISDN"

def _rename_columns(df):
    rename_map = {}
    for col in df.columns:
        n = _collapse(col).lower()
        for k_excel in COLMAP.keys():
            if _collapse(k_excel).lower() == n:
                rename_map[col] = k_excel
                break
    return df.rename(columns=rename_map), rename_map

def _row_to_payload(row: dict) -> dict:
    d: Dict[str, object] = {}
    for k_excel, model_field in COLMAP.items():
        if k_excel in row:
            d[model_field] = row[k_excel]

    d["days_in_status"]   = _to_int_smart(d.get("days_in_status"))
    d["subscription_fee"] = _to_int_smart(d.get("subscription_fee"))
    d["balance"]          = _to_decimal(d.get("balance"))

    for k in ["msisdn","departments","status_from","write_offs_date","client","rate_plan",
              "account","branches","status","phone"]:
        if k in d and d[k] is not None:
            d[k] = _collapse(d[k])

    if "account" in d and _is_placeholder(d.get("account")):
        d["account"] = None
    if "phone" in d and _is_placeholder(d.get("phone")):
        d["phone"] = None

    return d

def read_excel_payloads(content: bytes, filename: str, log_lines: List[str]) -> Dict[str, dict]:
    import pandas as pd
    engine = None
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xlsm")):
        engine = "openpyxl"
    xls = pd.ExcelFile(io.BytesIO(content), engine=engine)

    out: Dict[str, dict] = {}
    used = False
    for sheet in xls.sheet_names:
        df = xls.parse(sheet_name=sheet, dtype=str).fillna("")
        df, rmap = _rename_columns(df)
        if MSISDN_COL in df.columns:
            used = True
            log_lines.append(f"Sheet '{sheet}': mapped -> {sorted(list(rmap.values()))}")
            for r in df.to_dict(orient="records"):
                p = _row_to_payload(r)
                msisdn = _collapse(p.get("msisdn") or "")
                if not msisdn:
                    continue
                out[msisdn] = p
    if not used:
        log_lines.append("No sheets matched required columns (MSISDN).")
    return out

def split_even(items: List[str], k: int) -> List[List[str]]:
    n = len(items)
    if k <= 0:
        return [items]
    q, r = divmod(n, k)
    chunks = []
    start = 0
    for i in range(k):
        step = q + (1 if i < r else 0)
        chunks.append(items[start:start+step])
        start += step
    return chunks

def _subdict(d: Dict[str, dict], keys: Iterable[str]) -> Dict[str, dict]:
    return {k: d[k] for k in keys if k in d}

def _resolve_unique_conflicts(payload: dict, msisdn: str, counters: dict) -> dict:
    out = dict(payload)
    acc = out.get("account")
    if acc:
        exists_other = Actives.objects.filter(account=acc).exclude(msisdn=msisdn).exists()
        if exists_other:
            counters["account_conflict"] = counters.get("account_conflict", 0) + 1
            out.pop("account", None)
    ph = out.get("phone")
    if ph:
        exists_other = Actives.objects.filter(phone=ph).exclude(msisdn=msisdn).exists()
        if exists_other:
            counters["phone_conflict"] = counters.get("phone_conflict", 0) + 1
            out.pop("phone", None)
    return out

@transaction.atomic
def _write_batch_to_db(
    batch_msisdns: List[str],
    batch_results: Dict[str, Dict[str, str]],
    all_payloads: Dict[str, dict],
    counters: dict,
) -> int:
    saved = 0
    for ms in batch_msisdns:
        res = batch_results.get(ms)
        if not res:
            continue
        p = dict(all_payloads.get(ms, {}))
        if not p:
            continue
        if res.get("client"):
            p["client"] = res["client"]
        if res.get("status"):
            p["status"] = res["status"]
        if p.get("account") is None:
            counters["acc_sanitized"] = counters.get("acc_sanitized", 0) + 1
        if p.get("phone") is None:
            counters["phone_sanitized"] = counters.get("phone_sanitized", 0) + 1
        safe = _resolve_unique_conflicts(p, ms, counters)
        Actives.objects.update_or_create(msisdn=ms, defaults=safe)
        saved += 1
    return saved

def _django_bootstrap_in_child():
    if not os.environ.get("DJANGO_SETTINGS_MODULE"):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
    try:
        import django as _dj
        _dj.setup()
    except Exception:
        pass

def _worker_fetch_and_persist(
    ga_id: int,
    chunk_msisdns: List[str],
    payloads_for_chunk: Dict[str, dict],
    *,
    headless: bool,
    batch_save: int,
    log_file_path: Optional[str] = None,
    worker_suffix: str = "",
) -> dict:
    _django_bootstrap_in_child()

    from crm_api.models import GoogleAccount  # noqa
    from crm_api.services.filter_sbms import (
        build_driver, login, open_combined_form, perform_search_fast,
        read_name_and_status, switch_to_any_window_and_frame_with
    )
    from crm_api.logfilters import SafeWorkerFormatter, EnsureWorkerFilter  # noqa

    ga = GoogleAccount.objects.select_related("sbms_account").get(id=ga_id)
    tag = (worker_suffix or ga.label or f"ga-{ga.id}")

    fh = None
    if log_file_path:
        try:
            logger = logging.getLogger("crm")
            fh = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
            fmt = SafeWorkerFormatter("%(asctime)s | %(levelname)s | %(worker)s | %(message)s", "%H:%M:%S")
            fh.setFormatter(fmt)
            fh.addFilter(EnsureWorkerFilter())
            logger.addHandler(fh)
        except Exception:
            fh = None

    _log(tag, f"START: {len(chunk_msisdns)} msisdns, headless={headless}")

    sbms_user = (ga.sbms_account.username if ga.sbms_account else "") or LOGIN_USER_ENV_FALLBACK
    sbms_pass = (ga.sbms_account.password if ga.sbms_account else "") or LOGIN_PASS_ENV_FALLBACK
    if not (sbms_user and sbms_pass):
        _log(tag, "ERROR: no SBMS credentials", logging.ERROR)
        if fh:
            try:
                logging.getLogger("crm").removeHandler(fh); fh.close()
            except Exception:
                pass
        return {"ok": False, "error": "no sbms creds"}

    driver = build_driver(
        headless=headless,
        user_data_dir=(ga.user_data_dir or None),
        profile_directory=(ga.profile_directory or None),
        chrome_binary=(ga.chrome_binary or None),
    )

    saved_total = 0
    counters = {"acc_sanitized": 0, "phone_sanitized": 0, "account_conflict": 0, "phone_conflict": 0}
    batch_results: Dict[str, Dict[str, str]] = {}
    batch_order: List[str] = []

    try:
        login(driver, sbms_username=sbms_user, sbms_password=sbms_pass)
        open_combined_form(driver)

        total = len(chunk_msisdns)
        for idx, ms in enumerate(chunk_msisdns, 1):
            switch_to_any_window_and_frame_with(driver, "//*[contains(text(),'Комбинированная форма')]", window_timeout=2)
            if not perform_search_fast(driver, ms):
                _log(tag, f"[{idx}/{total}] {ms} -> not found/timeout")
                continue
            name, status = read_name_and_status(driver)
            batch_results[ms] = {"client": name, "status": status}
            batch_order.append(ms)
            _log(tag, f"[{idx}/{total}] {ms} -> {name} | {status}")

            if len(batch_order) >= batch_save:
                saved = _write_batch_to_db(batch_order, batch_results, payloads_for_chunk, counters)
                saved_total += saved
                _log(tag, f"FLUSH {saved} (total={saved_total})")
                for k in batch_order:
                    batch_results.pop(k, None)
                batch_order.clear()

        if batch_order:
            saved = _write_batch_to_db(batch_order, batch_results, payloads_for_chunk, counters)
            saved_total += saved
            _log(tag, f"FLUSH {saved} (total={saved_total})")
            batch_order.clear()
            batch_results.clear()

        _log(tag, f"DONE: saved_total={saved_total}")
        return {"ok": True, "saved": saved_total, "counters": counters}
    except Exception as e:
        _log(tag, f"EXCEPTION: {type(e).__name__}: {e}", logging.ERROR)
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if fh:
            try:
                logging.getLogger("crm").removeHandler(fh)
                fh.close()
            except Exception:
                pass


def import_split_parallel(now) -> Tuple[List[ExcelUpload], int, int, bool, dict]:
    headless = HEADLESS_DEFAULT

    try:
        info = ensure_chrome_profiles_dirs(BASE_CHROME_PROFILES_DIR, GA_FOLDERS_COUNT)
        _log("profiles", f"prepared: base={info['base_dir']} created={len(info['created'])} existed={len(info['existed'])}")
    except Exception as e:
        _log("profiles", f"prepare failed: {type(e).__name__}: {e}", logging.ERROR)

    now_local = timezone.localtime(now)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)

    with transaction.atomic():
        base_qs = (
            ExcelUpload.objects
            .select_for_update(skip_locked=True)
            .filter(
                uploaded_at__gte=start_local,
                uploaded_at__lt=end_local,
                status__in=[ExcelUpload.NEW, ExcelUpload.FAILED],
            )
            .order_by("uploaded_at")
        )
        candidates = [up for up in base_qs if not (_collapse(up.log))]

        for up in candidates:
            up.mark_processing("Picked by import_split_parallel")

    if not candidates:
        return [], 0, 0, False, {}

    ga_qs = GoogleAccount.objects.filter(is_active=True).select_related("sbms_account").order_by("id")
    if IMPORT_GA_LABELS:
        ga_qs = ga_qs.filter(label__in=IMPORT_GA_LABELS)
    ga_all = list(ga_qs)

    if not ga_all:
        for up in candidates:
            up.mark_processed(0, 0, False, "No active GoogleAccount; nothing processed.")
        return [], 0, 0, False, {}

    use_n = min(IMPORT_MAX_WORKERS, len(ga_all)) if IMPORT_MAX_WORKERS > 0 else len(ga_all)
    ga_list = ga_all[:use_n]

    processed: List[ExcelUpload] = []
    total_saved_all = 0

    for up in candidates:
        run_id = f"excel-{up.id}-{int(time.time())}"
        run_logger, run_handler, run_log_path = _attach_run_file_logger(run_id, suffix="main")

        log_lines: List[str] = []
        found = 0
        saved_sum = 0

        try:
            _log("main", f"PROCESS UPLOAD id={up.id} name={up.original_name!r} | GA_USED={len(ga_list)} / workers={use_n}")

            up.file.open("rb")
            content = up.file.read()
            up.file.close()

            payloads = read_excel_payloads(content, up.original_name or up.file.name, log_lines)
            found = len(payloads)
            if found == 0:
                _maybe_attach_logfile_to_upload(up, run_log_path)
                up.mark_processed(0, 0, False, "\n".join(log_lines or ["No rows with MSISDN."]))
                processed.append(up)
                continue

            ms = list(payloads.keys())
            k = len(ga_list)
            parts = split_even(ms, k)
            summary = ", ".join(f"{ga_list[i].label or 'GA'+str(ga_list[i].id)}={len(parts[i])}" for i in range(k))
            log_lines.append(f"Split across {k} GA: {summary}")
            _log("main", f"file={up.id} split: {summary}")

            max_workers = min(use_n, len(ga_list)) or 1
            results = []
            with ProcessPoolExecutor(max_workers=max_workers) as ex:
                futs = []
                for i, ga in enumerate(ga_list):
                    chunk = parts[i]
                    if not chunk:
                        continue
                    futs.append(
                        ex.submit(
                            _worker_fetch_and_persist,
                            ga.id,
                            chunk,
                            _subdict(payloads, chunk),
                            headless=headless,
                            batch_save=BATCH_SAVE,
                            log_file_path=run_log_path,
                            worker_suffix=ga.label or f"ga-{ga.id}",
                        )
                    )
                for fut in as_completed(futs):
                    results.append(fut.result())

            ok = True
            counters_total = {"acc_sanitized": 0, "phone_sanitized": 0, "account_conflict": 0, "phone_conflict": 0}
            for res in results:
                if not res.get("ok"):
                    ok = False
                    log_lines.append(f"Worker error: {res.get('error')}")
                    _log("main", f"Worker error: {res.get('error')}", logging.ERROR)
                else:
                    saved_sum += int(res.get("saved", 0))
                    c = res.get("counters", {}) or {}
                    for k_ in counters_total.keys():
                        counters_total[k_] += int(c.get(k_, 0))

            if any(counters_total.values()):
                log_lines.append(
                    f"Summary: normalized to NULL — account={counters_total['acc_sanitized']}, "
                    f"phone={counters_total['phone_sanitized']}; unique conflicts skipped — "
                    f"account={counters_total['account_conflict']}, phone={counters_total['phone_conflict']}"
                )

            missed = found - saved_sum
            if missed > 0:
                log_lines.append(f"Skipped (no SBMS result): {missed} of {found}")

            _maybe_attach_logfile_to_upload(up, run_log_path)

            up.mark_processed(saved_sum, found, ok, "\n".join(log_lines))
            total_saved_all += saved_sum
            processed.append(up)

        except Exception as e:
            _maybe_attach_logfile_to_upload(up, run_log_path)
            up.mark_processed(0, found, False, "\n".join(log_lines + [f"ERROR: {type(e).__name__}: {e}"]))
            processed.append(up)
        finally:
            _detach_file_logger(run_handler)

    started = total_saved_all > 0
    stats = {"files": len(processed), "rows_saved": total_saved_all}
    return processed, total_saved_all, len(processed), started, stats

def _maybe_attach_logfile_to_upload(up: ExcelUpload, run_log_path: str) -> None:
    try:
        if hasattr(up, "log_file") and run_log_path and os.path.exists(run_log_path):
            if not getattr(up, "log_file"):
                with open(run_log_path, "rb") as f:
                    up.log_file.save(os.path.basename(run_log_path), File(f), save=True)
        elif hasattr(up, "log_path"):
            if (not getattr(up, "log_path", "")) and run_log_path:
                up.log_path = run_log_path
                up.save(update_fields=["log_path"])
    except Exception:
        pass

def main():
    now = timezone.now()
    files, imported_rows, processed_files, started, stats = import_split_parallel(now)

    print(f"\nДата: {now.date()}")
    print(f"Обработано файлов: {processed_files}")
    print(f"Импортировано (после SBMS): {imported_rows}")
    print(f"Параллельная фильтрация SBMS запущена: {started}  (stats: {stats})\n")

    for f in files:
        print(
            f"- [{f.status}] {f.original_name or f.file.name} "
            f"({f.rows_saved}/{f.rows_found})  "
            f"uploaded_by={getattr(f.uploaded_by, 'username', '-')}, "
            f"uploaded_at={f.uploaded_at:%Y-%m-%d %H:%M}"
        )
        if f.log:
            print("  └─ log:")
            for line in f.log.splitlines():
                if line.strip():
                    print(f"     • {line}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            signal.raise_signal(getattr(signal, "SIGTERM", signal.SIGINT))
        except Exception:
            pass
def run():
    now = timezone.now()
    files, imported_rows, processed_files, started, stats = import_split_parallel(now)

    print(f"\nДата: {now.date()}")
    print(f"Обработано файлов: {processed_files}")
    print(f"Импортировано (после SBMS): {imported_rows}")
    print(f"Параллельная фильтрация SBMS запущена: {started}  (stats: {stats})\n")

    for f in files:
        print(
            f"- [{f.status}] {f.original_name or f.file.name} "
            f"({f.rows_saved}/{f.rows_found})  "
            f"uploaded_by={getattr(f.uploaded_by, 'username', '-')}, "
            f"uploaded_at={f.uploaded_at:%Y-%m-%d %H:%M}"
        )
        if f.log:
            print("  └─ log:")
            for line in f.log.splitlines():
                if line.strip():
                    print(f"     • {line}")

    return {
        "processed_files": processed_files,
        "rows_saved": imported_rows,
        "started": started,
        "stats": stats,
    }