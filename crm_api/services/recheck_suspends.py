import os
import time
import logging
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
import django
django.setup()

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.core.files.base import File

from crm_api.models import Actives, GoogleAccount, RecheckRun
try:
    from crm_api.models import Suspends
    SUSPENDS_MODEL = Suspends
except Exception:
    SUSPENDS_MODEL = Actives

from crm_api.services.filter_sbms import (
    build_driver, login, open_combined_form, perform_search_fast,
    read_name_and_status, switch_to_any_window_and_frame_with,
    HEADLESS_DEFAULT,
)

RECHECK_GA_LABELS = [s.strip() for s in os.getenv("RECHECK_GA_LABELS", "").split(",") if s.strip()]
RECHECK_MAX_WORKERS = int(os.getenv("RECHECK_MAX_WORKERS", "6") or "6")
RECHECK_BATCH_SAVE = int(os.getenv("RECHECK_BATCH_SAVE", "100"))
RECHECK_LIMIT = int(os.getenv("RECHECK_LIMIT", "0") or "0")  # 0 = без лимита
RECHECK_EPHEMERAL_PROFILE = os.getenv("RECHECK_EPHEMERAL_PROFILE", "1").lower() in ("1", "true", "yes")

ACTIVE_KEYWORDS = [s.strip().lower() for s in os.getenv("RECHECK_ACTIVE_KEYWORDS", "active,актив").split(",") if s.strip()]
SUSPEND_KEYWORDS = [s.strip().lower() for s in os.getenv("RECHECK_SUSPEND_KEYWORDS", "suspend,суспенд,приост,останов").split(",") if s.strip()]

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def _attach_run_file_logger(run_id: str, suffix: str = "recheck") -> tuple[logging.Logger, logging.Handler, str]:
    logs_dir = Path(getattr(settings, "RUNLOGS_DIR", Path(settings.BASE_DIR) / "runlogs"))
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"{run_id}-{suffix}.log"
    logger = logging.getLogger("crm")
    h = logging.FileHandler(path, mode="a", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(worker)s | %(message)s", "%H:%M:%S")
    h.setFormatter(fmt)
    logger.addHandler(h)
    return logger, h, str(path)

def _detach(handler: logging.Handler):
    try:
        logging.getLogger("crm").removeHandler(handler)
        handler.close()
    except Exception:
        pass

def _log(tag: str, msg: str, level=logging.INFO):
    line = f"{_ts()} | {tag:<10} | pid={os.getpid()} | {msg}"
    print(line, flush=True)
    try:
        logging.getLogger("crm").log(level, msg, extra={"worker": tag})
    except Exception:
        pass

def _is_active_status(s: str) -> bool:
    ls = (s or "").lower()
    return any(k in ls for k in ACTIVE_KEYWORDS)

def _looks_like_suspend(s: str) -> bool:
    ls = (s or "").lower()
    return any(k in ls for k in SUSPEND_KEYWORDS)

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

def _bootstrap_child():
    if not os.environ.get("DJANGO_SETTINGS_MODULE"):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
    try:
        import django as _dj
        _dj.setup()
    except Exception:
        pass

def _worker_recheck(
    ga_id: int,
    msisdns: List[str],
    *,
    headless: bool,
    batch_save: int,
    ephemeral_profile: bool,
    run_log_path: Optional[str],
    label: str,
) -> dict:
    _bootstrap_child()
    from crm_api.models import GoogleAccount, Actives  # noqa
    import tempfile as _tf
    import shutil as _sh

    ga = GoogleAccount.objects.select_related("sbms_account").get(id=ga_id)
    tag = label or ga.label or f"ga-{ga.id}"

    fh = None
    if run_log_path:
        try:
            logger = logging.getLogger("crm")
            fh = logging.FileHandler(run_log_path, mode="a", encoding="utf-8")
            fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(worker)s | %(message)s", "%H:%M:%S")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            fh = None

    tmp_dir = None
    user_data_dir = ga.user_data_dir or None
    profile_directory = ga.profile_directory or None
    if ephemeral_profile or not user_data_dir:
        tmp_dir = _tf.mkdtemp(prefix=f"recheck-{ga.id}-")
        user_data_dir = tmp_dir
        profile_directory = None

    _log(tag, f"START recheck: {len(msisdns)} headless={headless} ephemeral={bool(tmp_dir)}")

    sbms_user = (ga.sbms_account.username if ga.sbms_account else "") or os.getenv("SBMS_USER", "")
    sbms_pass = (ga.sbms_account.password if ga.sbms_account else "") or os.getenv("SBMS_PASS", "")
    if not (sbms_user and sbms_pass):
        _log(tag, "ERROR: no SBMS creds", logging.ERROR)
        if fh:
            logging.getLogger("crm").removeHandler(fh); fh.close()
        if tmp_dir:
            _sh.rmtree(tmp_dir, ignore_errors=True)
        return {"ok": False, "error": "no sbms creds"}

    driver = build_driver(
        headless=headless,
        user_data_dir=user_data_dir,
        profile_directory=profile_directory,
        chrome_binary=(ga.chrome_binary or None),
    )

    checked = 0
    activated = 0
    still_suspend = 0
    updated = 0

    activated_items: List[Tuple[str, str, str]] = []
    try:
        login(driver, sbms_username=sbms_user, sbms_password=sbms_pass)
        open_combined_form(driver)

        total = len(msisdns)
        for idx, ms in enumerate(msisdns, 1):
            switch_to_any_window_and_frame_with(driver, "//*[contains(text(),'Комбинированная форма')]", window_timeout=2)
            if not perform_search_fast(driver, ms):
                _log(tag, f"[{idx}/{total}] {ms} -> not found/timeout")
                checked += 1
                continue

            name, status = read_name_and_status(driver)
            _log(tag, f"[{idx}/{total}] {ms} -> {name} | {status}")
            is_active = _is_active_status(status)

            try:
                with transaction.atomic():
                    defaults = {}
                    if name: defaults["client"] = name
                    if status: defaults["status"] = status
                    Actives.objects.update_or_create(msisdn=ms, defaults=defaults)

                    if is_active and SUSPENDS_MODEL is not Actives:
                        SUSPENDS_MODEL.objects.filter(msisdn=ms).delete()
                updated += 1
            except Exception as e:
                _log(tag, f"DB update failed for {ms}: {type(e).__name__}: {e}", logging.ERROR)

            if is_active:
                activated += 1
                activated_items.append((ms, name or "", status or ""))
            else:
                still_suspend += 1

            checked += 1

        _log(tag, f"DONE: checked={checked}, activated={activated}, still_suspend={still_suspend}, updated={updated}")
        return {
            "ok": True,
            "checked": checked,
            "activated": activated,
            "still_suspend": still_suspend,
            "updated": updated,
            "activated_items": activated_items,
        }

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
                logging.getLogger("crm").removeHandler(fh); fh.close()
            except Exception:
                pass
        if tmp_dir:
            _sh.rmtree(tmp_dir, ignore_errors=True)

def _write_activated_file(run_id: str, rows: List[Tuple[str, str, str]]) -> str:
    logs_dir = Path(getattr(settings, "RUNLOGS_DIR", Path(settings.BASE_DIR) / "runlogs"))
    logs_dir.mkdir(parents=True, exist_ok=True)
    path = logs_dir / f"{run_id}-activated.txt"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("MSISDN\tCLIENT\tSTATUS\n")
            for ms, name, st in rows:
                safe_name = (name or "").replace("\t", " ").strip()
                safe_st = (st or "").replace("\t", " ").strip()
                f.write(f"{ms}\t{safe_name}\t{safe_st}\n")
    except Exception:
        pass
    return str(path)

def recheck_suspends(today) -> Tuple[dict, int]:
    headless = HEADLESS_DEFAULT

    if SUSPENDS_MODEL is Actives:
        base = Actives.objects.filter(status__icontains="suspend")
    else:
        base = SUSPENDS_MODEL.objects.all()

    ids = list(base.values_list("msisdn", flat=True)[:RECHECK_LIMIT or None])
    if not ids:
        return {"checked": 0, "activated": 0, "still_suspend": 0, "updated": 0, "errors": 0, "activated_items": []}, 0

    ga_qs = GoogleAccount.objects.filter(is_active=True).select_related("sbms_account").order_by("id")
    if RECHECK_GA_LABELS:
        ga_qs = ga_qs.filter(label__in=RECHECK_GA_LABELS)
    ga_all = list(ga_qs)
    if not ga_all:
        return {"checked": 0, "activated": 0, "still_suspend": 0, "updated": 0, "errors": 0, "activated_items": []}, 0

    if RECHECK_GA_LABELS:
        ga_list = ga_all[:max(1, min(RECHECK_MAX_WORKERS, len(ga_all)))]
    else:
        if len(ga_all) < 7:
            return {"checked": 0, "activated": 0, "still_suspend": 0, "updated": 0, "errors": 0, "activated_items": []}, 0
        start = 6
        end = min(len(ga_all), start + max(1, min(RECHECK_MAX_WORKERS, len(ga_all) - start)))
        ga_list = ga_all[start:end]

    if not ga_list:
        return {"checked": 0, "activated": 0, "still_suspend": 0, "updated": 0, "errors": 0, "activated_items": []}, 0

    parts = split_even(ids, len(ga_list))

    run_id = f"recheck-{int(time.time())}"
    now = timezone.now()
    _, h, run_log_path = _attach_run_file_logger(run_id, suffix="recheck")

    run = RecheckRun.objects.create(
        run_id=run_id,
        started_at=now,
        checked=0,
        activated=0,
        still_suspend=0,
        updated=0,
        errors=0,
        log_path=run_log_path,
        activated_path="",
    )

    max_workers = max(1, min(RECHECK_MAX_WORKERS, len(ga_list)))

    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = []
        for i, ga in enumerate(ga_list):
            chunk = parts[i]
            if not chunk:
                continue
            futs.append(
                ex.submit(
                    _worker_recheck,
                    ga.id,
                    chunk,
                    headless=headless,
                    batch_save=RECHECK_BATCH_SAVE,
                    ephemeral_profile=RECHECK_EPHEMERAL_PROFILE,
                    run_log_path=run_log_path,
                    label=ga.label or f"ga-{ga.id}",
                )
            )
        for f in as_completed(futs):
            results.append(f.result())

    stats = {"checked": 0, "activated": 0, "still_suspend": 0, "updated": 0, "errors": 0}
    activated_items_all: List[Tuple[str, str, str]] = []

    for r in results:
        if not r.get("ok"):
            stats["errors"] += 1
        else:
            stats["checked"] += int(r.get("checked", 0))
            stats["activated"] += int(r.get("activated", 0))
            stats["still_suspend"] += int(r.get("still_suspend", 0))
            stats["updated"] += int(r.get("updated", 0))
            activated_items_all.extend(r.get("activated_items", []) or [])

    activated_file_path = _write_activated_file(run_id, activated_items_all)

    run.finished_at = timezone.now()
    run.checked = stats["checked"]
    run.activated = stats["activated"]
    run.still_suspend = stats["still_suspend"]
    run.updated = stats["updated"]
    run.errors = stats["errors"]
    run.activated_path = activated_file_path

    try:
        if run_log_path and os.path.exists(run_log_path):
            with open(run_log_path, "rb") as fh:
                run.log_file.save(os.path.basename(run_log_path), File(fh), save=False)
    except Exception:
        pass

    run.save()

    _detach(h)

    stats["activated_items"] = activated_items_all
    return stats, len(results)

def main():
    now = timezone.now()
    stats, workers = recheck_suspends(now)

    print(f"\nRECHECK at {now:%Y-%m-%d %H:%M}")
    print(f"Workers: {workers}")
    print(f"Checked: {stats['checked']}")
    print(f"Activated (status ACTIVE on SBMS): {stats['activated']}")
    print(f"Still suspend: {stats['still_suspend']}")
    print(f"Updated rows: {stats['updated']}")
    print(f"Worker errors: {stats['errors']}")

    activated_items = stats.get("activated_items", []) or []
    if activated_items:
        print("\nActive subscribers (MSISDN | CLIENT | STATUS):")
        for ms, name, st in activated_items:
            print(f" - {ms} | {name} | {st}")
    else:
        print("\nActive subscribers: none")

    logs_dir = Path(getattr(settings, "RUNLOGS_DIR", Path(settings.BASE_DIR) / "runlogs"))
    latest = sorted(logs_dir.glob("recheck-*-recheck.log"))[-1:] if logs_dir.exists() else []
    latest_activated = sorted(logs_dir.glob("recheck-*-activated.txt"))[-1:] if logs_dir.exists() else []
    if latest:
        print(f"\nRun log: {latest[0]}")
    if latest_activated:
        print(f"Activated list: {latest_activated[0]}")
    print()

if __name__ == "__main__":
    main()


def run():
    now = timezone.now()
    stats, workers = recheck_suspends(now)

    print(f"\nRECHECK at {now:%Y-%m-%d %H:%M}")
    print(f"Workers: {workers}")
    print(f"Checked: {stats['checked']}")
    print(f"Activated (status ACTIVE on SBMS): {stats['activated']}")
    print(f"Still suspend: {stats['still_suspend']}")
    print(f"Updated rows: {stats['updated']}")
    print(f"Worker errors: {stats['errors']}")

    activated_items = stats.get("activated_items", []) or []
    if activated_items:
        print("\nActive subscribers (MSISDN | CLIENT | STATUS):")
        for ms, name, st in activated_items:
            print(f" - {ms} | {name} | {st}")
    else:
        print("\nActive subscribers: none")

    return {"workers": workers, **stats}