import os
import socket
import time
import logging
import tempfile
from datetime import datetime
from typing import Callable, Dict, Iterable, Optional

from django.db import transaction
from django.utils import timezone

from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

from crm_api.models import Actives, SbmsAudit

try:
    from crm_api.logfilters import set_request_id, set_log_user
except Exception:
    def set_request_id(_id: Optional[str] = None):
        pass
    def set_log_user(_user):
        pass

_logger = logging.getLogger("crm")

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def _emit(tag: str, msg: str, *, level: int = logging.INFO, also: Optional[Callable[[str], None]] = None) -> None:
    line = f"{_ts()} | {tag:<12} | {msg}"
    print(line, flush=True)
    try:
        _logger.log(level, msg, extra={"worker": tag})
    except Exception:
        pass
    if also and also is not print:
        try:
            also(msg)
        except Exception:
            pass

def _dump_debug(driver, tag: str):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.join(tempfile.gettempdir(), f"sbms_{tag}_{ts}")
        png = base + ".png"
        html = base + ".html"
        try:
            driver.save_screenshot(png)
        except Exception:
            png = None
        try:
            with open(html, "w", encoding="utf-8") as f:
                f.write(driver.page_source or "")
        except Exception:
            html = None
        _emit(tag, f"saved debug: screenshot={png or '-'} page={html or '-'}")
    except Exception:
        pass

_env_urls = (os.getenv("SBMS_URLS") or "").strip()
if _env_urls:
    CANDIDATE_URLS = [u.strip() for u in _env_urls.replace(",", ";").split(";") if u.strip()]
else:
    CANDIDATE_URLS = ["https://sbms.uztelecom.uz/ps/sbms/shell.html"]

HEADLESS_DEFAULT = os.getenv("HEADLESS", "1").lower() in ("1", "true", "yes", "on")

ATTEMPT_TIMEOUT = int(os.getenv("SBMS_ATTEMPT_TIMEOUT", "7"))
POLL = 0.12
RETRIES = int(os.getenv("SBMS_RETRIES", "3"))
LOGIN_WAIT = int(os.getenv("SBMS_LOGIN_WAIT", "40"))
FORM_OPEN_WAIT = int(os.getenv("SBMS_FORM_WAIT", "30"))
LOGIN_RETRIES = int(os.getenv("SBMS_LOGIN_RETRIES", "4"))

def _ensure_dir(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    p = os.path.normpath(path)
    os.makedirs(p, exist_ok=True)
    return p

BASE_CHROME_PROFILES_DIR = os.getenv("CHROME_PROFILES_DIR", r"C:\ChromeProfiles")
GA_FOLDERS_COUNT = int(os.getenv("CHROME_PROFILES_COUNT", "12"))
_PROFILES_DIRS_ENSURED = False

def ensure_chrome_profiles_dirs(base_dir: Optional[str] = None, count: Optional[int] = None) -> dict:
    base_dir = os.path.normpath(base_dir or BASE_CHROME_PROFILES_DIR)
    count = int(count or GA_FOLDERS_COUNT)

    created = []
    existed = []
    base_created = False

    os.makedirs(base_dir, exist_ok=True)
    if not os.path.isdir(base_dir):
        raise RuntimeError(f"Cannot create base dir: {base_dir}")

    try:
        if not os.listdir(base_dir):
            base_created = True
    except Exception:
        pass

    for i in range(1, count + 1):
        p = os.path.join(base_dir, f"ga-{i}")
        if not os.path.isdir(p):
            os.makedirs(p, exist_ok=True)
            created.append(p)
        else:
            existed.append(p)

    msg = (f"ChromeProfiles prepared: base='{base_dir}', "
           f"base_created={base_created}, created={len(created)}, existed={len(existed)}")
    _emit("profiles", msg)

    try:
        log_path = os.path.join(base_dir, "create.log")
        with open(log_path, "a", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{ts}] {msg}\n")
            if created:
                for p in created:
                    f.write(f"  + {p}\n")
            if existed:
                for p in existed[:3]:
                    f.write(f"  = {p}\n")
                if len(existed) > 3:
                    f.write(f"  = ... (+{len(existed)-3} more)\n")
    except Exception:
        pass

    return {
        "base_dir": base_dir,
        "base_created": base_created,
        "created": created,
        "existed": existed,
    }

def build_driver(
    *,
    headless: bool,
    user_data_dir: Optional[str] = None,
    profile_directory: Optional[str] = None,
    chrome_binary: Optional[str] = None,
) -> webdriver.Chrome:
    global _PROFILES_DIRS_ENSURED
    if not _PROFILES_DIRS_ENSURED:
        try:
            ensure_chrome_profiles_dirs()
        finally:
            _PROFILES_DIRS_ENSURED = True

    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-features=EnablePasswordManager,TranslateUI,AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    udir = _ensure_dir(user_data_dir)
    if udir:
        opts.add_argument(f"--user-data-dir={udir}")
    if profile_directory:
        opts.add_argument(f"--profile-directory={profile_directory}")
    if chrome_binary:
        opts.binary_location = chrome_binary

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 1,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install(), log_path=os.devnull)
    try:
        driver = webdriver.Chrome(service=service, options=opts)
    except WebDriverException as e:
        if profile_directory:
            opts.arguments = [arg for arg in opts.arguments if not arg.startswith("--profile-directory=")]
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            raise e
    return driver

def W(driver, timeout=ATTEMPT_TIMEOUT):
    return WebDriverWait(driver, timeout, poll_frequency=POLL)

def switch_to_any_window_and_frame_with(driver, xp: str, window_timeout=8, max_depth=4) -> bool:
    deadline = time.time() + window_timeout
    seen = set()
    while time.time() < deadline:
        for h in driver.window_handles:
            if h in seen:
                continue
            driver.switch_to.window(h)
            driver.switch_to.default_content()
            if driver.find_elements(By.XPATH, xp):
                return True
            if _into_frame_holding(driver, xp, max_depth=max_depth):
                return True
            seen.add(h)
        time.sleep(0.15)
    return False

def _into_frame_holding(driver, xp: str, max_depth=4) -> bool:
    try:
        if driver.find_elements(By.XPATH, xp):
            return True
    except Exception:
        pass
    if max_depth <= 0:
        return False
    frames = driver.find_elements(By.TAG_NAME, "iframe") + driver.find_elements(By.TAG_NAME, "frame")
    for fr in frames:
        try:
            driver.switch_to.frame(fr)
            if _into_frame_holding(driver, xp, max_depth - 1):
                return True
            driver.switch_to.parent_frame()
        except Exception:
            driver.switch_to.default_content()
    return False

def _url_is_resolvable(url: str) -> bool:
    try:
        host = url.split("//", 1)[1].split("/", 1)[0]
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def _first_working_url() -> str | None:
    for url in CANDIDATE_URLS:
        if _url_is_resolvable(url):
            return url
    return None

def login(driver, *, sbms_username: str, sbms_password: str):
    url = _first_working_url()
    if not url:
        raise RuntimeError("SBMS URL cannot be resolved. Tried: " + ", ".join(CANDIDATE_URLS))
    for attempt in range(1, LOGIN_RETRIES + 1):
        driver.get(url)
        try:
            W(driver, LOGIN_WAIT).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'Клиенты')]")))
            return
        except Exception:
            pass
        try:
            u = W(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@type='text' or @name='username']")))
            p = W(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@type='password' or @name='password']")))
            u.clear(); u.send_keys(sbms_username)
            p.clear(); p.send_keys(sbms_password)
            driver.find_element(By.XPATH, "//button[contains(.,'Войти') or @type='submit']").click()
            W(driver, LOGIN_WAIT).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'Клиенты')]")))
            return
        except Exception as e:
            _emit("sbms-login", f"attempt {attempt}/{LOGIN_RETRIES} failed: {type(e).__name__}: {e}")
            _dump_debug(driver, f"login_fail_{attempt}")
            if attempt == LOGIN_RETRIES:
                raise
            time.sleep(1.2)

def open_combined_form(driver):
    for attempt in range(1, LOGIN_RETRIES + 1):
        try:
            W(driver, 12).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(.,'Клиенты')]"))).click()
            W(driver, 12).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(.,'Комбинированная форма')]"))).click()
            ok = switch_to_any_window_and_frame_with(driver, "//*[contains(text(),'Комбинированная форма')]", window_timeout=FORM_OPEN_WAIT)
            if not ok:
                raise TimeoutException("combined form not detected")
            return
        except Exception as e:
            _emit("sbms-form", f"open attempt {attempt}/{LOGIN_RETRIES} failed: {type(e).__name__}: {e}")
            _dump_debug(driver, f"form_fail_{attempt}")
            if attempt == LOGIN_RETRIES:
                raise
            try:
                driver.refresh()
            except Exception:
                pass
            time.sleep(1.0)

def get_search_input(driver):
    xp = "//input[@ng-model='base.search.searchTokenValue' and not(contains(@class,'ng-hide'))]"
    return W(driver).until(EC.element_to_be_clickable((By.XPATH, xp)))

def click_search(driver):
    btn_xp = "//button[.//ps-icon[@icon='search-white']]"
    try:
        W(driver, 2.0).until(EC.element_to_be_clickable((By.XPATH, btn_xp))).click()
    except Exception:
        driver.switch_to.active_element.send_keys(Keys.ENTER)

def click_clear(driver):
    try:
        driver.find_element(By.XPATH, "//button[span[normalize-space()='Очистить']]").click()
        time.sleep(0.15)
    except Exception:
        pass

def click_refresh(driver):
    try:
        driver.find_element(By.XPATH, "//button[span[normalize-space()='Обновить']]").click()
        time.sleep(0.20)
    except Exception:
        pass

def normalize_login(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isalnum()).lower()

def read_msisdn_on_form(driver) -> str:
    try:
        return driver.find_element(By.XPATH, "//*[@ng-bind='base.subscriber.MSISDN']").text.strip()
    except Exception:
        return ""

def wait_msisdn_equal(driver, target_login: str, timeout: float) -> bool:
    norm_target = normalize_login(target_login)
    def ok(_):
        txt = read_msisdn_on_form(driver)
        return txt and normalize_login(txt) == norm_target
    try:
        WebDriverWait(driver, timeout, poll_frequency=POLL).until(ok)
        return True
    except Exception:
        try:
            click_refresh(driver)
        except Exception:
            pass
        try:
            WebDriverWait(driver, timeout, poll_frequency=POLL).until(ok)
            return True
        except Exception:
            return False

def perform_search_fast(driver, acc: str) -> bool:
    for attempt in range(1, RETRIES + 1):
        if normalize_login(read_msisdn_on_form(driver)) == normalize_login(acc):
            return True
        inp = get_search_input(driver)
        inp.send_keys(Keys.CONTROL, "a"); inp.send_keys(Keys.DELETE); inp.send_keys(acc)
        click_search(driver)
        if wait_msisdn_equal(driver, acc, ATTEMPT_TIMEOUT):
            return True
        (click_refresh if attempt == 1 else click_clear)(driver)
    return False

def read_name_and_status(driver):
    name = ""
    try:
        name = driver.find_element(By.XPATH, "//*[@ng-bind='base.customer.nameOriginal']").text.strip()
    except Exception:
        try:
            name = driver.find_element(By.XPATH, "//*[contains(text(),'Имя клиента')]/following::td[1]//b").text.strip()
        except Exception:
            name = ""
    status = ""
    try:
        status = driver.find_element(
            By.XPATH,
            ("//div[contains(@class,'b-combined-view-sub-header__caption']"
             "[.//b[contains(.,'LC-статус')]]//span[contains(@ng-bind,'statusName')]")
        ).text.strip()
    except Exception:
        try:
            cap = driver.find_element(
                By.XPATH,
                "//div[contains(@class,'b-combined-view-sub-header__caption')][.//b[contains(.,'LC-статус')]]"
            ).text.strip()
            status = cap.replace("LC-статус", "").strip()
        except Exception:
            status = ""
    return name, status

def fetch_sbms_for_msisdns(
    msisdns: Iterable[str], *,
    headless: bool | None = None,
    user_data_dir: Optional[str] = None,
    profile_directory: Optional[str] = None,
    chrome_binary: Optional[str] = None,
    sbms_username: str,
    sbms_password: str,
    log: Callable[[str], None] = print,
) -> Dict[str, Dict[str, str]]:
    headless = HEADLESS_DEFAULT if headless is None else headless
    msisdns = [str(x).strip() for x in msisdns if str(x).strip()]
    res: Dict[str, Dict[str, str]] = {}
    if not msisdns:
        return res
    try:
        set_request_id(f"fetch:{int(time.time())}")
        set_log_user(None)
    except Exception:
        pass
    tag = (profile_directory or (user_data_dir or "") or "sbms").strip() or "sbms"
    _emit(tag, f"chrome headless={headless}; user_data_dir={user_data_dir or '-'}; profile={profile_directory or 'Default'}", also=log)
    driver = build_driver(
        headless=headless,
        user_data_dir=user_data_dir,
        profile_directory=profile_directory,
        chrome_binary=chrome_binary,
    )
    try:
        try:
            login(driver, sbms_username=sbms_username, sbms_password=sbms_password)
            open_combined_form(driver)
        except Exception as e:
            _emit(tag, f"login/open_form failed: {type(e).__name__}: {e}", level=logging.ERROR, also=log)
            _dump_debug(driver, "login_open_total_fail")
            raise
        total = len(msisdns)
        for idx, ms in enumerate(msisdns, 1):
            switch_to_any_window_and_frame_with(driver, "//*[contains(text(),'Комбинированная форма')]", window_timeout=2)
            if not perform_search_fast(driver, ms):
                _emit(tag, f"[{idx}/{total}] {ms} -> not found/timeout", also=log)
                continue
            name, status = read_name_and_status(driver)
            res[ms] = {"client": name, "status": status}
            _emit(tag, f"[{idx}/{total}] {ms} -> {name} | {status}", also=log)
    except Exception as e:
        _emit(tag, f"EXCEPTION: {type(e).__name__}: {e}", level=logging.ERROR, also=log)
        try:
            _logger.exception("fetch_sbms_for_msisdns failed")
        except Exception:
            pass
        raise
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    _emit(tag, f"done: fetched={len(res)}/{len(msisdns)}", also=log)
    return res

def run_sbms_sync(
    *, created_today: bool = True, headless: bool | None = None,
    log: Callable[[str], None] = print
) -> Dict[str, int]:
    headless = HEADLESS_DEFAULT if headless is None else headless
    qs = Actives.objects.all().only("id", "msisdn", "client", "status", "created_at")
    if created_today:
        qs = qs.filter(created_at__date=timezone.localdate())
    total = qs.count()
    tag = "sbms-sync"
    _emit(tag, f"records={total}, created_today={created_today}, headless={headless}", also=log)
    driver = build_driver(headless=headless)
    updated = 0
    try:
        sbms_user = os.getenv("SBMS_USER", "")
        sbms_pass = os.getenv("SBMS_PASS", "")
        try:
            login(driver, sbms_username=sbms_user, sbms_password=sbms_pass)
            open_combined_form(driver)
        except Exception as e:
            _emit(tag, f"login/open_form failed: {type(e).__name__}: {e}", level=logging.ERROR, also=log)
            _dump_debug(driver, "sync_login_open_fail")
            raise
        for idx, ab in enumerate(qs.iterator(chunk_size=500), start=1):
            msisdn = (ab.msisdn or "").strip()
            if not msisdn:
                SbmsAudit.objects.create(
                    abonent=ab, msisdn="", old_client=ab.client or "", new_client="",
                    old_status=ab.status or "", new_status="", ok=False, note="Пустой MSISDN"
                )
                continue
            switch_to_any_window_and_frame_with(driver, "//*[contains(text(),'Комбинированная форма')]", window_timeout=2)
            if not perform_search_fast(driver, msisdn):
                SbmsAudit.objects.create(
                    abonent=ab, msisdn=msisdn, old_client=ab.client or "", new_client="",
                    old_status=ab.status or "", new_status="", ok=False, note="Не найден/таймаут"
                )
                continue
            name, status = read_name_and_status(driver)
            note_parts, changed = [], False
            old_client, old_status = ab.client or "", ab.status or ""
            if status and status != old_status:
                ab.status = status; changed = True; note_parts.append(f"status: '{old_status}' -> '{status}'")
            if name and name != old_client:
                ab.client = name;  changed = True; note_parts.append(f"client: '{old_client}' -> '{name}'")
            with transaction.atomic():
                if changed:
                    ab.save(update_fields=["client", "status", "updated_at"])
                    updated += 1
                SbmsAudit.objects.create(
                    abonent=ab, msisdn=msisdn,
                    old_client=old_client, new_client=(ab.client or ""),
                    old_status=old_status, new_status=(ab.status or ""),
                    ok=True, note="Без изменений" if not note_parts else "; ".join(note_parts),
                )
            _emit(tag, f"[{idx}/{total}] {msisdn} -> {ab.client} | {ab.status} {'(UPDATED)' if changed else ''}", also=log)
    except Exception as e:
        _emit(tag, f"EXCEPTION: {type(e).__name__}: {e}", level=logging.ERROR, also=log)
        try:
            _logger.exception("run_sbms_sync failed")
        except Exception:
            pass
        raise
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    _emit(tag, f"done: checked={total}, updated={updated}", also=log)
    return {"checked": total, "updated": updated}
