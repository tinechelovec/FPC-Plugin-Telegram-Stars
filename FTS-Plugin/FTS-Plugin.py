import os
import json
import logging
import requests
import re as _re
import time
import random
import shutil
import threading
import html as _html, base64 as _b64
from concurrent.futures import ThreadPoolExecutor
from telebot.types import InlineKeyboardMarkup as K, InlineKeyboardButton as B
from telebot.apihelper import ApiTelegramException
from collections import defaultdict
import tg_bot.CBT as CBT
logger = logging.getLogger('FTS-Plugin')
LOG_TAG = '[FTS-Plugin]'
_HTTP = requests.Session()
class _AnsiColorFormatter(logging.Formatter):
    COLORS = {logging.DEBUG: '\x1b[90m', logging.INFO: '\x1b[36m', logging.WARNING: '\x1b[33m', logging.ERROR: '\x1b[31m', logging.CRITICAL: '\x1b[41m'}
    RESET = '\x1b[0m'
    def format(self, record):
        base = super().format(record)
        color = self.COLORS.get(record.levelno, '')
        reset = self.RESET if color else ''
        return f'{color}{base}{reset}'
HUMAN_LOGS = bool(int(os.getenv('FTS_HUMAN_LOGS', '1')))
HUMAN_DEDUP = bool(int(os.getenv('FTS_HUMAN_DEDUP', '1')))
_USERNAME_CHECK_GAP = float(os.getenv('FTS_USERNAME_CHECK_GAP', '0.8'))
_USERNAME_CHECK_JITTER = float(os.getenv('FTS_USERNAME_CHECK_JITTER', '0.4'))
_last_username_check_ts = {}
LITESERVER_RETRY_DEFAULT = bool(int(os.getenv('FTS_Plugin_RETRY_LITESERVER', '1')))
LITESERVER_RETRY_SLEEP_MIN = float(os.getenv('FTS_Plugin_RETRY_LITESERVER_SLEEP_MIN', '0.8'))
LITESERVER_RETRY_SLEEP_MAX = float(os.getenv('FTS_Plugin_RETRY_LITESERVER_SLEEP_MAX', '1.8'))
QUEUE_TIMEOUT_DEFAULT = int(os.getenv('FTS_QUEUE_TIMEOUT_SEC', '300'))
ORDER_WATCH_INTERVAL_DEFAULT = int(os.getenv('FTS_ORDER_WATCH_INTERVAL_SEC', '300'))
ORDER_WAIT_REMINDER_DEFAULT = int(os.getenv('FTS_ORDER_WAIT_REMINDER_SEC', '900'))
ORDER_REVIEW_REMINDER_DEFAULT = int(os.getenv('FTS_ORDER_REVIEW_REMINDER_SEC', '1800'))
ORDER_RECORDS_LIMIT = int(os.getenv('FTS_ORDER_RECORDS_LIMIT', '300'))
class _Ansi:
    R = '\x1b[31m'
    Y = '\x1b[33m'
    C = '\x1b[36m'
    G = '\x1b[32m'
    DIM = '\x1b[90m'
    BOLD = '\x1b[1m'
    RESET = '\x1b[0m'
class _HumanLog:
    import re as _re
    SEEN_AUTOREPLY_BY_OID = set()
    RULES = [(_re.compile('ORDER EVENT action=config_migrated.*', _re.I), lambda m: ('Конфиг обновлён до новой схемы — старые настройки сохранены.', '')), (_re.compile('ORDER EVENT action=new_order.*?oid=([^\\s]+).*?qty=([^\\s]+)', _re.I), lambda m: (f'Новый заказ #{m.group(1)} найден, количество: {m.group(2)}⭐.', m.group(1))), (_re.compile('ORDER EVENT action=paid_message.*?oid=([^\\s]+).*?qty=([^\\s]+)', _re.I), lambda m: (f'Получено системное сообщение об оплате заказа #{m.group(1)} на {m.group(2)}⭐.', m.group(1))), (_re.compile('ORDER EVENT action=username_received.*?oid=([^\\s]+).*?username=@?([A-Za-z0-9_]{5,32})', _re.I), lambda m: (f'Получен ник @{m.group(2)} для заказа #{m.group(1)}.', m.group(1))), (_re.compile('ORDER EVENT action=confirm_start.*?oid=([^\\s]+).*?qty=([^\\s]+).*?username=@?([A-Za-z0-9_]{5,32})', _re.I), lambda m: (f'Подтверждение заказа #{m.group(1)}: {m.group(2)}⭐ на @{m.group(3)}.', m.group(1))), (_re.compile('ORDER EVENT action=send_ok.*?oid=([^\\s]+).*?qty=([^\\s]+).*?username=@?([A-Za-z0-9_]{5,32})', _re.I), lambda m: (f'Заказ #{m.group(1)} выполнен: {m.group(2)}⭐ отправлены @{m.group(3)}.', m.group(1))), (_re.compile('ORDER EVENT action=send_fail.*?oid=([^\\s]+).*?username=@?([A-Za-z0-9_]{5,32})', _re.I), lambda m: (f'Ошибка отправки по заказу #{m.group(1)} для @{m.group(2)}.', m.group(1))), (_re.compile('\\[IGNORE\\]\\s*auto-reply skipped.*?(?:OID:([A-Z0-9\\-]+))?', _re.I), lambda m: ('Автоответ найден — пропустили сообщение.', m.group(1) or '')), (_re.compile('\\[IGNORE\\]\\s*gift/account-login system note', _re.I), lambda m: ('Системное примечание с «подарком»/«заходом на аккаунт» — игнорируем.', '')), (_re.compile('\\[QUEUE\\]\\s*merged\\s+(.+?)\\s*->\\s*([^\\s|]+)', _re.I), lambda m: (f'Объединили очереди: {m.group(1)} → {m.group(2)}', '')), (_re.compile('ORDER\\s+#([A-Z0-9\\-]+):\\s*queued,.*', _re.I), lambda m: (f'Заказ #{m.group(1)} добавлен в очередь — ждём @username или системное «заказ оплачен».', m.group(1))), (_re.compile('SEND start:\\s*(\\d+)\\s*⭐\\s*→\\s*@?([A-Za-z0-9_]{5,32})', _re.I), lambda m: (f'Начали отправку: {m.group(1)}⭐ на @{m.group(2)}', '')), (_re.compile('SEND result:\\s*ok=(True|False).*?status=(\\d+)', _re.I), lambda m: (f"Отправка завершена — {('успех' if m.group(1) == 'True' else 'ошибка')}, HTTP {m.group(2)}.", '')), (_re.compile('SEND exception:\\s*(.+)', _re.I), lambda m: (f'Ошибка при отправке: {m.group(1)}', '')), (_re.compile('ORDER FAIL\\s+#([A-Z0-9\\-]+)\\s+(\\d+)\\s*⭐\\s*@([A-Za-z0-9_]{5,32}):\\s*(.+?)\\s*\\|\\s*status=(\\d+)', _re.I), lambda m: (f'Не удалось выполнить заказ #{m.group(1)}: {m.group(4)} (HTTP {m.group(5)}). Кол-во: {m.group(2)}⭐, ник @{m.group(3)}.', m.group(1))), (_re.compile('\\[AUTODEACT\\].*?Баланс\\s+([0-9.]+)\\s*<\\s*([0-9.]+).*?категории\\s+(\\d+)', _re.I), lambda m: (f'Лоты категории {m.group(3)} отключены: баланс {m.group(1)} TON ниже порога {m.group(2)} TON.', '')), (_re.compile('MIN BALANCE set to\\s*([0-9.]+)\\s*TON', _re.I), lambda m: (f'Порог баланса обновлён: {m.group(1)} TON.', '')), (_re.compile('\\[PREORDER\\]\\s*Захватили ник\\s*@([A-Za-z0-9_]{5,32}).*?#([A-Z0-9\\-]+)', _re.I), lambda m: (f'Ник из заказа захвачен: @{m.group(1)} для #{m.group(2)} — ждём оплату.', m.group(2)))]
    @classmethod
    def _fmt_like_classic(cls, record, text, color_code):
        ts = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(record.created))
        lvl_letter = {logging.INFO: 'I', logging.WARNING: 'W', logging.ERROR: 'E', logging.DEBUG: 'D', logging.CRITICAL: 'C'}.get(record.levelno, 'I')
        return f'{color_code}[{ts}]> {_Ansi.BOLD}{lvl_letter}{_Ansi.RESET}{color_code}: {LOG_TAG} {text}{_Ansi.RESET}'
    @classmethod
    def humanize(cls, record):
        raw = record.getMessage() or ''
        oid_for_dedup = ''
        text = raw
        for pat, fn in cls.RULES:
            m = pat.search(raw)
            if m:
                try:
                    text, oid_for_dedup = fn(m)
                except Exception:
                    pass
                break
        text = text.replace('[FTS-Plugin]', '').strip()
        color = _Ansi.C
        if record.levelno >= logging.ERROR:
            color = _Ansi.R
        elif record.levelno == logging.WARNING:
            color = _Ansi.Y
        elif record.levelno == logging.DEBUG:
            color = _Ansi.DIM
        if HUMAN_DEDUP and text.startswith('Автоответ найден') and oid_for_dedup:
            key = f'auto:{oid_for_dedup}'
            if key in cls.SEEN_AUTOREPLY_BY_OID:
                return ('', True)
            cls.SEEN_AUTOREPLY_BY_OID.add(key)
        return (cls._fmt_like_classic(record, text, color), False)
class _HumanFilter(logging.Filter):
    def filter(self, record):
        msg, suppressed = _HumanLog.humanize(record)
        if suppressed:
            return False
        record._humanized = msg
        return True
class _HumanFormatter(logging.Formatter):
    def format(self, record):
        msg = getattr(record, '_humanized', None)
        if msg is None:
            msg, _ = _HumanLog.humanize(record)
        return msg
def _h(x):
    return _html.escape(str(x), quote=False)
def _setup_logging():
    for h in list(logger.handlers):
        logger.removeHandler(h)
    if HUMAN_LOGS:
        h = logging.StreamHandler()
        h.setFormatter(_HumanFormatter())
        logger.addHandler(h)
        logger.addFilter(_HumanFilter())
    else:
        h = logging.StreamHandler()
        h.setFormatter(_AnsiColorFormatter('%(asctime)s %(levelname)s ' + LOG_TAG + ' %(message)s'))
        logger.addHandler(h)
    raw_path = os.getenv('FTS_RAW_LOG_FILE')
    if raw_path:
        f = logging.FileHandler(raw_path, encoding='utf-8')
        f.setFormatter(_AnsiColorFormatter('%(asctime)s %(levelname)s ' + LOG_TAG + ' %(message)s'))
        logger.addHandler(f)
_setup_logging()
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(_AnsiColorFormatter('%(asctime)s %(levelname)s ' + LOG_TAG + ' %(message)s'))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
_STATE_LOCK = threading.RLock()
_SEND_EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv('FTS_SEND_WORKERS', '2')), thread_name_prefix='FTS-SEND')
_ACTIVE_JOBS = {}
_ACTIVE_JOBS_LOCK = threading.Lock()
def _schedule_job(key, fn, *args, **kwargs):
    with _ACTIVE_JOBS_LOCK:
        old = _ACTIVE_JOBS.get(key)
        if old is not None and (not old.done()):
            return False
        fut = _SEND_EXECUTOR.submit(fn, *args, **kwargs)
        _ACTIVE_JOBS[key] = fut
        def _cleanup(_f):
            with _ACTIVE_JOBS_LOCK:
                cur = _ACTIVE_JOBS.get(key)
                if cur is _f:
                    _ACTIVE_JOBS.pop(key, None)
        fut.add_done_callback(_cleanup)
        return True
def _schedule_confirm_send(cardinal, chat_id, oid=None, *, notify_busy=True):
    job_key = f"send:{chat_id}:{oid or 'cur'}"
    def _run():
        try:
            if oid:
                _do_confirm_send_for_oid(cardinal, chat_id, oid)
            else:
                _do_confirm_send(cardinal, chat_id)
        except Exception as e:
            logger.exception(f'background send failed: {e}')
    return _schedule_job(job_key, _run)
def _log(level, msg):
    if level == 'info':
        logger.info(f'{msg}')
    elif level == 'warn':
        logger.warning(f'{msg}')
    elif level == 'error':
        logger.error(f'{msg}')
    else:
        logger.debug(f'{msg}')
def _short_log_value(v, limit=140):
    try:
        s = str(v)
    except Exception:
        s = repr(v)
    s = s.replace('\n', ' ').replace('\r', ' ').strip()
    if len(s) > limit:
        return s[:limit - 1] + '…'
    return s
def _order_log(level, action, oid=None, chat_id=None, qty=None, username=None, **extra):
    parts = [f'ORDER EVENT action={action}']
    if oid is not None:
        parts.append(f'oid={_short_log_value(oid, 64)}')
    if chat_id is not None:
        parts.append(f'chat_id={_short_log_value(chat_id, 64)}')
    if qty is not None:
        parts.append(f'qty={_short_log_value(qty, 32)}')
    if username:
        parts.append(f"username=@{_short_log_value(str(username).lstrip('@'), 40)}")
    for k, v in extra.items():
        if v is None:
            continue
        parts.append(f'{k}={_short_log_value(v)}')
    _log(level, ' '.join(parts))
def _s64(x):
    return _b64.b64decode(x.encode()).decode('utf-8')
NAME = 'FTS-Plugin'
VERSION = '1.7.2'
DESCRIPTION = 'Плагин по продаже звезд.'
CREDITS = _s64('QHRpbmVjaGVsb3ZlYw==')
UUID = _s64('ZmEwYzJmM2EtN2E4NS00YzA5LWEzYjItOWYzYTliOGY4YTc1')
SETTINGS_PAGE = False
CREATOR_URL = _s64('aHR0cHM6Ly90Lm1lL3RpbmVjaGVsb3ZlYw==')
GROUP_URL = _s64('aHR0cHM6Ly90Lm1lL2Rldl90aGNfY2hhdA==')
CHANNEL_URL = _s64('aHR0cHM6Ly90Lm1lL2J5X3RoYw==')
GITHUB_URL = _s64('aHR0cHM6Ly9naXRodWIuY29tL3RpbmVjaGVsb3ZlYy9GUEMtUGx1Z2luLVRlbGVncmFtLVN0YXJz')
GITHUB_UPDATE_URL = os.getenv('FTS_PLUGIN_UPDATE_URL', 'https://raw.githubusercontent.com/tinechelovec/FPC-Plugin-Telegram-Stars/main/FTS-Plugin/FTS-Plugin.py').strip()
INSTRUCTION_URL = _s64('aHR0cHM6Ly90ZWxldHlwZS5pbi9AdGluZWNoZWxvdmVjL0ZUUy1QbHVnaW4=')
FRAGMENT_BASE = os.getenv('FRAGMENT_BASE', 'https://api.fragment-api.com/v1')
FRAGMENT_WALLET_URL = os.getenv('FRAGMENT_WALLET_URL', f'{FRAGMENT_BASE}/misc/wallet/')
FRAGMENT_WALLET_URLS = [FRAGMENT_WALLET_URL, f'{FRAGMENT_BASE}/wallet/balance/', f'{FRAGMENT_BASE}/misc/wallet/balance/']
FRAGMENT_PRICES_URLS = [os.getenv('FRAGMENT_PRICES_URL', f'{FRAGMENT_BASE}/misc/prices/'), f'{FRAGMENT_BASE}/prices/', f'{FRAGMENT_BASE}/order/prices/']
TONAPI_RATES_URL = os.getenv('FTS_TONAPI_RATES_URL', 'https://tonapi.io/v2/rates')
TONAPI_KEY = os.getenv('FTS_TONAPI_KEY', '').strip()
TONAPI_TON_TOKEN = os.getenv('FTS_TONAPI_TON_TOKEN', 'ton')
TONAPI_USDT_TOKEN = os.getenv('FTS_TONAPI_USDT_TOKEN', 'usdt')
FRAGMENT_USER_URLS = [os.getenv('FNP_FRAGMENT_USER_URL', f'{FRAGMENT_BASE}/misc/user/user/'), f'{FRAGMENT_BASE}/misc/user/']
FRAGMENT_ORDER_STARS = os.getenv('FRAGMENT_ORDER_STARS', f'{FRAGMENT_BASE}/order/stars/')
FNP_STARS_CATEGORY_ID = int(os.getenv('FTS_Plugin_CATEGORY_ID', '2418'))
FNP_MIN_BALANCE_TON = float(os.getenv('FTS_Plugin_MIN_BALANCE_TON', '5.0'))
FNP_MIN_BALANCE_USDT = float(os.getenv('FTS_Plugin_MIN_BALANCE_USDT', '5.0'))
FTS_AUTO_PRICE_INTERVAL_SEC = int(os.getenv('FTS_AUTO_PRICE_INTERVAL_SEC', '1800'))
FTS_AUTODUMP_DEFAULT_INTERVAL_SEC = int(os.getenv('FTS_AUTODUMP_INTERVAL_SEC', '1800'))
FTS_AUTODUMP_STEP_RUB = float(os.getenv('FTS_AUTODUMP_STEP_RUB', '1'))
FTS_AUTODUMP_RAISE_STEP_RUB = float(os.getenv('FTS_AUTODUMP_RAISE_STEP_RUB', '1'))
FTS_BALANCE_LOT_RESERVE_RATIO = float(os.getenv('FTS_BALANCE_LOT_RESERVE_RATIO', '1.0'))
FTS_MIN_STARS = int(os.getenv('FTS_MIN_STARS', '50'))
FTS_CURRENCY_TON = 'ton'
FTS_CURRENCY_USDT_TON = 'usdt_ton'
FTS_SUPPORTED_CURRENCIES = {FTS_CURRENCY_TON, FTS_CURRENCY_USDT_TON}
FTS_DEFAULT_CURRENCY = os.getenv('FTS_DEFAULT_CURRENCY', FTS_CURRENCY_TON).strip().lower()
if FTS_DEFAULT_CURRENCY not in FTS_SUPPORTED_CURRENCIES:
    FTS_DEFAULT_CURRENCY = FTS_CURRENCY_TON
PLUGIN_FOLDER = 'storage/plugins/FTS-Plugin'
SETTINGS_FILE = os.path.join(PLUGIN_FOLDER, 'settings.json')
SETTINGS_BAK = SETTINGS_FILE + '.bak'
SETTINGS_SCHEMA_VERSION = 4
LEGACY_SETTINGS_KEY = '__legacy__'
SETTINGS_META_KEY = '__meta__'
_CFG_KNOWN_KEYS = {'plugin_enabled', 'lots_active', 'auto_refund', 'auto_deactivate', 'preorder_username', 'unit_star_price', 'markup_percent', 'fragment_jwt', 'wallet_version', 'balance_ton', 'balance_usdt', 'last_wallet_raw', 'templates', 'category_id', 'min_balance_ton', 'min_balance_usdt', 'star_lots', 'retry_liteserver', 'auto_send_without_plus', 'skip_username_check', 'queue_mode', 'queue_timeout_sec', 'stars_currency', 'usdt_fallback_to_ton', 'price_change_notifications', 'auto_price_fragment_enabled', 'autodump_enabled', 'autodump_interval_sec', 'autodump_notifications', 'balance_lot_filter_enabled', 'balance_lot_filter_notifications', 'last_auto_deact_reason', 'managed_lot_ids', 'last_lot_toggle_report', 'last_lot_toggle_ts', 'order_watch_enabled', 'order_watch_interval_sec', 'order_wait_reminder_sec', 'order_review_reminder_enabled', 'order_review_reminder_sec', 'order_records', 'last_order_watch_ts', 'last_usdt_fallback_reason', 'last_usdt_fallback_ts', 'last_auto_price_base_unit', 'last_auto_price_ts', 'autodump_last_ts', 'config_version'}
_CFG_TOKEN_ALIASES = ('jwt', 'token', 'fragment_token', 'fragmentApiToken', 'fragment_api_token')
_SETTINGS_IO_LOCK = threading.RLock()
def _atomic_write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
def _try_parse_settings_text(txt):
    if txt is None:
        return None
    s = txt.lstrip('\ufeff').strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except json.JSONDecodeError:
        try:
            dec = json.JSONDecoder()
            obj, idx = dec.raw_decode(s)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    except Exception:
        return None
    return None
def _load_settings():
    with _SETTINGS_IO_LOCK:
        loaded_from = None
        for path, label in ((SETTINGS_FILE, 'settings.json'), (SETTINGS_BAK, 'settings.json.bak')):
            try:
                if not os.path.exists(path):
                    continue
                with open(path, 'r', encoding='utf-8') as f:
                    txt = f.read()
                obj = _try_parse_settings_text(txt)
                if isinstance(obj, dict):
                    data, changed, notes = _migrate_settings_data(obj)
                    if label != 'settings.json':
                        logger.warning('Settings restored from .bak')
                        changed = True
                    if changed:
                        try:
                            _atomic_write_json(SETTINGS_FILE, data)
                            logger.warning('Settings repaired/migrated: ' + ('; '.join(notes) if notes else 'schema normalized'))
                        except Exception as e:
                            logger.warning(f'Settings migration save failed: {e}')
                    loaded_from = label
                    return data
            except Exception as e:
                if label == 'settings.json':
                    logger.error(f'Load settings error: {e}')
                else:
                    logger.warning(f'Load settings backup error: {e}')
        try:
            if os.path.exists(SETTINGS_FILE):
                ts = time.strftime('%Y%m%d-%H%M%S')
                bad = SETTINGS_FILE + f'.corrupt.{ts}'
                os.replace(SETTINGS_FILE, bad)
                logger.warning(f'Corrupt settings moved to {bad}')
        except Exception:
            pass
        return {}
def _save_settings(data):
    if not isinstance(data, dict):
        return
    with _SETTINGS_IO_LOCK:
        try:
            data, _, _ = _migrate_settings_data(data)
            json.dumps(data, ensure_ascii=False)
            try:
                if os.path.exists(SETTINGS_FILE):
                    shutil.copy2(SETTINGS_FILE, SETTINGS_BAK)
            except Exception:
                pass
            _atomic_write_json(SETTINGS_FILE, data)
        except Exception as e:
            logger.error(f'Save settings error: {e}')
os.makedirs(PLUGIN_FOLDER, exist_ok=True)
if not os.path.exists(SETTINGS_FILE):
    try:
        _atomic_write_json(SETTINGS_FILE, {})
    except Exception:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f, indent=4, ensure_ascii=False)
LOG_FILE_LOCAL = os.path.join(PLUGIN_FOLDER, 'log.txt')
try:
    _fh_local = logging.FileHandler(LOG_FILE_LOCAL, encoding='utf-8')
    _fh_local.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [FTS-Plugin] %(message)s'))
    logger.addHandler(_fh_local)
except Exception as e:
    logger.debug(f'Local file logging init failed: {e}')
def _cfg_bool(cfg, key, default=False):
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ('1', 'true', 'yes', 'on'):
            return True
        if s in ('0', 'false', 'no', 'off', ''):
            return False
    return default
def _as_int(v, default, min_value=None, max_value=None):
    try:
        if isinstance(v, bool):
            raise ValueError()
        if isinstance(v, str):
            m = _re.search('-?\\d+', v.strip())
            if not m:
                raise ValueError()
            n = int(m.group(0))
        else:
            n = int(float(v))
    except Exception:
        n = int(default)
    if min_value is not None:
        n = max(int(min_value), n)
    if max_value is not None:
        n = min(int(max_value), n)
    return n
def _as_float_cfg(v, default=None, min_value=None):
    try:
        if isinstance(v, bool) or v is None:
            return default
        if isinstance(v, str):
            s = v.strip().replace(',', '.')
            if not s:
                return default
            m = _re.search('-?\\d+(?:\\.\\d+)?', s)
            if not m:
                return default
            f = float(m.group(0))
        else:
            f = float(v)
        if min_value is not None:
            f = max(float(min_value), f)
        return f
    except Exception:
        return default
def _is_cfg_like_dict(obj):
    return isinstance(obj, dict) and bool(_CFG_KNOWN_KEYS.intersection(obj.keys()) or any((k in obj for k in _CFG_TOKEN_ALIASES)))
def _sanitize_templates(tpls):
    base = _default_templates().copy()
    if isinstance(tpls, dict):
        for k, v in tpls.items():
            if k in base and isinstance(v, str) and v.strip():
                base[k] = v
    return base
def _sanitize_star_lots(items):
    result = []
    seen = set()
    def add(qty, lot_id, active=True, extra=None):
        q = _as_int(qty, 0, 0)
        lid = _as_int(lot_id, 0, 0)
        if q <= 0 or lid <= 0:
            return
        key = (q, lid)
        if key in seen:
            return
        seen.add(key)
        row = dict(extra or {})
        row['qty'] = q
        row['lot_id'] = lid
        row['active'] = _cfg_bool({'v': active}, 'v', True)
        for num_key in ('price', 'floor_price', 'autodump_floor', 'autodump_floor_rub'):
            if num_key in row:
                row[num_key] = _as_float_cfg(row.get(num_key), None, 0.0)
        result.append(row)
    if isinstance(items, dict):
        for k, v in items.items():
            if isinstance(v, dict):
                add(v.get('qty', k), v.get('lot_id') or v.get('id') or v.get('lot'), v.get('active', True), v)
            else:
                add(k, v, True, {})
    elif isinstance(items, (list, tuple, set)):
        for it in items:
            if isinstance(it, dict):
                add(it.get('qty') or it.get('stars') or it.get('count'), it.get('lot_id') or it.get('id') or it.get('lot'), it.get('active', True), it)
            elif isinstance(it, (list, tuple)) and len(it) >= 2:
                add(it[0], it[1], True, {})
    return sorted(result, key=lambda x: (int(x.get('qty', 0)), int(x.get('lot_id', 0))))
def _sanitize_lot_ids(items):
    ids = set()
    def add(v):
        try:
            lid = _as_int(v, 0, 1)
            if lid > 0:
                ids.add(int(lid))
        except Exception:
            pass
    if isinstance(items, dict):
        for k, v in items.items():
            if isinstance(v, dict):
                add(v.get('lot_id') or v.get('id') or v.get('lot') or k)
            else:
                add(v if not str(k).isdigit() else k)
    elif isinstance(items, (list, tuple, set)):
        for it in items:
            if isinstance(it, dict):
                add(it.get('lot_id') or it.get('id') or it.get('lot'))
            elif isinstance(it, (list, tuple)) and it:
                add(it[0])
            else:
                add(it)
    elif items is not None:
        add(items)
    return sorted(ids)
def _sanitize_order_records(items):
    if not isinstance(items, dict):
        return {}
    out = {}
    for oid, rec in list(items.items())[-ORDER_RECORDS_LIMIT:]:
        if oid is None or not isinstance(rec, dict):
            continue
        r = dict(rec)
        r['oid'] = str(r.get('oid') or oid)
        for k in ('created_ts', 'sent_ts', 'updated_ts', 'last_wait_reminder_ts', 'review_reminder_ts', 'finalized_ts'):
            if k in r:
                r[k] = _as_int(r.get(k), 0, 0)
        if 'qty' in r:
            r['qty'] = _as_int(r.get('qty'), 0, 0)
        out[str(oid)] = r
    return out
def _sanitize_cfg(raw, chat_id=None):
    raw = raw if isinstance(raw, dict) else {}
    cfg = dict(raw)
    for alias in _CFG_TOKEN_ALIASES:
        if not cfg.get('fragment_jwt') and cfg.get(alias):
            cfg['fragment_jwt'] = cfg.get(alias)
    cfg['plugin_enabled'] = _cfg_bool(cfg, 'plugin_enabled', True)
    cfg['lots_active'] = _cfg_bool(cfg, 'lots_active', False)
    cfg['auto_refund'] = _cfg_bool(cfg, 'auto_refund', False)
    cfg['auto_deactivate'] = _cfg_bool(cfg, 'auto_deactivate', True)
    cfg['preorder_username'] = _cfg_bool(cfg, 'preorder_username', False)
    cfg['fragment_jwt'] = str(cfg.get('fragment_jwt')).strip() if cfg.get('fragment_jwt') else None
    cfg['unit_star_price'] = _as_float_cfg(cfg.get('unit_star_price'), None, 0.0)
    cfg['markup_percent'] = _as_float_cfg(cfg.get('markup_percent'), 0.0) or 0.0
    cfg['wallet_version'] = str(cfg.get('wallet_version')).strip() if cfg.get('wallet_version') else None
    cfg['balance_ton'] = _as_float_cfg(cfg.get('balance_ton'), None, 0.0)
    cfg['balance_usdt'] = _as_float_cfg(cfg.get('balance_usdt'), None, 0.0)
    cfg.setdefault('last_wallet_raw', None)
    cfg['templates'] = _sanitize_templates(cfg.get('templates'))
    cfg['category_id'] = FNP_STARS_CATEGORY_ID
    cfg['min_balance_ton'] = _as_float_cfg(cfg.get('min_balance_ton'), FNP_MIN_BALANCE_TON, 0.0)
    cfg['min_balance_usdt'] = _as_float_cfg(cfg.get('min_balance_usdt'), FNP_MIN_BALANCE_USDT, 0.0)
    cfg['star_lots'] = _sanitize_star_lots(cfg.get('star_lots'))
    cfg['managed_lot_ids'] = _sanitize_lot_ids(cfg.get('managed_lot_ids'))
    cfg['retry_liteserver'] = _cfg_bool(cfg, 'retry_liteserver', LITESERVER_RETRY_DEFAULT)
    cfg['auto_send_without_plus'] = _cfg_bool(cfg, 'auto_send_without_plus', False)
    cfg['skip_username_check'] = _cfg_bool(cfg, 'skip_username_check', False)
    cfg['queue_mode'] = _as_int(cfg.get('queue_mode', 1), 1, 1, 3)
    cfg['queue_timeout_sec'] = _as_int(cfg.get('queue_timeout_sec', QUEUE_TIMEOUT_DEFAULT), QUEUE_TIMEOUT_DEFAULT, 30, 86400)
    cfg['stars_currency'] = _normalize_stars_currency(cfg.get('stars_currency'))
    cfg['usdt_fallback_to_ton'] = _cfg_bool(cfg, 'usdt_fallback_to_ton', False)
    cfg['price_change_notifications'] = _cfg_bool(cfg, 'price_change_notifications', True)
    cfg['auto_price_fragment_enabled'] = _cfg_bool(cfg, 'auto_price_fragment_enabled', False)
    cfg['autodump_enabled'] = _cfg_bool(cfg, 'autodump_enabled', False)
    cfg['autodump_interval_sec'] = _as_int(cfg.get('autodump_interval_sec', FTS_AUTODUMP_DEFAULT_INTERVAL_SEC), FTS_AUTODUMP_DEFAULT_INTERVAL_SEC, 600, 86400)
    cfg['autodump_notifications'] = _cfg_bool(cfg, 'autodump_notifications', True)
    cfg['balance_lot_filter_enabled'] = _cfg_bool(cfg, 'balance_lot_filter_enabled', True)
    cfg['balance_lot_filter_notifications'] = _cfg_bool(cfg, 'balance_lot_filter_notifications', True)
    cfg['order_watch_enabled'] = _cfg_bool(cfg, 'order_watch_enabled', True)
    cfg['order_watch_interval_sec'] = _as_int(cfg.get('order_watch_interval_sec', ORDER_WATCH_INTERVAL_DEFAULT), ORDER_WATCH_INTERVAL_DEFAULT, 60, 86400)
    cfg['order_wait_reminder_sec'] = _as_int(cfg.get('order_wait_reminder_sec', ORDER_WAIT_REMINDER_DEFAULT), ORDER_WAIT_REMINDER_DEFAULT, 120, 86400)
    cfg['order_review_reminder_enabled'] = _cfg_bool(cfg, 'order_review_reminder_enabled', True)
    cfg['order_review_reminder_sec'] = _as_int(cfg.get('order_review_reminder_sec', ORDER_REVIEW_REMINDER_DEFAULT), ORDER_REVIEW_REMINDER_DEFAULT, 300, 604800)
    cfg['order_records'] = _sanitize_order_records(cfg.get('order_records'))
    cfg['config_version'] = SETTINGS_SCHEMA_VERSION
    return cfg
def _migrate_settings_data(data):
    notes = []
    changed = False
    if not isinstance(data, dict):
        return ({}, True, ['settings root was not an object'])
    data = dict(data)
    if isinstance(data.get('settings'), dict) and (not any((str(k).lstrip('-').isdigit() for k in data.keys()))):
        data = dict(data['settings'])
        changed = True
        notes.append('unwrapped settings object')
    flat_keys = [k for k in list(data.keys()) if k in _CFG_KNOWN_KEYS or k in _CFG_TOKEN_ALIASES]
    if flat_keys:
        legacy = data.get(LEGACY_SETTINGS_KEY) if isinstance(data.get(LEGACY_SETTINGS_KEY), dict) else {}
        for k in flat_keys:
            legacy[k] = data.pop(k)
        data[LEGACY_SETTINGS_KEY] = legacy
        changed = True
        notes.append('old flat config moved to legacy profile')
    for k, v in list(data.items()):
        if k == SETTINGS_META_KEY:
            continue
        if isinstance(v, dict) and (k == LEGACY_SETTINGS_KEY or _is_cfg_like_dict(v) or str(k).lstrip('-').isdigit()):
            fixed = _sanitize_cfg(v, chat_id=k)
            if fixed != v:
                data[k] = fixed
                changed = True
    meta = data.get(SETTINGS_META_KEY) if isinstance(data.get(SETTINGS_META_KEY), dict) else {}
    if meta.get('schema') != SETTINGS_SCHEMA_VERSION or meta.get('plugin') != NAME:
        meta.update({'schema': SETTINGS_SCHEMA_VERSION, 'plugin': NAME, 'updated_at': int(time.time())})
        data[SETTINGS_META_KEY] = meta
        changed = True
    return (data, changed, notes)
def _attach_legacy_cfg_if_needed(data, key, cfg):
    if isinstance(cfg, dict) and cfg:
        return (cfg, False)
    legacy = data.get(LEGACY_SETTINGS_KEY)
    if isinstance(legacy, dict) and legacy:
        logger.warning(f'Settings legacy profile attached to chat_id={key}; users from <=1.7.0 do not need to delete config')
        try:
            _order_log('info', 'config_migrated', chat_id=key, schema=SETTINGS_SCHEMA_VERSION, source='legacy')
        except Exception:
            pass
        return (dict(legacy), True)
    return ({}, False)
def _auto_send_without_plus(chat_id):
    try:
        cfg = _get_cfg_for_orders(chat_id)
        return _cfg_bool(cfg, 'auto_send_without_plus', False)
    except Exception:
        return True
_anti_dup_prompts = {}
def _should_prompt_once(chat_id, order_id, qty, window_sec=20):
    key = f"{chat_id}:{order_id or 'noid'}:{int(qty)}"
    now = time.time()
    last = _anti_dup_prompts.get(key, 0.0)
    if now - last < window_sec:
        return False
    _anti_dup_prompts[key] = now
    return True
def _default_templates():
    return {'purchase_created': 'Спасибо за покупку {qty}⭐!\nНапишите ваш Telegram-тег одной строкой в формате @username.\nПример: @username', 'username_received': 'Принял тег: @{username}. Проверяю…', 'username_invalid': '❌ Некорректный или несуществующий тег.\nОтправьте верный Telegram-тег в формате @username (5–32, латиница/цифры/подчёркивание), а затем подтвердите ответом «+».\nПример: @username', 'username_valid': '✅ Тег принят: @{username}.', 'sending': 'Отправляю {qty}⭐ на @{username}…', 'sent': '✅ Готово: отправлено {qty}⭐ на @{username}. {order_url}', 'failed': '❌ Не удалось отправить звёзды: {reason}', 'queued': '🕒 Заказ принят. Сейчас вы в очереди: позиция {pos}.\nЯ напишу, когда дойдёт ваша очередь.', 'your_turn': '⭐️ До вас дошла очередь на {qty}⭐.\nПришлите ваш Telegram-тег одной строкой: @username'}
def _fmt_tpl(tpl, **kw):
    try:
        return tpl.format(**kw)
    except Exception:
        return tpl
def _tpl(chat_id, key, **kw):
    cfg_owner = _get_cfg_for_orders(chat_id)
    tpls = (cfg_owner.get('templates') if isinstance(cfg_owner, dict) else None) or {}
    if not tpls:
        cfg_local = _get_cfg(chat_id)
        tpls = (cfg_local.get('templates') if isinstance(cfg_local, dict) else None) or {}
    default = _default_templates().get(key, '')
    raw = tpls.get(key, default)
    return _fmt_tpl(raw, **kw)
def _get_cfg(chat_id):
    data = _load_settings()
    key = str(chat_id)
    raw_cfg, attached = _attach_legacy_cfg_if_needed(data, key, data.get(key))
    cfg = _sanitize_cfg(raw_cfg, chat_id=key)
    changed = attached or data.get(key) != cfg
    data[key] = cfg
    if changed:
        _save_settings(data)
    return cfg
def _cfg_key_for_orders(chat_id):
    key = str(chat_id)
    try:
        data = _load_settings()
        cfg = data.get(key) or {}
        if isinstance(cfg, dict) and cfg.get('fragment_jwt'):
            return key
        legacy = data.get(LEGACY_SETTINGS_KEY)
        if isinstance(legacy, dict) and legacy.get('fragment_jwt'):
            return LEGACY_SETTINGS_KEY
        for k, v in data.items():
            if k == SETTINGS_META_KEY:
                continue
            if isinstance(v, dict) and v.get('fragment_jwt'):
                return str(k)
    except Exception as e:
        logger.warning(f'_cfg_key_for_orders failed: {e}')
    return key
def _get_cfg_for_orders(chat_id):
    cfg = _get_cfg(chat_id)
    if cfg.get('fragment_jwt'):
        return cfg
    data = _load_settings()
    legacy = data.get(LEGACY_SETTINGS_KEY)
    if isinstance(legacy, dict) and legacy.get('fragment_jwt'):
        return _sanitize_cfg(legacy, chat_id=LEGACY_SETTINGS_KEY)
    for k, v in data.items():
        if k == SETTINGS_META_KEY:
            continue
        if isinstance(v, dict) and v.get('fragment_jwt'):
            return _sanitize_cfg(v, chat_id=k)
    return cfg
def _set_cfg_for_orders(chat_id, **updates):
    return _set_cfg(_cfg_key_for_orders(chat_id), **updates)
def _skip_username_check(chat_id):
    try:
        cfg = _get_cfg(chat_id)
        return _cfg_bool(cfg, 'skip_username_check', False)
    except Exception:
        return False
def _set_cfg(chat_id, **updates):
    with _SETTINGS_IO_LOCK:
        data = _load_settings()
        key = str(chat_id)
        raw_cfg, attached = _attach_legacy_cfg_if_needed(data, key, data.get(key))
        cfg = _sanitize_cfg(raw_cfg, chat_id=key)
        cfg.update(updates)
        cfg = _sanitize_cfg(cfg, chat_id=key)
        data[key] = cfg
        _save_settings(data)
        return cfg
def _state_on(v):
    return '🟢 Включено' if v else '🔴 Выключено'
def _normalize_stars_currency(v):
    s = str(v or FTS_CURRENCY_TON).strip().lower()
    if s in {'usdt', 'usd', 'usdt-ton', 'usdt_ton', 'usdt ton'}:
        return FTS_CURRENCY_USDT_TON
    return FTS_CURRENCY_TON
def _stars_currency_label(v):
    return 'USDT (TON)' if _normalize_stars_currency(v) == FTS_CURRENCY_USDT_TON else 'TON'
def _stars_currency_emoji(v):
    return '💲' if _normalize_stars_currency(v) == FTS_CURRENCY_USDT_TON else '💎'
def _fmt_amount(v, suffix):
    if isinstance(v, (int, float)):
        return f'{float(v):.6f}'.rstrip('0').rstrip('.') + f' {suffix}'
    return f'— {suffix}'
def _wallet_balance_text(cfg):
    return f"{_fmt_amount(cfg.get('balance_ton'), 'TON')} / {_fmt_amount(cfg.get('balance_usdt'), 'USDT')}"
def _safe_edit(bot, chat_id, msg_id, text, kb=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id, parse_mode='HTML', reply_markup=kb, disable_web_page_preview=True)
        return True
    except ApiTelegramException as e:
        low = str(e).lower()
        if 'message is not modified' in low:
            logger.debug(f'edit_message skipped: {e}')
            return True
        if 'message to edit not found' in low or "message can't be edited" in low or 'there is no text in the message to edit' in low or ('message is not a text message' in low) or ('chat not found' in low) or ('bot was blocked' in low):
            logger.debug(f'edit_message skipped: {e}')
            return False
        logger.debug(f'edit_message failed: {e}')
        return False
    except Exception as e:
        logger.debug(f'edit_message failed: {e}')
        return False
def _safe_send_tg(bot, chat_id, text, kb=None):
    try:
        return bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb, disable_web_page_preview=True)
    except TypeError:
        try:
            return bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
        except Exception as e:
            logger.debug(f'send_message failed: {e}')
    except Exception as e:
        logger.debug(f'send_message failed: {e}')
    return None
def _safe_delete(bot, chat_id, msg_id):
    try:
        if msg_id:
            bot.delete_message(chat_id, msg_id)
    except ApiTelegramException as e:
        low = str(e).lower()
        if 'message to delete not found' in low or 'chat not found' in low or 'bot was blocked' in low:
            logger.debug(f'delete_message skipped: {e}')
            return
        logger.debug(f'delete_message failed: {e}')
    except Exception as e:
        logger.debug(f'delete_message failed: {e}')
def _about_text():
    if not _meta_guard():
        return _tamper_text()
    return f'🧩 <b>Плагин:</b> FTS Plugin\n📦 <b>Версия:</b> <code>{VERSION}</code>\n👤 <b>Автор:</b> <a href="{CREATOR_URL}">{CREDITS}</a>\n\nВыберите раздел ниже.'
def _settings_text(chat_id):
    if not _meta_guard():
        return _tamper_text()
    cfg = _get_cfg(chat_id)
    token_state = 'привязан ✅' if cfg.get('fragment_jwt') else 'не добавлен ❌'
    lot_count = len(cfg.get('star_lots') or [])
    reason = cfg.get('last_auto_deact_reason')
    state_txt, _ = _lots_state_summary(cfg)
    lots_line = f'• Лоты: <b>{state_txt}</b>'
    if state_txt != '🟢 Включены' and reason:
        lots_line += f' <i>(авто-выкл: {reason})</i>'
    currency = _normalize_stars_currency(cfg.get('stars_currency'))
    fallback_line = ''
    if currency == FTS_CURRENCY_USDT_TON:
        fallback_line = f"• Автопереход USDT → TON: <b>{_state_on(cfg.get('usdt_fallback_to_ton', False))}</b>\n"
    min_bal_line = f"• Порог баланса USDT: <code>{cfg.get('min_balance_usdt', FNP_MIN_BALANCE_USDT)}</code>\n" if currency == FTS_CURRENCY_USDT_TON else f"• Порог баланса TON: <code>{cfg.get('min_balance_ton', FNP_MIN_BALANCE_TON)}</code>\n"
    return f"<b>Текущие настройки</b>\n\n• Плагин: <b>{_state_on(cfg.get('plugin_enabled', True))}</b>\n{lots_line}\n• Автовозврат: <b>{_state_on(cfg.get('auto_refund', False))}</b>\n• Автодеактивация: <b>{_state_on(cfg.get('auto_deactivate', True))}</b>\n• Ник из заказа: <b>{_state_on(cfg.get('preorder_username', False))}</b> (<i>без проверки существования</i>)\n• Валюта отправки звёзд: <b>{_stars_currency_emoji(currency)} {_stars_currency_label(currency)}</b>\n{fallback_line}• Наценка на звёзды: <code>{cfg.get('markup_percent', 0.0)}%</code>\n• Фильтр лотов по балансу: <b>{_state_on(cfg.get('balance_lot_filter_enabled', True))}</b>\n{min_bal_line}• Токен (JWT): <b>{token_state}</b>\n• Баланс: <code>{_wallet_balance_text(cfg)}</code>\n• ⭐ Звёздных лотов: <b>{lot_count}</b>\n\nВыберите действие:"
def _token_text(chat_id):
    if not _meta_guard():
        return _tamper_text()
    cfg = _get_cfg(chat_id)
    token_state = 'Токен привязан ✅' if cfg.get('fragment_jwt') else 'Токен не импортирован ❌'
    currency = _normalize_stars_currency(cfg.get('stars_currency'))
    return f'<b>Токен Fragment</b>\n\n• Состояние: <b>{token_state}</b>\n• Баланс: <code>{_wallet_balance_text(cfg)}</code>\n• Валюта звёзд: <b>{_stars_currency_emoji(currency)} {_stars_currency_label(currency)}</b>\n\nСоздание токена внутри плагина отключено: старый способ авторизации удалён. Импортируйте готовый JWT из внешнего кабинета/инструмента.\n\nUSDT работает в сети TON, поэтому для оплаты комиссии сети всё равно нужен небольшой запас TON.'
def _toggle_plugin(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get('plugin_enabled', True))
    _set_cfg(chat_id, plugin_enabled=new_state)
    try:
        bot.answer_callback_query(call.id, 'Плагин включён.' if new_state else 'Плагин выключен.')
    except Exception:
        pass
    _open_settings(bot, call)
def _toggle_preorder_username(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get('preorder_username', False))
    _set_cfg(chat_id, preorder_username=new_state)
    try:
        bot.answer_callback_query(call.id, 'Ник из заказа включён.' if new_state else 'Ник из заказа выключен.')
    except Exception:
        pass
    _open_settings(bot, call)
def _stars_text(chat_id):
    cfg = _get_cfg(chat_id)
    items = cfg.get('star_lots') or []
    unit = cfg.get('unit_star_price')
    unit_txt = f'{unit}' if isinstance(unit, (int, float)) else '—'
    header = f"<b>⚙️ Настройка лотов</b>\n\nТекущая наценка: <b>{cfg.get('markup_percent', 0.0)}%</b>\nЦена за 1⭐: <b>{unit_txt}</b>\n\n"
    if not items:
        body = 'Пока нет лотов со звёздами.\nНажмите «➕ Добавить лот».'
    else:
        rows = []
        for it in sorted(items, key=lambda x: (int(x.get('qty', 0)), int(x.get('lot_id', 0)))):
            rows.append(f"• <b>{it.get('qty')}</b> ⭐ → LOT <code>{it.get('lot_id')}</code> — " + ('🟢 активен' if it.get('active') else '🔴 выключен'))
        body = '\n'.join(rows)
    return header + body
def _extract_qty_from_title(title):
    if not title:
        return None
    s = (title or '').strip()
    m = _re.search('(\\d{1,7})\\s*(?:зв[её]зд\\w*|stars?)\\b', s, _re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    m = _re.search('(\\d{1,7})\\s*(?:⭐️|⭐)', s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    m = _re.search('(?:⭐️|⭐)\\s*(\\d{1,7})', s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    if _re.search('(?:зв[её]зд|stars?|⭐️|⭐)', s, _re.I):
        nums = []
        for x in _re.findall('\\d{1,7}', s):
            try:
                nums.append(int(x))
            except Exception:
                pass
        return max(nums) if nums else None
    return None
_sending_chats = set()
def _is_sending(chat_id):
    with _STATE_LOCK:
        return str(chat_id) in _sending_chats
def _set_sending(chat_id, v):
    k = str(chat_id)
    with _STATE_LOCK:
        if v:
            _sending_chats.add(k)
        else:
            _sending_chats.discard(k)
def _extract_username_from_text(text):
    if not text:
        return None
    s = _strip_invisible(str(text))
    m = _re.search('(?i)(?:по|by)\\s*username\\s*[,:\\-]?\\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)
    m = _re.search('(?i)\\b(?:ник|username)\\s*[:=]\\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)
    s2 = _re.sub('(?i)покупатель\\s+[A-Za-z0-9_]{5,32}\\s+оплатил(?:\\s+заказ)?[^.\\n]*\\.?', ' ', s)
    m = _re.search('@([A-Za-z0-9_]{5,32})', s2)
    if m:
        return m.group(1)
    return None
def _extract_username_from_order_text(text):
    if not text:
        return None
    s = _strip_invisible(str(text))
    u = _extract_username_from_text(s)
    if u:
        return u
    m = _re.search('(?i)(?:https?://)?t\\.me/(?:@)?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)
    m = _re.search('(?i)\\b(?:tg|тг|telegram|телеграм|телега)\\b\\s*[,:\\-=]?\\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)
    m = _re.search('(?i)\\b(?:для|to)\\b\\s*@?([A-Za-z0-9_]{5,32})\\b', s)
    if m:
        cand = m.group(1)
        if _validate_username(cand) and _re.search('[A-Za-z]', cand):
            return cand
    m = _re.fullmatch('\\s*@?([A-Za-z0-9_]{5,32})\\s*[.!?,;:]*\\s*', s)
    if m:
        cand = m.group(1)
        if _re.search('[A-Za-z]', cand):
            return cand
    return None
def _extract_explicit_handle(text):
    if not text:
        return None
    m = _re.search('@([A-Za-z0-9_]{5,32})', text)
    return m.group(1) if m else None
def _extract_username_from_any(x, depth=0):
    if depth > 2 or x is None:
        return None
    if isinstance(x, str):
        s = str(x)
        m = _re.search('@([A-Za-z0-9_]{4,32})', s)
        if m:
            return m.group(1)
        m = _re.search('(?i)(?:по|by)\\s*username\\s*[,:\\-]?\\s*@?([A-Za-z0-9_]{4,32})', s)
        if m:
            return m.group(1)
        m = _re.search('(?i)\\b(?:ник|username)\\s*[:=]\\s*@?([A-Za-z0-9_]{4,32})', s)
        if m:
            return m.group(1)
        return None
    if isinstance(x, dict):
        for k, v in x.items():
            if not isinstance(v, str):
                continue
            key_l = str(k).lower()
            if any((t in key_l for t in ('telegram', 'tg', 'ник', 'handle', 'stars', 'звезд', 'звезда'))):
                cand = _extract_username_from_any(v, depth + 1)
                if cand:
                    return cand
        for v in x.values():
            u = _extract_username_from_any(v, depth + 1)
            if u:
                return u
        return None
    if isinstance(x, (list, tuple, set)):
        for v in x:
            u = _extract_username_from_any(v, depth + 1)
            if u:
                return u
        return None
    try:
        for name in dir(x):
            if name.startswith('_'):
                continue
            try:
                v = getattr(x, name)
            except Exception:
                continue
            if isinstance(v, (str, dict, list, tuple, set)):
                u = _extract_username_from_any(v, depth + 1)
                if u:
                    return u
    except Exception:
        pass
    return None
def _check_username_exists(username, jwt):
    if not username:
        return False
    uname = username.lstrip('@').strip()
    urls = []
    for base in FRAGMENT_USER_URLS:
        base = (base or '').rstrip('/')
        if base:
            urls.append(f'{base}/{uname}/')
    urls.append(f'{FRAGMENT_BASE}/misc/user/{uname}/')
    headers_with_jwt = {'Accept': 'application/json'}
    if jwt:
        headers_with_jwt['Authorization'] = f'JWT {jwt}'
    for url in urls:
        try:
            r = _HTTP.get(url, headers=headers_with_jwt, timeout=8)
            if r.status_code == 200:
                try:
                    data = r.json()
                    if isinstance(data, dict) and (data.get('username') or data.get('user') or data.get('id')):
                        return True
                except Exception:
                    pass
            r2 = _HTTP.get(url, headers={'Accept': 'application/json'}, timeout=8)
            if r2.status_code == 200:
                try:
                    data = r2.json()
                    if isinstance(data, dict) and (data.get('username') or data.get('user') or data.get('id')):
                        return True
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f'_check_username_exists {url} failed: {e}')
    return False
def _check_username_exists_throttled(username, jwt, chat_id=None):
    key = str(chat_id) if chat_id is not None else '__global__'
    now = time.time()
    last = _last_username_check_ts.get(key, 0.0)
    wait = last + _USERNAME_CHECK_GAP - now
    if wait > 0:
        time.sleep(min(wait + random.random() * _USERNAME_CHECK_JITTER, _USERNAME_CHECK_GAP + _USERNAME_CHECK_JITTER))
    _last_username_check_ts[key] = time.time()
    return _check_username_exists(username, jwt)
def _as_balance_float(v):
    try:
        if isinstance(v, bool) or v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            s = v.strip().replace(',', '.')
            m = _re.search('-?\\d+(?:\\.\\d+)?', s)
            return float(m.group(0)) if m else None
    except Exception:
        return None
    return None
def _balance_from_node(node):
    if isinstance(node, dict):
        for key in ('balance', 'amount', 'value', 'available', 'free'):
            val = _as_balance_float(node.get(key))
            if val is not None:
                return val
    return _as_balance_float(node)
def _extract_wallet_info(data):
    if not isinstance(data, dict):
        return (None, None, None)
    ver = None
    bal_ton = None
    bal_usdt = None
    for key in ('wallet_version', 'walletVersion', 'version'):
        if key in data and isinstance(data[key], (str, int, float)):
            ver = str(data[key])
            break
    for key in ('balance_ton', 'balanceTon', 'ton_balance', 'tonBalance', 'balance'):
        if key in data:
            bal_ton = _balance_from_node(data.get(key))
            if bal_ton is not None:
                break
    for key in ('balance_usdt', 'balanceUsdt', 'usdt_balance', 'usdtBalance', 'usdt_ton_balance', 'usdtTonBalance'):
        if key in data:
            bal_usdt = _balance_from_node(data.get(key))
            if bal_usdt is not None:
                break
    for outer in ('wallet', 'ton'):
        node = data.get(outer)
        if isinstance(node, dict) and bal_ton is None:
            bal_ton = _balance_from_node(node)
    for outer in ('usdt', 'usdt_ton', 'tether'):
        node = data.get(outer)
        if bal_usdt is None:
            bal_usdt = _balance_from_node(node)
    if isinstance(data.get('balances'), (list, tuple)):
        for item in data.get('balances') or []:
            if not isinstance(item, dict):
                continue
            name = str(item.get('currency') or item.get('symbol') or item.get('asset') or item.get('coin') or '').lower()
            val = _balance_from_node(item)
            if val is None:
                continue
            if 'usdt' in name and bal_usdt is None:
                bal_usdt = val
            elif name in {'ton', 'toncoin'} and bal_ton is None:
                bal_ton = val
    if bal_ton is None:
        for key in ('nanoton', 'nanoTon', 'nanotons', 'balance_nano', 'balanceNano'):
            if key in data:
                v = _as_balance_float(data.get(key))
                if v is not None:
                    bal_ton = v / 1000000000.0 if v > 1000000.0 else v
                    break
    def walk_usdt(x):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if 'usdt' in lk:
                    val = _balance_from_node(v)
                    if val is not None:
                        return val
                    if isinstance(v, dict):
                        val = walk_usdt(v)
                        if val is not None:
                            return val
            for v in x.values():
                if isinstance(v, (dict, list, tuple)):
                    val = walk_usdt(v)
                    if val is not None:
                        return val
        elif isinstance(x, (list, tuple)):
            for v in x:
                val = walk_usdt(v)
                if val is not None:
                    return val
        return None
    if bal_usdt is None:
        bal_usdt = walk_usdt(data)
    return (ver, bal_ton, bal_usdt)
def _check_fragment_wallet(jwt):
    last_err = None
    for url in FRAGMENT_WALLET_URLS:
        try:
            r = _HTTP.get(url, headers={'Accept': 'application/json', 'Authorization': f'JWT {jwt}'}, timeout=20)
            r.raise_for_status()
            data = r.json()
            ver, bal_ton, bal_usdt = _extract_wallet_info(data if isinstance(data, dict) else {})
            return (ver, bal_ton, bal_usdt, data if isinstance(data, dict) else {'raw': data})
        except Exception as e:
            last_err = e
    logger.warning(f'Fragment wallet check failed: {last_err}')
    return (None, None, None, None)
_LS_RE = _re.compile('(?:\\blite\\s*server\\b|liteserver)', _re.I)
def _is_liteserver_transient_failure(resp_text, status=0, resp_json=None):
    txt = resp_text or ''
    low = txt.lower()
    if not _LS_RE.search(txt):
        if isinstance(resp_json, dict):
            dump = json.dumps(resp_json, ensure_ascii=False)
            if not _LS_RE.search(dump):
                return False
            low = dump.lower()
        else:
            return False
    if 'seqno' in low:
        return False
    if any((w in low for w in ('not enough', 'insufficient', 'balance', 'username', 'user not found', 'invalid', 'too many requests', '429', 'version'))):
        return False
    if status and status not in (0, 408, 500, 502, 503, 504):
        return False
    return True
def _fragment_order_status(resp_json):
    if isinstance(resp_json, dict):
        for key in ('status', 'state', 'order_status'):
            val = resp_json.get(key)
            if val:
                return str(val).upper()
    return ''
def _fragment_order_id(resp_json):
    if not isinstance(resp_json, dict):
        return None
    for key in ('id', 'order_id', 'orderId', 'transaction_id', 'transactionId', 'request_id', 'requestId'):
        val = resp_json.get(key)
        if val:
            return str(val)
    for subkey in ('data', 'result', 'payload'):
        sub = resp_json.get(subkey)
        if isinstance(sub, dict):
            val = _fragment_order_id(sub)
            if val:
                return val
    return None
def _is_balance_failure_resp(resp):
    try:
        if not isinstance(resp, dict):
            return False
        raw = resp.get('json')
        text = resp.get('text') or ''
        dump = json.dumps(raw, ensure_ascii=False) if isinstance(raw, (dict, list)) else str(raw or '')
        low = (text + ' ' + dump).lower()
        return any((x in low for x in ('not enough', 'insufficient', 'balance', 'low ton balance', 'no jetton wallet', 'недостат', 'не хватает')))
    except Exception:
        return False
def _order_stars_with_retry(jwt, username, quantity, show_sender=False, webhook_url=None, retry_enabled=False, currency=None, response_url=None):
    resp = _order_stars(jwt, username=username, quantity=quantity, show_sender=show_sender, webhook_url=webhook_url, currency=currency, response_url=response_url)
    if resp.get('ok') or not retry_enabled:
        return resp
    if _is_liteserver_transient_failure(resp.get('text', ''), int(resp.get('status') or 0), resp.get('json')):
        delay = random.uniform(LITESERVER_RETRY_SLEEP_MIN, LITESERVER_RETRY_SLEEP_MAX)
        _log('warn', f'SEND retry: liteserver transient error, attempt=2, sleep={delay:.2f}s')
        time.sleep(delay)
        resp2 = _order_stars(jwt, username=username, quantity=quantity, show_sender=show_sender, webhook_url=webhook_url, currency=currency, response_url=response_url)
        resp2['_retried'] = True
        return resp2
    return resp
def _order_stars(jwt, username, quantity, show_sender=False, webhook_url=None, currency=None, response_url=None):
    try:
        u = username.lstrip('@').strip()
        cur = _normalize_stars_currency(currency)
        payload = {'username': u, 'quantity': quantity, 'show_sender': bool(show_sender)}
        if cur == FTS_CURRENCY_USDT_TON:
            payload['currency'] = FTS_CURRENCY_USDT_TON
        if response_url:
            payload['response_url'] = response_url
        elif webhook_url:
            payload['response_url'] = webhook_url
        _log('info', f'SEND start: {quantity}⭐ → @{u} currency={cur}')
        r = _HTTP.post(FRAGMENT_ORDER_STARS, json=payload, headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'JWT {jwt}'}, timeout=120)
        resp_json = None
        ct = (r.headers.get('Content-Type') or '').lower()
        if 'application/json' in ct:
            try:
                resp_json = r.json()
            except Exception:
                resp_json = None
        ok_flags = set()
        status_val = _fragment_order_status(resp_json).lower()
        if isinstance(resp_json, dict):
            for k in ('ok', 'success', 'sent', 'purchased', 'done'):
                v = resp_json.get(k)
                if isinstance(v, bool) and v:
                    ok_flags.add(k)
            if status_val in {'ok', 'success', 'completed', 'complete', 'done', 'pending', 'blockchain_sent'}:
                ok_flags.add('status')
            if any((k in resp_json for k in ('tx', 'transaction', 'order_id', 'orderId', 'id', 'transaction_id', 'request_id'))):
                ok_flags.add('tx')
        ok = bool(ok_flags) and resp_json is not None
        body_text = (r.text or '')[:400]
        _log('info' if ok else 'error', f"SEND result: ok={ok} status={r.status_code} currency={cur} order_status={status_val or '-'} flags={','.join(sorted(ok_flags)) or '-'} body={body_text}")
        return {'ok': ok, 'status': r.status_code, 'text': r.text, 'json': resp_json, 'currency': cur, 'order_status': status_val.upper() if status_val else None, 'fragment_order_id': _fragment_order_id(resp_json)}
    except Exception as e:
        _log('error', f'SEND exception: {e}')
        return {'ok': False, 'status': 0, 'text': str(e), 'json': None, 'currency': _normalize_stars_currency(currency)}
def _send_stars_with_currency_fallback(cardinal, chat_id, cfg, jwt, username, quantity, show_sender=False):
    cur = _normalize_stars_currency(cfg.get('stars_currency'))
    resp = _order_stars_with_retry(jwt, username=username, quantity=quantity, show_sender=show_sender, retry_enabled=bool(cfg.get('retry_liteserver', LITESERVER_RETRY_DEFAULT)), currency=cur)
    if cur == FTS_CURRENCY_USDT_TON and (not (resp or {}).get('ok')) and _cfg_bool(cfg, 'usdt_fallback_to_ton', False) and _is_balance_failure_resp(resp):
        try:
            _set_cfg_for_orders(chat_id, stars_currency=FTS_CURRENCY_TON, last_usdt_fallback_reason='USDT balance is not enough for Fragment order', last_usdt_fallback_ts=int(time.time()))
        except Exception:
            pass
        _safe_send(cardinal, chat_id, '⚠️ USDT не хватило для оплаты. Переключаю оплату звёзд на TON и пробую ещё раз…')
        resp = _order_stars_with_retry(jwt, username=username, quantity=quantity, show_sender=show_sender, retry_enabled=bool(cfg.get('retry_liteserver', LITESERVER_RETRY_DEFAULT)), currency=FTS_CURRENCY_TON)
        resp['_currency_fallback'] = 'usdt_ton->ton'
    return resp
def _send_order_result_message(cardinal, chat_id, qty, username, order_url, resp=None):
    st = str((resp or {}).get('order_status') or '').upper()
    cur = _stars_currency_label((resp or {}).get('currency'))
    fid = (resp or {}).get('fragment_order_id')
    if st in {'PENDING', 'BLOCKCHAIN_SENT'}:
        extra = f'\nID Fragment: <code>{_h(fid)}</code>' if fid else ''
        _safe_send(cardinal, chat_id, f"✅ Fragment принял заявку на {int(qty)}⭐ для @{username.lstrip('@')}.\nВалюта: <b>{_h(cur)}</b>. Статус: <code>{_h(st)}</code>.{extra}\nЕсли Fragment ещё подтверждает транзакцию, звёзды могут прийти чуть позже.")
        return
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'sent', qty=qty, username=username.lstrip('@'), order_url=order_url))
def _queue_mode(chat_id):
    try:
        cfg = _get_cfg_for_orders(chat_id)
        return int(cfg.get('queue_mode') or 1)
    except Exception:
        return 1
def _queue_timeout_sec(chat_id):
    try:
        cfg = _get_cfg_for_orders(chat_id)
        v = cfg.get('queue_timeout_sec', int(os.getenv('FTS_QUEUE_TIMEOUT_SEC', '300')))
        return max(30, int(float(v)))
    except Exception:
        return int(os.getenv('FTS_QUEUE_TIMEOUT_SEC', '300'))
def _queue_mode_label(mode, timeout_sec):
    if mode == 1:
        return 'СТРОГАЯ'
    if mode == 2:
        return 'ПРОПУСК ГОТОВЫХ'
    if mode == 3:
        return f'ТАЙМАУТ→В КОНЕЦ ({timeout_sec // 60}М)'
    return 'СТРОГАЯ'
def _maybe_rotate_queue_head(cardinal, any_chat_id):
    try:
        if _queue_mode(any_chat_id) != 3:
            return False
        q = _q(any_chat_id)
        if not q:
            return False
        head = q[0]
        if head.get('finalized') or not _allowed_stages(head):
            return False
        stage = str(head.get('stage'))
        if stage not in {'await_username', 'await_confirm'}:
            return False
        ts = head.get('turn_ts')
        if not ts:
            if head.get('prompted') or _was_prompted(head.get('chat_id'), head.get('order_id')):
                head['turn_ts'] = time.time()
            return False
        if time.time() - float(ts) < float(_queue_timeout_sec(any_chat_id)):
            return False
        moved = q.pop(0)
        oid = moved.get('order_id')
        cid = moved.get('chat_id')
        moved['turn_ts'] = None
        moved['prompted'] = False
        moved['preconfirmed'] = False
        moved['auto_attempted_for'] = None
        moved['queue_notified'] = False
        if oid:
            _unmark_prompted(cid, oid, everywhere=True)
        q.append(moved)
        try:
            if cid is not None:
                _safe_send(cardinal, cid, '⏳ Вы не ответили — перенёс заказ в конец очереди. Напишите @username/«+», когда будете готовы.')
        except Exception:
            pass
        logger.info(f'[QUEUE] timeout move to end: OID={oid} CID={cid}')
        _notify_next_turn(cardinal, any_chat_id)
        return True
    except Exception as e:
        logger.debug(f'_maybe_rotate_queue_head failed: {e}')
        return False
def _get_my_subcategory_lots_safe(cardinal, subcategory_id):
    acc = getattr(cardinal, 'account', None)
    if not acc:
        return []
    fn = getattr(acc, 'get_my_subcategory_lots', None)
    if not callable(fn):
        return []
    try:
        return fn(int(subcategory_id)) or []
    except Exception as e:
        logger.warning(f'_get_my_subcategory_lots_safe failed: {e}')
        return []
def _get_my_lot_ids_by_subcategory(cardinal, subcategory_id):
    ids = set()
    for lot in _get_my_subcategory_lots_safe(cardinal, subcategory_id):
        try:
            lid = getattr(lot, 'id', None)
            if lid is None:
                continue
            ids.add(int(lid))
        except Exception:
            pass
    try:
        ids.update((int(x) for x in (_get_my_lots_by_category(cardinal, subcategory_id) or {}).keys()))
    except Exception as e:
        logger.debug(f'_get_my_lot_ids_by_subcategory profile fallback failed: {e}')
    return sorted(ids)
def _get_my_lots_by_category(cardinal, category_id):
    category_id = int(category_id)
    lots = {}
    def _extract_ids_from_sorted_map(all_map):
        ids = []
        if not isinstance(all_map, dict):
            return ids
        for _k, items in all_map.items():
            if isinstance(items, dict):
                for key, val in items.items():
                    lid = getattr(key, 'id', key)
                    try:
                        ids.append(int(lid))
                    except Exception:
                        continue
            else:
                seq = items if isinstance(items, (list, tuple, set)) else []
                for val in seq:
                    lid = getattr(val, 'id', None)
                    if lid is None:
                        continue
                    try:
                        ids.append(int(lid))
                    except Exception:
                        pass
        return ids
    try:
        cardinal.update_lots_and_categories()
        cand_ids = set()
        prof = getattr(cardinal, 'tg_profile', None) or getattr(cardinal, 'profile', None)
        if prof and hasattr(prof, 'get_sorted_lots'):
            for mode in (2, 1, 0, 3):
                try:
                    all_map = prof.get_sorted_lots(mode) or {}
                    cand_ids.update(_extract_ids_from_sorted_map(all_map))
                except Exception:
                    continue
        for lot_id in sorted(cand_ids):
            try:
                fields = cardinal.account.get_lot_fields(int(lot_id))
                if not fields:
                    continue
                sub = getattr(fields, 'subcategory', None) or getattr(fields, 'subcat', None)
                cid = None
                if sub is not None:
                    for attr in ('id', 'subcategory_id', 'category_id'):
                        if hasattr(sub, attr):
                            cid = getattr(sub, attr)
                            break
                if cid is None:
                    for attr in ('subcategory_id', 'category_id'):
                        if hasattr(fields, attr):
                            cid = getattr(fields, attr)
                            break
                if cid is None:
                    continue
                if int(cid) == category_id:
                    lots[int(lot_id)] = fields
            except Exception:
                continue
    except Exception as e:
        logger.warning(f'_get_my_lots_by_category failed: {e}')
    return lots
def _is_stars_lot(cardinal, lot_id):
    try:
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return False
        sub = getattr(fields, 'subcategory', None) or getattr(fields, 'subcat', None)
        cid = None
        if sub is not None:
            for attr in ('id', 'subcategory_id', 'category_id'):
                if hasattr(sub, attr):
                    cid = getattr(sub, attr)
                    break
        if cid is None:
            for attr in ('subcategory_id', 'category_id'):
                if hasattr(fields, attr):
                    cid = getattr(fields, attr)
                    break
        return cid is not None and int(cid) == int(FNP_STARS_CATEGORY_ID)
    except Exception:
        return False
def _order_is_stars(order):
    try:
        cand = getattr(order, 'subcategory_id', None) or getattr(order, 'category_id', None) or getattr(getattr(order, 'subcategory', None), 'id', None) or getattr(getattr(order, 'category', None), 'id', None)
        if cand is None:
            return False
        return int(cand) == int(FNP_STARS_CATEGORY_ID)
    except Exception:
        return False
def _activate_lot(cardinal, lot_id, trusted=False):
    try:
        lot_id = int(lot_id)
        is_star = _is_stars_lot(cardinal, lot_id)
        if not is_star and (not trusted):
            logger.warning(f'_activate_lot skipped: lot {lot_id} not in category {FNP_STARS_CATEGORY_ID}')
            return False
        if not is_star and trusted:
            logger.warning(f'_activate_lot trusted mode: category check failed for lot {lot_id}, trying to enable anyway')
        fields = cardinal.account.get_lot_fields(lot_id)
        if not fields:
            logger.warning(f'_activate_lot skipped: get_lot_fields returned empty for lot {lot_id}')
            return False
        if not getattr(fields, 'active', False):
            fields.active = True
            cardinal.account.save_lot(fields)
            logger.info(f'[LOTS] activated lot={lot_id}')
        else:
            logger.info(f'[LOTS] lot={lot_id} already active')
        return True
    except Exception as e:
        logger.warning(f'_activate_lot {lot_id} failed: {e}')
        return False
def _deactivate_lot(cardinal, lot_id, trusted=False):
    try:
        lot_id = int(lot_id)
        is_star = _is_stars_lot(cardinal, lot_id)
        if not is_star and (not trusted):
            logger.warning(f'_deactivate_lot skipped: lot {lot_id} not in category {FNP_STARS_CATEGORY_ID}')
            return False
        if not is_star and trusted:
            logger.warning(f'_deactivate_lot trusted mode: category check failed for lot {lot_id}, trying to disable anyway')
        fields = cardinal.account.get_lot_fields(lot_id)
        if not fields:
            logger.warning(f'_deactivate_lot skipped: get_lot_fields returned empty for lot {lot_id}')
            return False
        if getattr(fields, 'active', False):
            fields.active = False
            cardinal.account.save_lot(fields)
            logger.info(f'[LOTS] deactivated lot={lot_id}')
        else:
            logger.info(f'[LOTS] lot={lot_id} already inactive')
        return True
    except Exception as e:
        logger.warning(f'_deactivate_lot {lot_id} failed: {e}')
        return False
def _apply_star_lots_state(cardinal, star_lots, enabled):
    report = {'ok': [], 'skip': [], 'err': []}
    for it in star_lots or []:
        lot_id = it.get('lot_id')
        if not lot_id:
            continue
        try:
            ok = _activate_lot(cardinal, lot_id, trusted=True) if enabled else _deactivate_lot(cardinal, lot_id, trusted=True)
            (report['ok'] if ok else report['skip']).append(int(lot_id))
        except Exception as e:
            report['err'].append(int(lot_id))
            logger.warning(f'apply_star_lots_state {lot_id} failed: {e}')
    logger.info(f'[LOTS] apply_star_lots_state enabled={enabled} report={report}')
    return report
def _apply_category_state(cardinal, category_id, enabled, known_lot_ids=None):
    category_id = int(category_id or FNP_STARS_CATEGORY_ID)
    report = {'ok': [], 'skip': [], 'err': []}
    known = set(_sanitize_lot_ids(known_lot_ids or []))
    direct = set(_get_my_lot_ids_by_subcategory(cardinal, category_id))
    profile = set()
    try:
        profile = set((int(x) for x in (_get_my_lots_by_category(cardinal, category_id) or {}).keys()))
    except Exception as e:
        logger.debug(f'_apply_category_state profile fallback failed: {e}')
    lot_ids = sorted(known | direct | profile)
    trusted_ids = set(lot_ids)
    logger.info(f'[CATEGORY] apply_state: subcategory={category_id} enabled={enabled} known={len(known)} direct={len(direct)} profile={len(profile)} total={len(lot_ids)}')
    for lot_id in lot_ids:
        try:
            ok = _activate_lot(cardinal, lot_id, trusted=lot_id in trusted_ids) if enabled else _deactivate_lot(cardinal, lot_id, trusted=lot_id in trusted_ids)
            (report['ok'] if ok else report['skip']).append(int(lot_id))
        except Exception as e:
            report['err'].append(int(lot_id))
            logger.warning(f'apply_category_state {lot_id} failed: {e}')
    return report
def _managed_lot_ids_from_cfg(cfg):
    ids = set(_sanitize_lot_ids((cfg or {}).get('managed_lot_ids')))
    for it in (cfg or {}).get('star_lots') or []:
        try:
            lid = int(it.get('lot_id') or 0)
            if lid > 0:
                ids.add(lid)
        except Exception:
            pass
    return sorted(ids)
def _merge_lot_ids(*groups):
    ids = set()
    for g in groups:
        ids.update(_sanitize_lot_ids(g))
    return sorted(ids)
def _ids_from_report(rep):
    if not isinstance(rep, dict):
        return []
    return _merge_lot_ids(rep.get('ok') or [], rep.get('skip') or [], rep.get('err') or [])
def _lot_report_short(rep):
    if not isinstance(rep, dict):
        return 'нет отчёта'
    return f"ok={len(rep.get('ok') or [])}, skip={len(rep.get('skip') or [])}, err={len(rep.get('err') or [])}"
def _event_chat_id(e):
    return getattr(getattr(e, 'message', None), 'chat_id', None) or getattr(e, 'chat_id', None) or getattr(getattr(e, 'order', None), 'chat_id', None)
def _parse_fragment_error_text(response_text, status_code=0):
    fallback = 'Ошибка обработки заказа.'
    low_text = (response_text or '').lower()
    try:
        data = json.loads(response_text)
    except Exception:
        data = None
    if 'no jetton wallet' in low_text:
        return 'USDT-кошелёк Fragment/TON не инициализирован или на нём нет USDT-jetton. Для оплаты USDT нужен входящий USDT в сети TON и небольшой запас TON на комиссию.'
    if 'low ton balance' in low_text:
        return 'Недостаточно TON для комиссии сети TON. Даже при оплате USDT нужен небольшой запас TON.'
    if 'seqno' in low_text and ('exit code -256' in low_text or 'get method "seqno"' in low_text):
        return 'Неверная версия кошелька или кошелёк не инициализирован. Проверьте кошелёк/JWT в личном кабинете и выполните небольшую исходящую транзакцию.'
    if status_code == 429:
        return 'Слишком много запросов. Попробуйте ещё раз через минуту.'
    if status_code in (500, 502, 503, 504):
        return 'Сервис Fragment временно недоступен. Повторите позже.'
    if status_code in (401, 403):
        return 'Нужна повторная авторизация продавца. Попробуем ещё раз чуть позже.'
    if 'liteserver' in low_text or 'lite server' in low_text:
        return 'Сервис TON/Fragment временно недоступен. Попробуйте ещё раз позже.'
    if isinstance(data, dict):
        if 'username' in data:
            return 'Неверный Telegram-тег (проверьте @username).'
        if 'quantity' in data:
            return 'Минимальное количество для покупки — 50 ⭐.'
        for k in ('detail', 'message', 'error'):
            if data.get(k):
                msg = str(data[k])
                ml = msg.lower()
                if 'no jetton wallet' in ml:
                    return 'USDT-кошелёк Fragment/TON не инициализирован или на нём нет USDT-jetton.'
                if 'low ton balance' in ml:
                    return 'Недостаточно TON для комиссии сети TON.'
                if 'not enough' in ml or 'balance' in ml:
                    return 'Недостаточно средств на кошельке Fragment.'
                if 'version' in ml:
                    return 'Неверная версия кошелька у продавца.'
                if 'username' in ml:
                    return 'Пользователь с таким @username не найден.'
                return msg[:200]
        if isinstance(data.get('errors'), list):
            joined = ' | '.join((str(x.get('error') or x) for x in data['errors'][:3]))
            jl = joined.lower()
            if 'no jetton wallet' in jl:
                return 'USDT-кошелёк Fragment/TON не инициализирован или на нём нет USDT-jetton.'
            if 'low ton balance' in jl:
                return 'Недостаточно TON для комиссии сети TON.'
            if 'balance' in jl:
                return 'Недостаточно средств на кошельке Fragment.'
            return (joined or fallback)[:200]
        if isinstance(data.get('data'), dict):
            for k in ('error', 'message', 'detail'):
                if data['data'].get(k):
                    return str(data['data'][k])[:200]
    elif isinstance(data, list) and data:
        return ' | '.join((str(x) for x in data[:3]))[:200]
    return fallback
def _classify_send_failure(resp_text, status, username, jwt):
    low_resp = (resp_text or '').lower()
    if 'no jetton wallet' in low_resp:
        return ('seller', 'USDT-кошелёк Fragment/TON не инициализирован или на нём нет USDT-jetton.')
    if 'low ton balance' in low_resp:
        return ('seller', 'Недостаточно TON для комиссии сети TON.')
    if status in (401, 403):
        return ('seller', 'Нужна повторная авторизация продавца.')
    if status == 429:
        return ('seller', 'Слишком много запросов. Попробуйте ещё раз через минуту.')
    if status in (500, 502, 503, 504):
        return ('seller', 'Сервис Fragment временно недоступен. Повторите позже.')
    reason = _parse_fragment_error_text(resp_text, status)
    low = (reason or '').lower()
    if 'seqno' in low_resp:
        return ('seller', 'Неверная версия кошелька у продавца или кошелёк не инициализирован.')
    if any((t in low for t in ('username', 'user not found', 'not found', 'invalid', 'does not exist'))):
        return ('username', 'Пользователь с таким @username не найден.')
    if status == 400 and username and (not _check_username_exists_throttled(username, jwt)):
        return ('username', 'Пользователь с таким @username не найден.')
    if any((t in low for t in ('balance', 'not enough', 'jetton', 'комисс'))):
        return ('seller', reason or 'Недостаточно средств на кошельке Fragment.')
    return ('seller', reason or 'Ошибка обработки заказа.')
def _auto_refund_order(cardinal, order_id, chat_id, reason):
    try:
        if order_id and str(order_id) in _done_oids:
            logger.warning(f'[REFUND] skip #{order_id}: already sent/done')
            return False
        cfg = _get_cfg(chat_id)
        rec = (cfg.get('order_records') or {}).get(str(order_id), {}) if order_id else {}
        if str((rec or {}).get('status') or '') in {'sent', 'sent_pending'}:
            logger.warning(f"[REFUND] skip #{order_id}: order record status={(rec or {}).get('status')}")
            return False
        cardinal.account.refund(order_id)
        _safe_send(cardinal, chat_id, '✅ Средства успешно возвращены.')
        logger.warning(f'[REFUND] Заказ {order_id}: возврат выполнен. Причина: {reason}')
        return True
    except Exception as e:
        logger.error(f'[REFUND] Не удалось вернуть средства за заказ {order_id}: {e}')
        _safe_send(cardinal, chat_id, '❌ Не удалось оформить возврат автоматически. Свяжитесь с админом.')
        return False
def _coin_unit_for_balance(cfg, cur):
    cur = _normalize_stars_currency(cur)
    data, src = _fetch_fragment_prices(cfg.get('fragment_jwt'))
    unit = _fragment_unit_price(data, cur) if data is not None else None
    if unit:
        return (float(unit), f'Fragment Prices: {unit:g} {_stars_currency_label(cur)} за 1⭐')
    rates = _tonapi_rates_rub()
    rate = rates.get('usdt' if cur == FTS_CURRENCY_USDT_TON else 'ton')
    rub = _num(cfg.get('last_auto_price_base_unit') or cfg.get('unit_star_price'))
    if rub and rate:
        return (float(rub) / float(rate), f'fallback: {rub:g} RUB / {rate:g} RUB')
    return (None, f'не удалось получить стоимость 1⭐ в {_stars_currency_label(cur)} ({src})')
def _balance_filter_msg(rows, cur, balance, unit):
    out = [f'<b>🧮 Фильтр лотов по балансу</b>\nБаланс: <code>{balance:g} {_stars_currency_label(cur)}</code>; 1⭐ ≈ <code>{unit:g} {_stars_currency_label(cur)}</code>']
    for r in rows[:60]:
        out.append(f"• {r['qty']}⭐ LOT <code>{r['lot_id']}</code>: нужно <code>{r['need']:.6f} {_stars_currency_label(cur)}</code> → {('🟢 включён' if r['active'] else '🔴 выключен')}")
    if len(rows) > 60:
        out.append(f'… и ещё {len(rows) - 60} лот(ов)')
    return '\n'.join(out)
def _apply_balance_lot_filter(cardinal, chat_id, cfg, bal_ton=None, bal_usdt=None, force=False):
    if not force and (not _cfg_bool(cfg, 'balance_lot_filter_enabled', True)):
        return (False, 'Фильтр по балансу выключен.')
    cur = _normalize_stars_currency(cfg.get('stars_currency'))
    bal = bal_usdt if cur == FTS_CURRENCY_USDT_TON else bal_ton
    if bal is None:
        bal = cfg.get('balance_usdt' if cur == FTS_CURRENCY_USDT_TON else 'balance_ton')
    if not isinstance(bal, (int, float)):
        return (False, 'Не удалось получить баланс кошелька.')
    unit, info = _coin_unit_for_balance(cfg, cur)
    if not unit:
        return (False, 'Не удалось посчитать стоимость лотов: ' + info)
    avail = max(0.0, float(bal) * max(0.0, min(1.0, float(FTS_BALANCE_LOT_RESERVE_RATIO))))
    items = cfg.get('star_lots') or []
    rows = []
    changed = False
    for it in items:
        try:
            qty = int(it.get('qty'))
            lid = int(it.get('lot_id'))
            need = float(qty) * float(unit)
            ok = need <= avail + 1e-12
            was = bool(it.get('active'))
            filtered = bool(it.get('balance_filtered'))
            want = ok and (was or filtered or bool(cfg.get('lots_active')))
            it['balance_required_' + ('usdt' if cur == FTS_CURRENCY_USDT_TON else 'ton')] = round(need, 6)
            if not ok and was:
                _deactivate_lot(cardinal, lid, trusted=True)
                it['active'] = False
                it['balance_filtered'] = True
                changed = True
                rows.append({'qty': qty, 'lot_id': lid, 'need': need, 'active': False})
            elif want and filtered and (not was):
                _activate_lot(cardinal, lid, trusted=True)
                it['active'] = True
                it['balance_filtered'] = False
                changed = True
                rows.append({'qty': qty, 'lot_id': lid, 'need': need, 'active': True})
        except Exception as e:
            logger.debug(f'balance lot filter skip: {e}')
    if changed:
        _set_cfg(chat_id, star_lots=items, lots_active=any((x.get('active') for x in items)), managed_lot_ids=_merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [x.get('lot_id') for x in items]), last_balance_filter_info=f'баланс {float(bal):g} {_stars_currency_label(cur)}, 1⭐≈{float(unit):g}', last_balance_filter_ts=int(time.time()))
        if _cfg_bool(cfg, 'balance_lot_filter_notifications', True):
            try:
                cardinal.telegram.bot.send_message(chat_id, _balance_filter_msg(rows, cur, float(bal), float(unit)), parse_mode='HTML')
            except Exception as e:
                logger.warning(f'balance filter notify failed: {e}')
        return (True, f'Фильтр применён: изменено {len(rows)} лот(ов). {info}')
    _set_cfg(chat_id, last_balance_filter_info=f'изменений нет; баланс {float(bal):g} {_stars_currency_label(cur)}, 1⭐≈{float(unit):g}', last_balance_filter_ts=int(time.time()))
    return (True, 'Фильтр проверил лоты: менять нечего. ' + info)
def _maybe_auto_deactivate(cardinal, cfg, chat_id=None):
    jwt = cfg.get('fragment_jwt')
    ver, bal_ton, bal_usdt, _raw = _check_fragment_wallet(jwt) if jwt else (None, None, None, None)
    if bal_ton is None and bal_usdt is None:
        logger.warning('[BALANCE] Не удалось получить баланс Fragment.')
        return
    owner_chat = _cfg_key_for_orders(chat_id) if chat_id is not None else '__orders__'
    if jwt:
        try:
            _set_cfg(owner_chat, wallet_version=ver, balance_ton=round(bal_ton, 6) if isinstance(bal_ton, (int, float)) else None, balance_usdt=round(bal_usdt, 6) if isinstance(bal_usdt, (int, float)) else None, last_wallet_raw=_raw)
        except Exception:
            pass
    try:
        if cardinal is not None and chat_id is not None:
            _maybe_auto_price_update(cardinal, chat_id)
            _maybe_autodump_update(cardinal, chat_id)
    except Exception as e:
        logger.debug(f'auto price update skipped: {e}')
    try:
        if cardinal is not None and chat_id is not None:
            _apply_balance_lot_filter(cardinal, owner_chat, _get_cfg(owner_chat), bal_ton, bal_usdt)
    except Exception as e:
        logger.debug(f'balance lot filter skipped: {e}')
    cur = _normalize_stars_currency(cfg.get('stars_currency'))
    if cur == FTS_CURRENCY_USDT_TON:
        thr_usdt = float(cfg.get('min_balance_usdt') or FNP_MIN_BALANCE_USDT)
        if _cfg_bool(cfg, 'usdt_fallback_to_ton', False) and isinstance(bal_usdt, (int, float)) and (bal_usdt < thr_usdt):
            _set_cfg_for_orders(owner_chat, stars_currency=FTS_CURRENCY_TON, last_usdt_fallback_reason=f'USDT balance {bal_usdt} < {thr_usdt}')
            logger.warning(f'[USDT->TON] USDT balance {bal_usdt} < {thr_usdt}, switched stars currency to TON.')
            return
        if isinstance(bal_usdt, (int, float)) and bal_usdt < thr_usdt and cfg.get('auto_deactivate', False):
            cat_id = FNP_STARS_CATEGORY_ID
            items = cfg.get('star_lots') or []
            known_ids = _managed_lot_ids_from_cfg(cfg)
            rep = _apply_category_state(cardinal, cat_id, False, known_lot_ids=known_ids)
            for it in items:
                it['active'] = False
            managed_ids = _merge_lot_ids(known_ids, _ids_from_report(rep))
            _set_cfg(owner_chat, lots_active=False, star_lots=items, managed_lot_ids=managed_ids, last_auto_deact_reason=f'Баланс {bal_usdt} USDT < порога {thr_usdt}', last_lot_toggle_report=f'auto_deactivate_usdt: {_lot_report_short(rep)}', last_lot_toggle_ts=int(time.time()))
            logger.warning(f'[AUTODEACT] USDT balance {bal_usdt} < {thr_usdt}. Выключены лоты категории {cat_id}. Managed={managed_ids}. Report={rep}')
        return
    thr = float(cfg.get('min_balance_ton') or FNP_MIN_BALANCE_TON)
    if bal_ton is not None and bal_ton < thr and cfg.get('auto_deactivate', False):
        cat_id = FNP_STARS_CATEGORY_ID
        items = cfg.get('star_lots') or []
        known_ids = _managed_lot_ids_from_cfg(cfg)
        rep = _apply_category_state(cardinal, cat_id, False, known_lot_ids=known_ids)
        for it in items:
            it['active'] = False
        managed_ids = _merge_lot_ids(known_ids, _ids_from_report(rep))
        _set_cfg(owner_chat, lots_active=False, star_lots=items, managed_lot_ids=managed_ids, last_auto_deact_reason=f'Баланс {bal_ton} TON < порога {thr}', last_lot_toggle_report=f'auto_deactivate_ton: {_lot_report_short(rep)}', last_lot_toggle_ts=int(time.time()))
        logger.warning(f'[AUTODEACT] Баланс {bal_ton} TON < {thr}. Выключены лоты категории {cat_id}. Managed={managed_ids}. Report={rep}')
CBT_HOME = f'{UUID}:home'
CBT_SETTINGS = f'{UUID}:settings'
CBT_FSM_CANCEL = f'{UUID}:fsm_cancel'
CBT_BACK_PLUGINS = getattr(CBT, 'BACK', f'{UUID}:back')
CBT_TOGGLE_PLUGIN = f'{UUID}:toggle_plugin'
CBT_TOGGLE_LOTS = f'{UUID}:toggle_lots'
CBT_TOGGLE_REFUND = f'{UUID}:toggle_refund'
CBT_TOGGLE_DEACT = f'{UUID}:toggle_deact'
CBT_REFRESH = f'{UUID}:refresh'
CBT_SET_MIN_BAL = f'{UUID}:set_min_balance'
CBT_TOKEN = f'{UUID}:token'
CBT_MESSAGES = f'{UUID}:msgs'
CBT_SAVES = f'{UUID}:saves'
CBT_SAVES_IMPORT = f'{UUID}:saves_import'
CBT_SAVES_DOWNLOAD = f'{UUID}:saves_download'
CBT_MSG_EDIT_P = f'{UUID}:msg_edit:'
CBT_MSG_RESET_P = f'{UUID}:msg_reset:'
CBT_SET_JWT = f'{UUID}:set_jwt'
CBT_DEL_JWT = f'{UUID}:del_jwt'
CBT_STARS = f'{UUID}:stars'
CBT_STAR_ADD = f'{UUID}:star_add'
CBT_STAR_ACT_ALL = f'{UUID}:star_act_all'
CBT_STAR_DEACT_ALL = f'{UUID}:star_deact_all'
CBT_STAR_TOGGLE_P = f'{UUID}:star_toggle:'
CBT_STAR_DEL_P = f'{UUID}:star_del:'
CBT_STAR_AUTOADD = f'{UUID}:star_autoadd'
CBT_CONFIRM_SEND = f'{UUID}:confirm_send'
CBT_CHANGE_USERNAME = f'{UUID}:change_username'
CBT_CANCEL_FLOW = f'{UUID}:cancel_flow'
CBT_TOGGLE_PREORDER = f'{UUID}:toggle_preorder_username'
CBT_MARKUP = f'{UUID}:markup'
CBT_MARKUP_APPLY = f'{UUID}:markup_apply'
CBT_MARKUP_CHANGE = f'{UUID}:markup_change'
CBT_PRICING = f'{UUID}:pricing'
CBT_AUTO_PRICE_FRAGMENT = f'{UUID}:auto_price_fragment'
CBT_TOGGLE_AUTO_PRICE = f'{UUID}:toggle_auto_price'
CBT_AUTODUMP = f'{UUID}:autodump'
CBT_TOGGLE_AUTODUMP = f'{UUID}:toggle_autodump'
CBT_AUTODUMP_RUN = f'{UUID}:autodump_run'
CBT_AUTODUMP_INTERVAL = f'{UUID}:autodump_interval'
CBT_TOGGLE_AUTODUMP_NOTIFY = f'{UUID}:toggle_autodump_notify'
CBT_AUTODUMP_FLOOR_P = f'{UUID}:autodump_floor:'
CBT_NOTIFICATIONS = f'{UUID}:notifications'
CBT_TOGGLE_PRICE_NOTIFY = f'{UUID}:toggle_price_notify'
CBT_TOGGLE_BALANCE_FILTER = f'{UUID}:toggle_balance_filter'
CBT_BALANCE_FILTER_RUN = f'{UUID}:balance_filter_run'
CBT_MINI_SETTINGS = f'{UUID}:mini'
CBT_ORDER_TOOLS = f'{UUID}:order_tools'
CBT_STAR_PRICE_P = f'{UUID}:star_price:'
CBT_MARKUP_RESET = f'{UUID}:markup_reset'
CBT_LOGS = f'{UUID}:logs'
CBT_STATS = f'{UUID}:stats'
CBT_STATS_RANGE_P = f'{UUID}:stats_range:'
CBT_UPDATE_PLUGIN = f'{UUID}:update_plugin'
CBT_UPDATE_PLUGIN_YES = f'{UUID}:update_plugin_yes'
CBT_UPDATE_PLUGIN_NO = f'{UUID}:update_plugin_no'
CBT_DELETE_ASK = f'{UUID}:delete_ask'
CBT_DELETE_YES = f'{UUID}:delete_yes'
CBT_DELETE_NO = f'{UUID}:delete_no'
CBT_PLUGINS_LIST_OPEN = f"{getattr(CBT, 'PLUGINS_LIST', '44')}:0"
CBT_TOGGLE_LITESERVER_RETRY = f'{UUID}:toggle_liteserver_retry'
CBT_TOGGLE_USERNAME_CHECK = f'{UUID}:toggle_username_check'
CBT_TOGGLE_AUTOSEND_PLUS = f'{UUID}:toggle_autosend_plus'
CBT_TOGGLE_ORDER_WATCH = f'{UUID}:toggle_order_watch'
CBT_TOGGLE_REVIEW_REMINDER = f'{UUID}:toggle_review_reminder'
CBT_ORDER_WATCH_RUN = f'{UUID}:order_watch_run'
CBT_ORDER_WATCH_INTERVAL = f'{UUID}:order_watch_interval'
CBT_ORDER_WAIT_REMINDER = f'{UUID}:order_wait_reminder'
CBT_REVIEW_REMINDER_TIME = f'{UUID}:review_reminder_time'
CBT_TOGGLE_QUEUE_MODE = f'{UUID}:toggle_queue_mode'
CBT_TOGGLE_STARS_CURRENCY = f'{UUID}:toggle_stars_currency'
CBT_TOGGLE_USDT_FALLBACK = f'{UUID}:toggle_usdt_fallback'
CBT_RESET_SETTINGS_ASK = f'{UUID}:reset_settings_ask'
CBT_RESET_SETTINGS_YES = f'{UUID}:reset_settings_yes'
CBT_RESET_SETTINGS_NO = f'{UUID}:reset_settings_no'
CBT_UNIT_PRICE = f'{UUID}:unit_price'
CBT_UNIT_PRICE_APPLY = f'{UUID}:unit_price_apply'
CBT_UNIT_PRICE_CHANGE = f'{UUID}:unit_price_change'
_fsm = {}
CLEAN_FSM_SENSITIVE = bool(int(os.getenv('FTS_Plugin_CLEAN_FSM_SENSITIVE', '1')))
def _track_fsm_mid(state, mid):
    if not CLEAN_FSM_SENSITIVE or not mid:
        return
    state.setdefault('cleanup_msg_ids', [])
    state['cleanup_msg_ids'].append(int(mid))
def _cleanup_fsm_msgs(bot, chat_id, state):
    if not CLEAN_FSM_SENSITIVE:
        return
    ids = state.get('cleanup_msg_ids') or []
    for mid in sorted(set(ids), reverse=True):
        _safe_delete(bot, chat_id, mid)
    state['cleanup_msg_ids'] = []
def _home_kb():
    kb = K()
    kb.row(B('⚙️ Настройки', callback_data=CBT_SETTINGS), B('📖 Инструкция', url=INSTRUCTION_URL))
    kb.row(B('⬆️ Обновить плагин', callback_data=CBT_UPDATE_PLUGIN), B('🗑 Удалить', callback_data=CBT_DELETE_ASK))
    kb.add(B('🔙 К списку плагинов', callback_data=CBT_PLUGINS_LIST_OPEN))
    return kb
def _settings_kb(chat_id):
    cfg = _get_cfg(chat_id)
    def onoff(v):
        return '🟢 Включено' if v else '🔴 Выключено'
    def onoff_short(v):
        return '🟢 Включён' if v else '🔴 Выключен'
    kb = K()
    kb.row(B(f"Плагин: {onoff(cfg.get('plugin_enabled', True))}", callback_data=CBT_TOGGLE_PLUGIN))
    state_txt, _ = _lots_state_summary(cfg)
    kb.row(B(f'Лоты: {state_txt}', callback_data=CBT_TOGGLE_LOTS))
    kb.row(B(f"Автовозврат: {onoff_short(cfg.get('auto_refund', False))}", callback_data=CBT_TOGGLE_REFUND), B(f"Автодеактивация: {onoff(cfg.get('auto_deactivate', True))}", callback_data=CBT_TOGGLE_DEACT))
    kb.row(B(f"Ник из заказа: {onoff_short(cfg.get('preorder_username', False))}", callback_data=CBT_TOGGLE_PREORDER))
    kb.row(B('🔐 Токен', callback_data=CBT_TOKEN))
    kb.row(B(f"💱 Оплата звёзд: {_stars_currency_label(cfg.get('stars_currency'))}", callback_data=CBT_TOGGLE_STARS_CURRENCY))
    kb.row(B('⚙️ Настройка лотов', callback_data=CBT_STARS))
    kb.row(B('🛠️ Мини-настройки', callback_data=CBT_MINI_SETTINGS))
    kb.row(B('📜 Логи', callback_data=CBT_LOGS))
    kb.row(B('📊 Статистика', callback_data=CBT_STATS))
    kb.row(B('🔄 Обновить данные', callback_data=CBT_REFRESH))
    kb.add(B('◀️ Назад', callback_data=CBT_HOME))
    return kb
def _fmt_minutes_from_sec(sec):
    try:
        mins = max(1, int(float(sec)) // 60)
    except Exception:
        mins = 0
    if mins < 60:
        return f'{mins} мин'
    if mins % 60 == 0:
        h = mins // 60
        return f'{h} ч'
    return f'{mins} мин'
def _mini_settings_text(chat_id):
    cfg = _get_cfg(chat_id)
    cur_code = _normalize_stars_currency(cfg.get('stars_currency'))
    cur_min = cfg.get('min_balance_usdt', FNP_MIN_BALANCE_USDT) if cur_code == FTS_CURRENCY_USDT_TON else cfg.get('min_balance_ton', FNP_MIN_BALANCE_TON)
    cur_min_label = 'USDT' if cur_code == FTS_CURRENCY_USDT_TON else 'TON'
    retry_state = _state_on(cfg.get('retry_liteserver', LITESERVER_RETRY_DEFAULT))
    check_state = '🔴 выключена (проверим при отправке)' if cfg.get('skip_username_check', False) else '🟢 включена'
    autosend = cfg.get('auto_send_without_plus', False)
    plus_state = '🟢 не нужно (автоотправка)' if autosend else '🟡 нужно (как раньше)'
    qtxt = _queue_mode_label(_queue_mode(chat_id), _queue_timeout_sec(chat_id))
    watch_every = _fmt_minutes_from_sec(cfg.get('order_watch_interval_sec', ORDER_WATCH_INTERVAL_DEFAULT))
    wait_after = _fmt_minutes_from_sec(cfg.get('order_wait_reminder_sec', ORDER_WAIT_REMINDER_DEFAULT))
    review_after = _fmt_minutes_from_sec(cfg.get('order_review_reminder_sec', ORDER_REVIEW_REMINDER_DEFAULT))
    order_summary = f"{_state_on(cfg.get('order_watch_enabled', True))}, проверка {watch_every}, ожидание {wait_after}, отзыв {review_after}"
    fb_line = ''
    if _normalize_stars_currency(cfg.get('stars_currency')) == FTS_CURRENCY_USDT_TON:
        fb_line = f"• Автопереход USDT → TON: <b>{_state_on(cfg.get('usdt_fallback_to_ton', False))}</b>\n"
    return f"<b>Мини-настройки</b>\n\n• Очередь: <b>{qtxt}</b>\n• Мин. баланс {cur_min_label}: <code>{cur_min}</code>\n• Повтор при LiteServer: <b>{retry_state}</b>\n• Проверка @username при вводе: <b>{check_state}</b>\n• Подтверждение «+»: <b>{plus_state}</b>\n• Заказы и отзывы: <code>{_h(order_summary)}</code>\n{fb_line}• Уведомления цен: <b>{_state_on(cfg.get('price_change_notifications', True))}</b>\n• Сообщения: редактирование шаблонов ответов покупателю\n• Сохранения: импорт, скачивание и сброс settings.json\n\nВыберите действие ниже."
def _mini_settings_kb(chat_id):
    cfg = _get_cfg(chat_id)
    kb = K()
    mode = _queue_mode(chat_id)
    timeout = _queue_timeout_sec(chat_id)
    kb.row(B(f'🧾 Очередь: {_queue_mode_label(mode, timeout)}', callback_data=CBT_TOGGLE_QUEUE_MODE))
    cur_code = _normalize_stars_currency(cfg.get('stars_currency'))
    cur_min = cfg.get('min_balance_usdt', FNP_MIN_BALANCE_USDT) if cur_code == FTS_CURRENCY_USDT_TON else cfg.get('min_balance_ton', FNP_MIN_BALANCE_TON)
    cur_label = 'USDT' if cur_code == FTS_CURRENCY_USDT_TON else 'TON'
    kb.row(B(f'🔋 Мин. баланс: {cur_min} {cur_label}', callback_data=CBT_SET_MIN_BAL))
    retry_label = '🔁 LiteServer-ретрай: ВКЛ' if cfg.get('retry_liteserver', LITESERVER_RETRY_DEFAULT) else '🔁 LiteServer-ретрай: ВЫКЛ'
    kb.row(B(retry_label, callback_data=CBT_TOGGLE_LITESERVER_RETRY))
    autosend = cfg.get('auto_send_without_plus', False)
    autosend_label = "⚡ Автоотправка без '+': ВКЛ" if autosend else "✋ Автоотправка без '+': ВЫКЛ (нужен '+')"
    kb.row(B(autosend_label, callback_data=CBT_TOGGLE_AUTOSEND_PLUS))
    kb.row(B('🧹 Заказы и отзывы', callback_data=CBT_ORDER_TOOLS))
    if _normalize_stars_currency(cfg.get('stars_currency')) == FTS_CURRENCY_USDT_TON:
        fb = cfg.get('usdt_fallback_to_ton', False)
        kb.row(B('🛟 USDT→TON при нехватке: ' + ('ВКЛ' if fb else 'ВЫКЛ'), callback_data=CBT_TOGGLE_USDT_FALLBACK))
    ucheck_label = '🔎 Проверка @username: ВКЛ' if not cfg.get('skip_username_check', False) else '🚫 Проверка @username: ВЫКЛ'
    kb.row(B(ucheck_label, callback_data=CBT_TOGGLE_USERNAME_CHECK))
    kb.row(B('🔔 Уведомления', callback_data=CBT_NOTIFICATIONS))
    kb.row(B('🧩 Сообщения', callback_data=CBT_MESSAGES))
    kb.row(B('💾 Сохранения', callback_data=CBT_SAVES))
    kb.add(B('◀️ Назад', callback_data=CBT_SETTINGS))
    return kb
def _open_mini_settings(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _mini_settings_text(chat_id), _mini_settings_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _order_tools_text(chat_id):
    cfg = _get_cfg(chat_id)
    watch_every = _fmt_minutes_from_sec(cfg.get('order_watch_interval_sec', ORDER_WATCH_INTERVAL_DEFAULT))
    wait_after = _fmt_minutes_from_sec(cfg.get('order_wait_reminder_sec', ORDER_WAIT_REMINDER_DEFAULT))
    review_after = _fmt_minutes_from_sec(cfg.get('order_review_reminder_sec', ORDER_REVIEW_REMINDER_DEFAULT))
    recs = cfg.get('order_records') or {}
    active = 0
    sent = 0
    try:
        for rec in recs.values():
            st = str((rec or {}).get('status') or '').lower()
            if st in {'queued', 'await_username', 'await_confirm', 'ready', 'sending'}:
                active += 1
            if st == 'sent':
                sent += 1
    except Exception:
        pass
    return f"<b>🧹 Заказы и отзывы</b>\n\n• Проверка зависших заказов: <b>{_state_on(cfg.get('order_watch_enabled', True))}</b>\n• Интервал проверки: <code>{watch_every}</code>\n• Напоминать ожидание через: <code>{wait_after}</code>\n• Напоминание об отзыве: <b>{_state_on(cfg.get('order_review_reminder_enabled', True))}</b>\n• Просить отзыв через: <code>{review_after}</code> после отправки\n• Записей заказов: <code>{len(recs)}</code>; активных: <code>{active}</code>; отправленных: <code>{sent}</code>\n\nЗдесь всё, что связано с повторной проверкой ваших заказов, зависшими заказами и просьбой оставить отзыв."
def _order_tools_kb(chat_id):
    cfg = _get_cfg(chat_id)
    kb = K()
    kb.row(B('🧹 Проверка заказов: ' + ('ВКЛ' if cfg.get('order_watch_enabled', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_ORDER_WATCH))
    kb.row(B(f"⏱ Проверять каждые: {_fmt_minutes_from_sec(cfg.get('order_watch_interval_sec', ORDER_WATCH_INTERVAL_DEFAULT))}", callback_data=CBT_ORDER_WATCH_INTERVAL))
    kb.row(B(f"⏳ Напоминать ожидание: {_fmt_minutes_from_sec(cfg.get('order_wait_reminder_sec', ORDER_WAIT_REMINDER_DEFAULT))}", callback_data=CBT_ORDER_WAIT_REMINDER))
    kb.row(B('⭐ Напоминание об отзыве: ' + ('ВКЛ' if cfg.get('order_review_reminder_enabled', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_REVIEW_REMINDER))
    kb.row(B(f"🕒 Отзыв через: {_fmt_minutes_from_sec(cfg.get('order_review_reminder_sec', ORDER_REVIEW_REMINDER_DEFAULT))}", callback_data=CBT_REVIEW_REMINDER_TIME))
    kb.row(B('🔍 Проверить зависшие сейчас', callback_data=CBT_ORDER_WATCH_RUN))
    kb.add(B('◀️ Назад', callback_data=CBT_MINI_SETTINGS))
    return kb
def _open_order_tools(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _order_tools_text(chat_id), _order_tools_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _saves_text(chat_id):
    try:
        size = os.path.getsize(SETTINGS_FILE) if os.path.exists(SETTINGS_FILE) else 0
    except Exception:
        size = 0
    return f'<b>💾 Сохранения</b>\n\nЗдесь можно управлять файлом <code>settings.json</code>:\n• импортировать сохранения из JSON-файла или текста;\n• скачать текущие сохранения;\n• сбросить сохранения после подтверждения.\n\nТекущий файл: <code>{size} байт</code>\n\n⚠️ В сохранениях может быть JWT-токен. Не отправляйте файл посторонним.'
def _saves_kb():
    kb = K()
    kb.row(B('📥 Импортировать', callback_data=CBT_SAVES_IMPORT))
    kb.row(B('📤 Скачать', callback_data=CBT_SAVES_DOWNLOAD))
    kb.row(B('♻️ Сбросить сохранения', callback_data=CBT_RESET_SETTINGS_ASK))
    kb.add(B('◀️ Назад', callback_data=CBT_MINI_SETTINGS))
    return kb
def _open_saves(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _saves_text(chat_id), _saves_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _ask_import_saves(bot, call):
    chat_id = call.message.chat.id
    st = {'step': 'saves_import'}
    _fsm[chat_id] = st
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, 'Пришлите файл <code>settings.json</code> или вставьте JSON текстом.\nИмпорт заменит текущие сохранения. Для отмены: /cancel', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or st
    _track_fsm_mid(st, getattr(m, 'message_id', None))
    _fsm[chat_id] = st
def _download_saves(bot, call):
    chat_id = call.message.chat.id
    try:
        data = _load_settings()
        payload = json.dumps(data if isinstance(data, dict) else {}, indent=4, ensure_ascii=False).encode('utf-8')
        fname = f"FTS-Plugin-settings-{time.strftime('%Y%m%d-%H%M%S')}.json"
        bot.send_document(chat_id, (fname, payload), caption='💾 Текущие сохранения FTS-Plugin. Файл может содержать JWT-токен.')
        try:
            bot.answer_callback_query(call.id, 'Сохранения отправлены.')
        except Exception:
            pass
    except Exception as e:
        try:
            bot.answer_callback_query(call.id, f'Не удалось скачать сохранения: {e}', show_alert=True)
        except Exception:
            pass
def _import_settings_payload(raw_text):
    try:
        obj = json.loads((raw_text or '').lstrip('\ufeff').strip())
        if isinstance(obj, dict) and isinstance(obj.get('settings'), dict):
            obj = obj['settings']
        if not isinstance(obj, dict):
            return (False, 'JSON должен быть объектом.')
        _atomic_write_json(SETTINGS_FILE, obj)
        return (True, '✅ Сохранения импортированы.')
    except json.JSONDecodeError as e:
        return (False, f'Некорректный JSON: {e}')
    except Exception as e:
        logger.error(f'Import settings error: {e}')
        return (False, f'Не удалось импортировать сохранения: {e}')
def _token_kb():
    kb = K()
    kb.add(B('📥 Импорт токена', callback_data=CBT_SET_JWT))
    kb.add(B('🗑 Удалить токен', callback_data=CBT_DEL_JWT))
    kb.add(B('◀️ Назад', callback_data=CBT_SETTINGS))
    return kb
def _stars_kb(chat_id):
    cfg = _get_cfg(chat_id)
    kb = K()
    for it in (cfg.get('star_lots') or [])[:10]:
        lot_id = it.get('lot_id')
        qty = it.get('qty')
        state = '🟢 ON' if it.get('active') else '🔴 OFF'
        floor = it.get('autodump_min_price')
        floor_txt = f'🧱 Порог {floor}' if isinstance(floor, (int, float)) else '🧱 Порог'
        kb.row(B(f'{qty}⭐  LOT {lot_id}  {state}', callback_data=f'{CBT_STAR_TOGGLE_P}{lot_id}'), B('💰 Цена', callback_data=f'{CBT_STAR_PRICE_P}{lot_id}'), B(floor_txt, callback_data=f'{CBT_AUTODUMP_FLOOR_P}{lot_id}'), B('🗑', callback_data=f'{CBT_STAR_DEL_P}{lot_id}'))
    kb.row(B('➕ Добавить лот', callback_data=CBT_STAR_ADD))
    kb.row(B('💹 Ценообразование', callback_data=CBT_PRICING))
    kb.row(B(f"🤖 Автодобавление лотов (кат. {cfg.get('category_id', FNP_STARS_CATEGORY_ID)})", callback_data=CBT_STAR_AUTOADD))
    kb.row(B('🔄 Обновить', callback_data=CBT_REFRESH))
    kb.row(B('⚡ Включить все', callback_data=CBT_STAR_ACT_ALL), B('💤 Выключить все', callback_data=CBT_STAR_DEACT_ALL))
    kb.add(B('◀️ Назад', callback_data=CBT_SETTINGS))
    return kb
def _pricing_text(chat_id):
    cfg = _get_cfg(chat_id)
    unit = cfg.get('unit_star_price')
    base = cfg.get('last_auto_price_base_unit')
    unit_txt = f'{unit}' if isinstance(unit, (int, float)) else '—'
    base_txt = f'{base}' if isinstance(base, (int, float)) else '—'
    return f"<b>💹 Ценообразование лотов</b>\n\n• Цена за 1⭐ без наценки: <code>{base_txt}</code>\n• Цена за 1⭐ с наценкой: <code>{unit_txt}</code>\n• Наценка: <code>{cfg.get('markup_percent', 0.0)}%</code>\n• Автоизменение цен: <b>{_state_on(cfg.get('auto_price_fragment_enabled', False))}</b>\n• Автодемп: <b>{_state_on(cfg.get('autodump_enabled', False))}</b>\n• Фильтр по балансу: <b>{_state_on(cfg.get('balance_lot_filter_enabled', True))}</b>\n• Валюта отправки звёзд: <b>{_stars_currency_emoji(cfg.get('stars_currency'))} {_stars_currency_label(cfg.get('stars_currency'))}</b>\n\n"
def _pricing_kb(chat_id=None):
    cfg = _get_cfg(chat_id) if chat_id is not None else {}
    kb = K()
    kb.row(B('⭐ Цена за 1⭐', callback_data=CBT_UNIT_PRICE), B('💹 Наценка %', callback_data=CBT_MARKUP))
    kb.row(B('🤖 Автоцены: ' + ('ВКЛ' if cfg.get('auto_price_fragment_enabled', False) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_AUTO_PRICE))
    kb.row(B('🔄 Обновить сейчас по Fragment', callback_data=CBT_AUTO_PRICE_FRAGMENT))
    kb.row(B('🧮 По балансу: ' + ('ВКЛ' if cfg.get('balance_lot_filter_enabled', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_BALANCE_FILTER), B('🔎 Проверить', callback_data=CBT_BALANCE_FILTER_RUN))
    kb.row(B('📉 Автодемп', callback_data=CBT_AUTODUMP))
    kb.row(B('♻️ Сбросить наценку', callback_data=CBT_MARKUP_RESET))
    kb.add(B('◀️ Назад', callback_data=CBT_STARS))
    return kb
def _open_pricing(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _pricing_text(chat_id), _pricing_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _autodump_text(chat_id):
    cfg = _get_cfg(chat_id)
    interval = int(cfg.get('autodump_interval_sec', 1800))
    mins = interval // 60
    qtys = sorted({int(x.get('qty')) for x in cfg.get('star_lots') or [] if x.get('qty')})
    floors = [f"{int(x.get('qty'))}⭐≥{x.get('autodump_min_price')}" for x in cfg.get('star_lots') or [] if x.get('qty') and isinstance(x.get('autodump_min_price'), (int, float))]
    last = cfg.get('last_autodump_info') or '—'
    qtxt = ', '.join(map(str, qtys[:30])) if qtys else '—'
    ftxt = ', '.join(floors[:20]) if floors else '—'
    return f"<b>📉 Автодемп</b>\n\n• Состояние: <b>{_state_on(cfg.get('autodump_enabled', False))}</b>\n• Интервал проверки: <code>{mins} мин.</code>\n• Уведомления: <b>{_state_on(cfg.get('autodump_notifications', True))}</b>\n• Количества для проверки: <code>{qtxt}</code>\n• Пороги демпа: <code>{_h(ftxt)}</code>\n• Последний результат: <code>{_h(last)}</code>\n\nАвтодемп ищет цены конкурентов по вашим количествам ⭐. Если точного количества нет, берёт ближайшее и досчитывает недостающие звёзды. Порог не даёт опустить цену ниже заданной."
def _autodump_kb(chat_id):
    cfg = _get_cfg(chat_id)
    kb = K()
    kb.row(B('📉 Автодемп: ' + ('ВКЛ' if cfg.get('autodump_enabled', False) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_AUTODUMP))
    kb.row(B('⏱ Интервал проверки', callback_data=CBT_AUTODUMP_INTERVAL), B('🔎 Проверить сейчас', callback_data=CBT_AUTODUMP_RUN))
    kb.row(B('🔔 Уведомления: ' + ('ВКЛ' if cfg.get('autodump_notifications', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_AUTODUMP_NOTIFY))
    kb.add(B('◀️ Назад', callback_data=CBT_PRICING))
    return kb
def _open_autodump(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _autodump_text(chat_id), _autodump_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _notifications_text(chat_id):
    cfg = _get_cfg(chat_id)
    return '<b>🔔 Уведомления</b>\n\n' + f"• Изменение цен лотов: <b>{_state_on(cfg.get('price_change_notifications', True))}</b>\n" + f"• Автодемп: <b>{_state_on(cfg.get('autodump_notifications', True))}</b>\n" + f"• Фильтр по балансу: <b>{_state_on(cfg.get('balance_lot_filter_notifications', True))}</b>\n\nУведомления приходят одним сообщением со всеми изменениями. Для автоцен показывается цена без наценки и с наценкой."
def _notifications_kb(chat_id):
    cfg = _get_cfg(chat_id)
    kb = K()
    kb.row(B('💹 Изменения цен: ' + ('ВКЛ' if cfg.get('price_change_notifications', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_PRICE_NOTIFY))
    kb.row(B('📉 Автодемп: ' + ('ВКЛ' if cfg.get('autodump_notifications', True) else 'ВЫКЛ'), callback_data=CBT_TOGGLE_AUTODUMP_NOTIFY))
    kb.add(B('◀️ Назад', callback_data=CBT_MINI_SETTINGS))
    return kb
def _open_notifications(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _notifications_text(chat_id), _notifications_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
_MSG_TITLES = {'purchase_created': 'После покупки (просим ник)', 'username_received': 'Ник получен (уведомление)', 'username_invalid': 'Ник неверный/не найден', 'username_valid': 'Ник верный (подтверждение)', 'sending': 'Отправка звёзд (процесс)', 'sent': 'Отправлено успешно', 'failed': 'Не удалось отправить', 'your_turn': 'Дошла очередь (просим ник)'}
def _messages_text(chat_id):
    tpls = _get_cfg(chat_id).get('templates') or _default_templates()
    pend = _current(chat_id)
    if pend:
        oid = pend.get('order_id') or 'ABC123'
        qty = int(pend.get('qty', 50)) or 150
        uname = pend.get('candidate') or '@username'
        order_url = f'https://funpay.com/orders/{oid}/'
    else:
        oid = 'ABC123'
        qty = 150
        uname = '@username'
        order_url = 'https://funpay.com/orders/ABC123/'
    lines = ['<b>Кастомные сообщения</b>', '', 'Плейсхолдеры (что это и зачем):', '• {qty} — количество звёзд в заказе', '• {username} — ник покупателя (с @), подставится автоматически', '• {order_id} — номер заказа на FunPay', '• {order_url} — ссылка на страницу заказа', '• {reason} — краткая причина ошибки при неудачной отправке', '', 'Текущие значения (пример):', f'qty={qty} username={uname} order_id={oid} order_url={order_url}', '', 'Выберите шаблон ниже, чтобы изменить:']
    for key, title in _MSG_TITLES.items():
        preview = (tpls.get(key) or '').replace('\n', ' ')[:70]
        lines.append(f'• <b>{title}</b>\n{preview}')
    return '\n'.join(lines)
def _messages_kb(chat_id):
    kb = K()
    for key, title in list(_MSG_TITLES.items()):
        kb.row(B(f'✏️ {title}', callback_data=f'{CBT_MSG_EDIT_P}{key}'), B('♻️', callback_data=f'{CBT_MSG_RESET_P}{key}'))
    kb.add(B('◀️ Назад', callback_data=CBT_MINI_SETTINGS))
    return kb
def _open_messages(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _messages_text(chat_id), _messages_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _msg_edit_start(bot, call):
    chat_id = call.message.chat.id
    key = call.data.split(':')[-1]
    _fsm[chat_id] = {'step': 'msg_edit_value', 'msg_key': key}
    pend = _current(chat_id)
    if pend:
        oid = pend.get('order_id') or 'ABC123'
        qty = int(pend.get('qty', 50)) or 150
        uname = pend.get('candidate') or '@username'
        order_url = f'https://funpay.com/orders/{oid}/'
    else:
        oid = 'ABC123'
        qty = 150
        uname = '@username'
        order_url = 'https://funpay.com/orders/ABC123/'
    cfg = _get_cfg(chat_id)
    tpls = cfg.get('templates') or _default_templates()
    cur_text = tpls.get(key, _default_templates().get(key, ''))
    try:
        bot.answer_callback_query(call.id, 'Введите новый текст шаблона. Можно использовать {qty}, {username}, {order_id}, {order_url}, {reason}')
    except Exception:
        pass
    title = _MSG_TITLES.get(key, key)
    text_block = f'Изменение: {title}\n\nДоступные плейсхолдеры:\n{{qty}} {{username}} {{order_id}} {{order_url}} {{reason}}\nТекущие значения (пример):\nqty={qty} username={uname} order_id={oid} order_url={order_url}\n\nТекущий текст шаблона:\n{cur_text}\n\nПришлите новый текст (или /cancel).'
    m = bot.send_message(chat_id, text_block, reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id, {})
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _msg_reset(bot, call):
    chat_id = call.message.chat.id
    key = call.data.split(':')[-1]
    cfg = _get_cfg(chat_id)
    tpls = cfg.get('templates') or {}
    defaults = _default_templates()
    if key in defaults:
        tpls[key] = defaults[key]
        _set_cfg(chat_id, templates=tpls)
    try:
        bot.answer_callback_query(call.id, 'Сброшено по умолчанию')
    except Exception:
        pass
    _open_messages(bot, call)
def _kb_cancel_fsm():
    kb = K()
    kb.add(B('❌ Отменить ввод', callback_data=CBT_FSM_CANCEL))
    return kb
def _fsm_cancel(cardinal, call):
    chat_id = call.message.chat.id
    st = _fsm.pop(chat_id, None) or {}
    _cleanup_fsm_msgs(cardinal.telegram.bot, chat_id, st)
    pmid = st.get('prompt_msg_id')
    _safe_delete(cardinal.telegram.bot, chat_id, pmid)
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Отменено.')
    except Exception:
        pass
    m = cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
    _safe_delete(cardinal.telegram.bot, chat_id, getattr(m, 'message_id', None))
def _looks_like_paid(text):
    t = (text or '').lower()
    return 'оплатил заказ' in t or 'заказ оплачен' in t or 'paid the order' in t or ('order paid' in t)
def _parse_order_info_from_text(text):
    if not text:
        return (None, None)
    oid = None
    m = _re.search('#([A-Z0-9]{6,})', text, _re.I)
    if m:
        oid = m.group(1)
    qty = None
    m = _re.search('(\\d{2,7})\\s*(?:зв[её]зд|stars|⭐)', text, _re.I)
    if m:
        try:
            v = int(m.group(1))
            if v >= 50:
                qty = v
        except Exception:
            pass
    if qty is None:
        for n in _re.findall('\\d{2,7}', text):
            try:
                v = int(n)
                if v >= 50:
                    qty = v
                    break
            except Exception:
                pass
    return (qty, oid)
def _validate_username(u):
    if not u:
        return False
    u = _strip_invisible(u).strip().lstrip('@')
    return bool(_re.fullmatch('[A-Za-z0-9_]{5,32}', u))
def _funpay_is_system_paid_message(text):
    if not text:
        return False
    t = text.lower()
    is_paid = 'оплатил заказ' in t or 'заказ оплачен' in t or 'paid the order' in t or ('order paid' in t)
    in_stars_category = 'telegram, звёзды' in t or 'telegram, звезды' in t or 'telegram, stars' in t
    is_gifts = _is_gift_like_text(t)
    is_account = _mentions_account_login(t)
    return is_paid and in_stars_category and (not is_gifts) and (not is_account)
def _funpay_extract_qty_and_order_id(text):
    qty = None
    oid = None
    try:
        m = _re.search('(?:заказ|order|орд[её]р|№)\\s*#?\\s*([A-Za-z0-9\\-]{6,})', text, _re.IGNORECASE)
        if m:
            oid = m.group(1)
        m2 = _re.search('(\\d+)\\s*(?:зв[её]зд|stars|⭐️|⭐)', text, _re.IGNORECASE)
        if m2:
            qty = int(m2.group(1))
    except Exception:
        pass
    return (qty, oid)
def _is_gift_like_text(text):
    if not text:
        return False
    t = text.lower()
    return any((x in t for x in ('подарок', 'подарком', 'подарки', 'подароч', 'gift', 'в подарок')))
def _mentions_account_login(text):
    if not text:
        return False
    t = text.lower()
    patterns = ['с\\s*заходом\\s*на\\s*аккаунт', 'заход\\s*на\\s*аккаунт', 'вход\\s*(?:в|на)?\\s*аккаунт', 'логин\\s*в\\s*аккаунт', 'login\\s*to\\s*account', 'sign\\s*in\\s*to\\s*account']
    return any((_re.search(p, t) for p in patterns))
def _deactivate_all_star_lots(cardinal, cfg, chat_id, reason='временная ошибка/невалидный заказ'):
    try:
        items = cfg.get('star_lots') or []
        known_ids = _managed_lot_ids_from_cfg(cfg)
        rep = {'ok': [], 'skip': [], 'err': []}
        if _CARDINAL_REF is not None:
            rep = _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, False, known_lot_ids=known_ids)
        for it in items:
            it['active'] = False
        managed_ids = _merge_lot_ids(known_ids, _ids_from_report(rep))
        _set_cfg(chat_id, lots_active=False, star_lots=items, managed_lot_ids=managed_ids, last_auto_deact_reason=reason, last_lot_toggle_report=f'auto_deactivate: {_lot_report_short(rep)}', last_lot_toggle_ts=int(time.time()))
    except Exception as e:
        logger.warning(f'_deactivate_all_star_lots failed: {e}')
def _preview_kb():
    kb = K()
    kb.row(B('✅ Отправить', callback_data=CBT_CONFIRM_SEND), B('🔁 Изменить ник', callback_data=CBT_CHANGE_USERNAME))
    kb.add(B('❌ Отмена', callback_data=CBT_CANCEL_FLOW))
    return kb
def _lots_state_summary(cfg):
    items = cfg.get('star_lots') or []
    if items:
        total = len(items)
        on = sum((1 for it in items if it.get('active')))
        if on == 0:
            return ('🔴 Выключены', False)
        if on == total:
            return ('🟢 Включены', True)
        return ('🟡 Частично', None)
    return ('🟢 Включены' if cfg.get('lots_active') else '🔴 Выключены', bool(cfg.get('lots_active')))
def _format_currency(value, currency):
    try:
        v = float(value)
    except Exception:
        return str(value)
    cur = getattr(currency, 'name', currency)
    cur = (str(cur) or 'RUB').upper()
    if cur in ('RUB', 'RUR', '₽'):
        v = round(v)
        return f'{int(v)}₽'
    return f'{v:.2f} {cur}'
def _parse_number_token(raw):
    try:
        s = _html.unescape(str(raw or '')).strip()
        s = s.replace('\xa0', ' ').replace('\u202f', ' ')
        s = _re.sub('\\s+', '', s)
        s = s.strip('.,')
        if not s or not _re.search('\\d', s):
            return None
        if ',' in s and '.' in s:
            last_comma = s.rfind(',')
            last_dot = s.rfind('.')
            dec = ',' if last_comma > last_dot else '.'
            thou = '.' if dec == ',' else ','
            s = s.replace(thou, '')
            if dec == ',':
                s = s.replace(',', '.')
        elif ',' in s or '.' in s:
            sep = ',' if ',' in s else '.'
            parts = s.split(sep)
            if len(parts) > 2 and all((len(p) == 3 for p in parts[1:])):
                s = ''.join(parts)
            elif len(parts) == 2 and len(parts[1]) == 3 and (len(parts[0]) <= 3):
                s = ''.join(parts)
            else:
                s = sep.join(parts)
                if sep == ',':
                    s = s.replace(',', '.')
        return float(s)
    except Exception:
        return None
def _num(v):
    try:
        if isinstance(v, bool) or v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = _html.unescape(str(v))
        m = _re.search('-?\\d[\\d\\s\\u00a0\\u202f.,]*', s)
        if not m:
            return None
        return _parse_number_token(m.group(0))
    except Exception:
        return None
def _fetch_fragment_prices(jwt):
    headers = {'Accept': 'application/json'}
    if jwt:
        headers['Authorization'] = f'JWT {jwt}'
    last = ''
    for url in FRAGMENT_PRICES_URLS:
        try:
            r = _HTTP.get(url, headers=headers, timeout=20)
            if r.status_code < 400:
                try:
                    return (r.json(), url)
                except Exception:
                    return ({'raw': r.text}, url)
            last = f'{url}: HTTP {r.status_code} {r.text[:160]}'
        except Exception as e:
            last = f'{url}: {e}'
    return (None, last)
def _price_nodes(x):
    if isinstance(x, dict):
        yield x
        for v in x.values():
            yield from _price_nodes(v)
    elif isinstance(x, (list, tuple)):
        for v in x:
            yield from _price_nodes(v)
def _qty_from_node(d):
    for k in ('quantity', 'qty', 'stars', 'count', 'amount_stars', 'stars_count'):
        v = _num(d.get(k))
        if v and v > 0:
            return v
    return None
def _fragment_unit_price(data, currency):
    cur = _normalize_stars_currency(currency)
    best = None
    for d in _price_nodes(data):
        q = _qty_from_node(d)
        vals = []
        for k, v in d.items():
            lk = str(k).lower()
            if 'fee' in lk or 'commission' in lk:
                continue
            n = _num(v)
            if n is None or n <= 0:
                continue
            if cur == FTS_CURRENCY_USDT_TON and ('usdt' in lk and 'price' in lk or (str(d.get('currency', '')).lower() in ('usdt', 'usdt_ton') and lk in ('price', 'amount', 'cost', 'total'))):
                vals.append(n)
            if cur == FTS_CURRENCY_TON and ('ton' in lk and 'price' in lk and ('usdt' not in lk) or (str(d.get('currency', '')).lower() == 'ton' and lk in ('price', 'amount', 'cost', 'total'))):
                vals.append(n)
        for price in vals:
            unit = price / q if q else price
            if unit > 0 and (best is None or unit < best):
                best = unit
    return best
def _tonapi_rates_rub():
    hdr = {'Accept': 'application/json'}
    if TONAPI_KEY:
        hdr['Authorization'] = f'Bearer {TONAPI_KEY}'
    out = {'ton': None, 'usdt': None}
    def val(x):
        n = _num(x)
        return float(n) if n is not None and n > 0 else None
    def usd_rub():
        for url, params, path in (('https://api.coingecko.com/api/v3/simple/price', {'ids': 'tether', 'vs_currencies': 'rub'}, ('tether', 'rub')), ('https://open.er-api.com/v6/latest/USD', None, ('rates', 'RUB'))):
            try:
                r = _HTTP.get(url, params=params, headers={'Accept': 'application/json'}, timeout=10)
                data = r.json()
                cur = data
                for k in path:
                    cur = cur.get(k) if isinstance(cur, dict) else None
                n = val(cur)
                if n:
                    return n
            except Exception:
                pass
        return None
    def from_rates(data, aliases):
        rr = data.get('rates') if isinstance(data, dict) else None
        if isinstance(rr, dict):
            for key, node in rr.items():
                blob = str(key).lower()
                if not any((a in blob for a in aliases)):
                    continue
                prices = node.get('prices') if isinstance(node, dict) else {}
                if isinstance(prices, dict):
                    rub = val(prices.get('RUB') or prices.get('rub') or prices.get('RUR') or prices.get('rur'))
                    usd = val(prices.get('USD') or prices.get('usd'))
                    if rub:
                        return rub
                    ur = usd_rub()
                    if usd and ur:
                        return usd * ur
        return None
    for tokens in (f'{TONAPI_TON_TOKEN},{TONAPI_USDT_TOKEN}', 'ton', TONAPI_TON_TOKEN, TONAPI_USDT_TOKEN):
        try:
            r = _HTTP.get(TONAPI_RATES_URL, params={'tokens': tokens, 'currencies': 'rub,usd'}, headers=hdr, timeout=15)
            if r.status_code >= 400:
                continue
            data = r.json()
            out['ton'] = out['ton'] or from_rates(data, ('ton', 'toncoin'))
            out['usdt'] = out['usdt'] or from_rates(data, ('usdt', 'tether'))
        except Exception as e:
            logger.debug(f'TonAPI rates attempt failed: {e}')
    if not out['ton'] or not out['usdt']:
        try:
            r = _HTTP.get('https://api.coingecko.com/api/v3/simple/price', params={'ids': 'the-open-network,tether', 'vs_currencies': 'rub'}, headers={'Accept': 'application/json'}, timeout=10)
            data = r.json()
            out['ton'] = out['ton'] or val((data.get('the-open-network') or {}).get('rub'))
            out['usdt'] = out['usdt'] or val((data.get('tether') or {}).get('rub'))
        except Exception as e:
            logger.debug(f'CoinGecko fallback failed: {e}')
    out['usdt'] = out['usdt'] or usd_rub()
    return {k: v for k, v in out.items() if v}
def _auto_unit_price_rub(cfg):
    data, src = _fetch_fragment_prices(cfg.get('fragment_jwt'))
    if data is None:
        return (None, f'Не удалось получить Fragment Prices: {src}', None)
    cur = _normalize_stars_currency(cfg.get('stars_currency'))
    unit = _fragment_unit_price(data, cur)
    if not unit:
        return (None, 'Не смог найти цену звёзд в ответе Fragment Prices. Проверьте FRAGMENT_PRICES_URL.', None)
    rates = _tonapi_rates_rub()
    rub = rates.get('usdt' if cur == FTS_CURRENCY_USDT_TON else 'ton')
    if not rub:
        return (None, 'Не удалось получить курс RUB через TonAPI и резервные источники. Проверьте интернет/FTS_TONAPI_KEY или задайте цену за 1⭐ вручную.', None)
    base = float(unit) * float(rub)
    markup_percent = float(cfg.get('markup_percent') or 0.0)
    final = base * (1.0 + markup_percent / 100.0)
    return (final, f'без наценки: {base:.6f} RUB за 1⭐; с наценкой {markup_percent:g}%: {final:.6f} RUB за 1⭐ ({unit:g} {_stars_currency_label(cur)} × {rub:g} RUB)', base)
def _price_changes_text(rows, title):
    out = [f'<b>{title}</b>']
    for r in rows[:60]:
        diff = r.get('diff', 0)
        sign = '+' if float(diff or 0) >= 0 else ''
        line = f"• LOT <code>{r['lot_id']}</code> — <b>{r.get('qty') or '?'}⭐</b>: <s>{_format_currency(r['old_price'], r['currency'])}</s> → <b>{_format_currency(r['new_price'], r['currency'])}</b> ({sign}{_format_currency(diff, r['currency'])})"
        if r.get('base_price') is not None and abs(float(r.get('base_price') or 0) - float(r.get('new_price') or 0)) >= 0.01:
            line += f"\n  └ без наценки: <code>{_format_currency(r['base_price'], r['currency'])}</code>; с наценкой: <b>{_format_currency(r['new_price'], r['currency'])}</b>"
        out.append(line)
    if len(rows) > 60:
        out.append(f'… и ещё {len(rows) - 60} лот(ов)')
    return '\n'.join(out)
def _notify_price_changes(cardinal, chat_id, rows, title):
    if rows and _cfg_bool(_get_cfg(chat_id), 'price_change_notifications', True):
        try:
            cardinal.telegram.bot.send_message(chat_id, _price_changes_text(rows, title), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'price notify failed: {e}')
def _apply_auto_prices(cardinal, chat_id, manual=False):
    cfg = _get_cfg(chat_id)
    unit, info, base_unit = _auto_unit_price_rub(cfg)
    if not unit:
        return (False, f'⚠️ Автообновление не удалось.\n{_h(info)}')
    rows, skipped = _collect_unit_price_targets(cardinal, cfg, unit, base_unit)
    if not rows:
        return (False, f'⚠️ Лотов для обновления не найдено. Пропущено: {skipped}')
    rep = _apply_markup_prices(cardinal, rows)
    _set_cfg(chat_id, unit_star_price=round(unit, 6), last_auto_price_base_unit=round(base_unit, 6) if isinstance(base_unit, (int, float)) else None, last_auto_price_info=info, last_auto_price_ts=int(time.time()))
    _notify_price_changes(cardinal, chat_id, rows, '💹 Цены лотов обновлены автоматически')
    return (True, f"✅ Автоцены применены: обновлено {len(rep['ok'])} из {len(rows)}.\n<code>{_h(info)}</code>")
def _maybe_auto_price_update(cardinal, chat_id, force=False):
    cfg = _get_cfg(chat_id)
    if not force and (not _cfg_bool(cfg, 'auto_price_fragment_enabled', False)):
        return False
    last = int(float(cfg.get('last_auto_price_ts') or 0))
    if not force and time.time() - last < FTS_AUTO_PRICE_INTERVAL_SEC:
        return False
    ok, msg = _apply_auto_prices(cardinal, chat_id, manual=False)
    if not ok:
        logger.warning(msg.replace('\n', ' '))
    return ok
def _cb_auto_price_fragment(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Считаю цены по Fragment/TonAPI…')
    except Exception:
        pass
    ok, msg = _apply_auto_prices(cardinal, chat_id, manual=True)
    bot.send_message(chat_id, msg, parse_mode='HTML')
    try:
        _open_pricing(bot, call)
    except Exception:
        pass
def _iter_lot_like(x):
    if x is None:
        return
    if isinstance(x, dict):
        if any((k in x for k in ('price', 'cost', 'amount', 'price_rub', 'title', 'name', 'tc_price'))):
            yield x
        for v in x.values():
            if isinstance(v, (list, tuple, dict, set)):
                yield from _iter_lot_like(v)
    elif isinstance(x, (list, tuple, set)):
        for v in x:
            yield from _iter_lot_like(v)
    else:
        yield x
def _lot_attr(o, names, default=None):
    if isinstance(o, dict):
        return next((o.get(n) for n in names if n in o), default)
    for n in names:
        if hasattr(o, n):
            try:
                return getattr(o, n)
            except Exception:
                pass
    return default
def _lot_url(o):
    val = _lot_attr(o, ('url', 'link', 'href', 'public_url', 'lot_url', 'offer_url'), None)
    if val:
        return str(val)
    lid = _lot_attr(o, ('id', 'lot_id', 'offer_id'), None)
    try:
        if lid:
            return f'https://funpay.com/lots/offer?id={int(lid)}'
    except Exception:
        pass
    return None
def _lot_price_qty(o):
    price = _num(_lot_attr(o, ('price', 'cost', 'amount', 'price_rub', 'tc_price', 'total')))
    if isinstance(_lot_attr(o, ('price',), None), dict):
        price = _balance_from_node(_lot_attr(o, ('price',), None))
    cur = _lot_attr(o, ('currency', 'cur'), 'RUB')
    qty = _num(_lot_attr(o, ('quantity', 'qty', 'stars', 'count', 'amount_stars', 'stars_count'), None))
    title = ' '.join((str(_lot_attr(o, (n,), '')) for n in ('title', 'name', 'description', 'short_description', 'summary')))
    qty = qty or _extract_qty_from_title(title)
    return (float(price) if price is not None else None, int(qty) if qty else None, getattr(cur, 'name', str(cur)) or 'RUB')
def _html_lot_id_from_href(href):
    if not href:
        return None
    try:
        m = _re.search('(?:id=|offer=|/)(\\d{2,})', str(href))
        return int(m.group(1)) if m else None
    except Exception:
        return None
def _extract_price_from_lot_html(chunk, plain=None):
    plain = plain if plain is not None else _html.unescape(_re.sub('<[^>]+>', ' ', chunk or ''))
    patterns = ('(?:data-s|data-price|data-cost|data-value)=["\\\']([^"\\\']+)["\\\']', 'class=["\\\'][^"\\\']*(?:tc-price|price)[^"\\\']*["\\\'][^>]*>(.{0,160}?)(?:</|$)', '(\\d[\\d\\s\\u00a0\\u202f.,]{0,16})\\s*(?:₽|руб\\.?|RUB)\\b')
    for pat in patterns:
        vals = _re.findall(pat, chunk or plain, _re.I | _re.S)
        for val in reversed(vals):
            v = _num(_html.unescape(_re.sub('<[^>]+>', ' ', str(val))))
            if v is not None and v > 0:
                return float(v)
    return None
def _funpay_http_lots(cardinal, category_id):
    sess = _HTTP
    acc = getattr(cardinal, 'account', None)
    for s in [getattr(obj, n, None) for obj in (acc, cardinal) if obj for n in ('session', '_session', 'requests', 'requester')]:
        if hasattr(s, 'get'):
            sess = s
    lots = []
    urls = (f'https://funpay.com/lots/{int(category_id)}/', f'https://funpay.com/chips/{int(category_id)}/')
    for url in urls:
        try:
            r = sess.get(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html', 'Accept-Language': 'ru,en;q=0.8'}, timeout=20)
            html = getattr(r, 'text', '') or ''
            status = int(getattr(r, 'status_code', 200) or 200)
            if not html or status >= 400:
                logger.debug(f'FunPay HTML lots skipped {url}: status={status} html_len={len(html)}')
                continue
            chunks = _re.split('(?=<[^>]+class=["\\\'][^"\\\']*(?:tc-item|offer|lot|tc-service)[^"\\\']*["\\\'])', html) or []
            if len(chunks) < 2:
                chunks = _re.findall('(?is)<a[^>]+href=["\\\'][^"\\\']*(?:lots|chips|offers|orders)[^"\\\']+["\\\'].{0,9000}?</a>', html)
            for ch in chunks:
                plain = _html.unescape(_re.sub('<[^>]+>', ' ', ch))
                qty = _extract_qty_from_title(plain)
                if not qty:
                    continue
                val = _extract_price_from_lot_html(ch, plain)
                href = None
                hm = _re.search('href=["\\\']([^"\\\']+)["\\\']', ch, _re.I)
                if hm:
                    href = _html.unescape(hm.group(1))
                    if href.startswith('/'):
                        href = 'https://funpay.com' + href
                lid = _html_lot_id_from_href(href)
                if val is not None and val > 0:
                    lots.append({'title': plain, 'qty': int(qty), 'price': float(val), 'currency': 'RUB', 'id': lid, 'url': href, 'source': 'html'})
        except Exception as e:
            logger.debug(f'FunPay HTML lots failed {url}: {e}')
    logger.debug(f'FunPay HTML lots parsed: category={category_id} count={len(lots)}')
    return lots
def _public_category_lots(cardinal, category_id):
    acc = getattr(cardinal, 'account', None)
    objs = [acc, cardinal, getattr(cardinal, 'profile', None), getattr(cardinal, 'tg_profile', None)]
    lots = []
    names = ('get_subcategory_public_lots', 'get_public_subcategory_lots', 'get_public_lots', 'get_category_lots', 'get_lots_by_subcategory', 'get_subcategory_lots', 'get_lots', 'get_offers', 'get_sorted_lots')
    for obj in objs:
        if not obj:
            continue
        for name in names:
            fn = getattr(obj, name, None)
            if not callable(fn):
                continue
            for args in ((int(category_id),), ('lot', int(category_id)), (int(category_id), 'lot'), (int(category_id), 1), (1, int(category_id)), ()):
                try:
                    res = fn(*args)
                    if res:
                        lots.extend(list(_iter_lot_like(res)))
                except Exception:
                    continue
    try:
        lots.extend(_funpay_http_lots(cardinal, category_id))
    except Exception as e:
        logger.debug(f'FunPay HTML fallback unavailable: {e}')
    return lots
def _competitor_star_prices(cardinal, cfg, return_debug=False):
    qtys = sorted({int(x.get('qty')) for x in cfg.get('star_lots') or [] if x.get('qty')})
    my = {int(x.get('lot_id')) for x in cfg.get('star_lots') or [] if x.get('lot_id')}
    cand = []
    out = {}
    dbg = {'target_qtys': qtys, 'raw_lots': 0, 'self_skipped': 0, 'bad_price_or_qty': 0, 'candidates': 0, 'matched_qtys': []}
    if not qtys:
        return (out, dbg) if return_debug else out
    raw_lots = _public_category_lots(cardinal, int(cfg.get('category_id', FNP_STARS_CATEGORY_ID)))
    dbg['raw_lots'] = len(raw_lots or [])
    seen = set()
    for lot in raw_lots:
        try:
            lid = _lot_attr(lot, ('id', 'lot_id', 'offer_id'))
            if lid is not None and int(lid) in my:
                dbg['self_skipped'] += 1
                continue
            price, qty, cur = _lot_price_qty(lot)
            if price is None or not qty or qty <= 0:
                dbg['bad_price_or_qty'] += 1
                continue
            key = (int(lid) if lid is not None else None, int(qty), round(float(price), 4), str(_lot_url(lot) or ''))
            if key in seen:
                continue
            seen.add(key)
            cand.append({'price': float(price), 'qty': int(qty), 'currency': cur, 'lot_id': lid, 'url': _lot_url(lot)})
        except Exception:
            dbg['bad_price_or_qty'] += 1
            continue
    dbg['candidates'] = len(cand)
    byq = {}
    for c in cand:
        byq.setdefault(c['qty'], []).append(c)
    for target in qtys:
        pool = byq.get(target)
        approx = False
        src = None
        if not pool:
            lower = [q for q in byq if q <= target]
            src = max(lower) if lower else min(byq.keys(), key=lambda q: abs(q - target)) if byq else None
            pool = byq.get(src, []) if src else []
            approx = True
        if not pool:
            continue
        best = None
        for c in pool:
            add = max(0, int(target) - int(c['qty']))
            unit = c['price'] / max(1, c['qty'])
            est = c['price'] if c['qty'] == target else c['price'] + unit * add if c['qty'] <= target else unit * target
            if best is None or est < best['price']:
                best = {**c, 'price': float(est), 'source_price': c['price'], 'source_qty': c['qty'], 'added_qty': add, 'approx': approx or c['qty'] != target}
        if best:
            out[target] = best
    dbg['matched_qtys'] = sorted(out.keys())
    logger.info(f"[AUTODUMP] public lots raw={dbg['raw_lots']} candidates={dbg['candidates']} self_skipped={dbg['self_skipped']} bad={dbg['bad_price_or_qty']} targets={qtys} matched={dbg['matched_qtys']}")
    return (out, dbg) if return_debug else out
def _autodump_target_price(price, currency):
    cur = getattr(currency, 'name', str(currency)).upper()
    step = FTS_AUTODUMP_STEP_RUB if cur in ('RUB', 'RUR', '₽') else 0.01
    return max(step, round(float(price) - step, 2))
def _autodump_changes_text(rows, title):
    out = [f'<b>{title}</b>']
    for r in rows[:60]:
        link = ''
        if r.get('competitor_url'):
            link = f''' — <a href="{_html.escape(str(r.get('competitor_url')), quote=True)}">лот конкурента</a>'''
        extra = ''
        if r.get('approx'):
            extra = f" (расчёт от {r.get('source_qty')}⭐: {_format_currency(r.get('source_price'), r['currency'])}"
            if int(r.get('added_qty') or 0) > 0:
                extra += f" + {int(r.get('added_qty'))}⭐"
            extra += ')'
        floor = f"; порог <code>{_format_currency(r['floor'], r['currency'])}</code>" if r.get('floor') is not None else ''
        decision = f"\n  └ {_h(r.get('decision'))}" if r.get('decision') else ''
        out.append(f"• {r.get('qty')}⭐ LOT <code>{r['lot_id']}</code>: конкурент <code>{_format_currency(r['competitor_price'], r['currency'])}</code>{extra}{link}{floor}; <s>{_format_currency(r['old_price'], r['currency'])}</s> → <b>{_format_currency(r['new_price'], r['currency'])}</b>{decision}")
    if len(rows) > 60:
        out.append(f'… и ещё {len(rows) - 60} лот(ов)')
    return '\n'.join(out)
def _notify_autodump_changes(cardinal, chat_id, rows, title):
    if rows and _cfg_bool(_get_cfg(chat_id), 'autodump_notifications', True):
        try:
            cardinal.telegram.bot.send_message(chat_id, _autodump_changes_text(rows, title), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'autodump notify failed: {e}')
def _apply_autodump(cardinal, chat_id, manual=False):
    cfg = _get_cfg(chat_id)
    comp, dbg = _competitor_star_prices(cardinal, cfg, return_debug=True)
    if not comp:
        info = f"лоты={dbg.get('raw_lots', 0)}, кандидаты={dbg.get('candidates', 0)}, свои={dbg.get('self_skipped', 0)}, без цены/кол-ва={dbg.get('bad_price_or_qty', 0)}, количества={','.join(map(str, dbg.get('target_qtys') or [])) or '—'}"
        _set_cfg(chat_id, last_autodump_ts=int(time.time()), last_autodump_info='цены конкурентов не найдены: ' + info)
        return (False, f'⚠️ Автодемп не нашёл подходящие цены конкурентов.\nДиагностика: <code>{_h(info)}</code>\nПлагин проверил методы Cardinal/FunPayAPI и HTML-страницу FunPay. Если в категории реально есть конкуренты, проверьте доступность публичной страницы FunPay из сервера и правильность категории звёзд.')
    rows = []
    fair_unit = fair_base = fair_info = None
    tried_fair = False
    for it in cfg.get('star_lots') or []:
        try:
            qty = int(it.get('qty'))
            lot_id = int(it.get('lot_id'))
            c = comp.get(qty)
            if not c:
                continue
            old, cur = _get_lot_price_currency(cardinal, lot_id)
            if old is None:
                continue
            target = _autodump_target_price(c['price'], cur)
            floor = _num(it.get('autodump_min_price'))
            decision = ''
            if floor is not None and floor > 0 and (target < floor):
                target = float(floor)
                decision = 'сработал порог демпа'
            if target > float(old) + 0.01:
                if not tried_fair:
                    fair_unit, fair_info, fair_base = _auto_unit_price_rub(cfg)
                    tried_fair = True
                if fair_unit:
                    fair_price = float(fair_unit) * qty
                    cur_name = getattr(cur, 'name', str(cur)).upper()
                    fair_price = float(int(round(fair_price))) if cur_name in ('RUB', 'RUR', '₽') else round(fair_price, 2)
                    if fair_price > float(old) + 0.01:
                        step = FTS_AUTODUMP_RAISE_STEP_RUB if cur_name in ('RUB', 'RUR', '₽') else 0.01
                        target = min(float(target), float(fair_price), float(old) + float(step))
                        decision = f'конкурент выше; курс/наценка нормальные, осторожно поднимаю до {_format_currency(target, cur)}'
                    else:
                        continue
                else:
                    continue
            if abs(float(target) - float(old)) < 0.01:
                continue
            rows.append({'lot_id': lot_id, 'qty': qty, 'currency': cur, 'old_price': old, 'new_price': target, 'diff': round(target - old, 2), 'competitor_price': c['price'], 'source_qty': c.get('source_qty'), 'source_price': c.get('source_price'), 'added_qty': c.get('added_qty'), 'competitor_url': c.get('url'), 'competitor_lot_id': c.get('lot_id'), 'approx': c.get('approx'), 'floor': floor, 'decision': decision})
        except Exception:
            continue
    if not rows:
        _set_cfg(chat_id, last_autodump_ts=int(time.time()), last_autodump_info='изменений нет')
        return (True, '✅ Автодемп проверил конкурентов: цены менять не нужно.')
    rep = _apply_markup_prices(cardinal, rows)
    _set_cfg(chat_id, last_autodump_ts=int(time.time()), last_autodump_info=f"обновлено {len(rep['ok'])}/{len(rows)}")
    _notify_autodump_changes(cardinal, chat_id, rows, '📉 Автодемп изменил цены')
    return (True, f"✅ Автодемп применён: обновлено {len(rep['ok'])} из {len(rows)}.")
def _maybe_autodump_update(cardinal, chat_id, force=False):
    cfg = _get_cfg(chat_id)
    if not force and (not _cfg_bool(cfg, 'autodump_enabled', False)):
        return False
    last = int(float(cfg.get('last_autodump_ts') or 0))
    interval = max(600, min(86400, int(float(cfg.get('autodump_interval_sec') or FTS_AUTODUMP_DEFAULT_INTERVAL_SEC))))
    if not force and time.time() - last < interval:
        return False
    ok, msg = _apply_autodump(cardinal, chat_id, manual=force)
    if not ok:
        logger.warning(msg.replace('\n', ' '))
    return ok
def _collect_markup_targets(cardinal, cfg, percent):
    targets = []
    star_lots = cfg.get('star_lots') or []
    lot_ids = []
    qty_map = {}
    if star_lots:
        for it in star_lots:
            try:
                lot_id = int(it.get('lot_id'))
                lot_ids.append(lot_id)
                qty_map[lot_id] = int(it.get('qty')) if it.get('qty') else None
            except Exception:
                continue
    else:
        lots = _get_my_lots_by_category(cardinal, FNP_STARS_CATEGORY_ID)
        lot_ids = [int(lid) for lid in lots.keys()]
    seen = set()
    for lot_id in lot_ids:
        if lot_id in seen:
            continue
        seen.add(lot_id)
        try:
            if not _is_stars_lot(cardinal, lot_id):
                continue
            fields = cardinal.account.get_lot_fields(int(lot_id))
            if not fields:
                continue
            title = getattr(fields, 'title', None) or getattr(fields, 'name', None) or ''
            old_price = None
            for price_attr in ('price', 'cost', 'amount', 'price_rub'):
                if hasattr(fields, price_attr):
                    try:
                        old_price = float(getattr(fields, price_attr))
                        break
                    except Exception:
                        pass
            if old_price is None:
                continue
            currency = getattr(fields, 'currency', None) or getattr(fields, 'cur', None) or 'RUB'
            qty = qty_map.get(lot_id)
            if qty is None:
                q = _extract_qty_from_title(title)
                qty = int(q) if q else None
            new_price = round(old_price * (1.0 + percent / 100.0), 2)
            if getattr(currency, 'name', str(currency)).upper() in ('RUB', 'RUR', '₽'):
                new_price = float(int(round(new_price)))
            targets.append({'lot_id': lot_id, 'title': title, 'qty': qty, 'currency': currency, 'old_price': old_price, 'new_price': new_price, 'diff': round(new_price - old_price, 2)})
        except Exception as e:
            logger.debug(f'_collect_markup_targets: lot {lot_id} skipped: {e}')
            continue
    return targets
def _collect_reset_markup_targets(cardinal, cfg, percent):
    if abs(float(percent or 0.0)) < 1e-12:
        return []
    rows = []
    star_lots = cfg.get('star_lots') or []
    if star_lots:
        lot_ids = [int(it.get('lot_id')) for it in star_lots if it.get('lot_id')]
    else:
        lots = _get_my_lots_by_category(cardinal, FNP_STARS_CATEGORY_ID) if cardinal else {}
        lot_ids = [int(lid) for lid in lots.keys()]
    seen = set()
    for lot_id in lot_ids:
        if lot_id in seen:
            continue
        seen.add(lot_id)
        try:
            if _CARDINAL_REF and (not _is_stars_lot(_CARDINAL_REF, lot_id)):
                continue
            fields = _CARDINAL_REF.account.get_lot_fields(lot_id) if _CARDINAL_REF else None
            if not fields:
                continue
            cur_price = None
            for price_attr in ('price', 'cost', 'amount', 'price_rub'):
                if hasattr(fields, price_attr):
                    try:
                        cur_price = float(getattr(fields, price_attr))
                        break
                    except Exception:
                        pass
            if cur_price is None:
                continue
            currency = getattr(fields, 'currency', None) or getattr(fields, 'cur', None) or 'RUB'
            title = getattr(fields, 'title', None) or getattr(fields, 'name', None) or ''
            qty = _extract_qty_from_title(title)
            factor = 1.0 + float(percent) / 100.0
            new_price = cur_price / factor
            curr_name = getattr(currency, 'name', str(currency)).upper()
            new_price = float(int(round(new_price))) if curr_name in ('RUB', 'RUR', '₽') else round(new_price, 2)
            rows.append({'lot_id': lot_id, 'title': title, 'qty': qty, 'currency': currency, 'old_price': cur_price, 'new_price': new_price, 'diff': round(new_price - cur_price, 2)})
        except Exception:
            continue
    return rows
def _cb_markup_reset(cardinal, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    try:
        cardinal.telegram.bot.answer_callback_query(call.id)
    except Exception:
        pass
    p = float(cfg.get('markup_percent') or 0.0)
    if abs(p) < 1e-12:
        cardinal.telegram.bot.send_message(chat_id, 'ℹ️ Наценка уже 0%. Нечего сбрасывать.')
        return
    if _CARDINAL_REF is None:
        cardinal.telegram.bot.send_message(chat_id, '⚠️ Внутренняя ошибка: нет ссылки на Cardinal.')
        return
    rows = _collect_reset_markup_targets(cardinal, cfg, p)
    if not rows:
        cardinal.telegram.bot.send_message(chat_id, 'Не нашёл лотов для отката наценки.')
        return
    rep = _apply_markup_prices(cardinal, rows)
    okn = len(rep['ok'])
    ern = len(rep['err'])
    total = len(rows)
    _set_cfg(chat_id, markup_percent=0.0)
    _notify_price_changes(cardinal, chat_id, rows, '💹 Наценка сброшена, цены изменены')
    msg = f'✅ Сброс наценки выполнен: обновлено {okn} из {total} лот(ов).'
    if ern:
        msg += f'\n⚠️ Ошибок: {ern}. См. логи.'
    cardinal.telegram.bot.send_message(chat_id, msg)
def _markup_preview_text(percent, rows):
    lines = [f'<b>Наценка: {percent}%</b>']
    if not rows:
        lines.append('Лотов не найдено.')
        return '\n'.join(lines)
    total_old = 0.0
    total_new = 0.0
    lines.append('')
    for r in rows[:20]:
        lot_id = r['lot_id']
        qty = r.get('qty')
        cur = r.get('currency')
        oldp = r['old_price']
        newp = r['new_price']
        diff = r['diff']
        total_old += float(oldp)
        total_new += float(newp)
        qty_part = f'{qty}⭐ — ' if qty else ''
        lines.append(f'• LOT <code>{lot_id}</code> — {qty_part}{_format_currency(oldp, cur)} → <b>{_format_currency(newp, cur)}</b> (+{_format_currency(diff, cur)})')
    more = len(rows) - 20
    if more > 0:
        lines.append(f'… и ещё {more} лот(ов)')
    lines.append('')
    lines.append(f"Итого: {_format_currency(total_old, rows[0]['currency'] if rows else 'RUB')} → <b>{_format_currency(total_new, rows[0]['currency'] if rows else 'RUB')}</b> (+{_format_currency(total_new - total_old, rows[0]['currency'] if rows else 'RUB')})")
    lines.append('')
    lines.append('Подтвердите наценку или измените процент.')
    return '\n'.join(lines)
def _kb_markup_preview():
    kb = K()
    kb.row(B('✅ Подтвердить', callback_data=CBT_MARKUP_APPLY), B('✏️ Изменить %', callback_data=CBT_MARKUP_CHANGE))
    kb.add(B('❌ Отмена', callback_data=CBT_FSM_CANCEL))
    return kb
def _apply_markup_prices(cardinal, rows):
    rep = {'ok': [], 'err': []}
    for r in rows:
        lot_id = int(r['lot_id'])
        new_price = float(r['new_price'])
        try:
            if not _is_stars_lot(cardinal, lot_id):
                rep['err'].append(lot_id)
                continue
            fields = cardinal.account.get_lot_fields(lot_id)
            if not fields:
                rep['err'].append(lot_id)
                continue
            set_ok = False
            for price_attr in ('price', 'cost', 'amount', 'price_rub'):
                if hasattr(fields, price_attr):
                    try:
                        setattr(fields, price_attr, new_price)
                        set_ok = True
                        break
                    except Exception:
                        pass
            if not set_ok:
                rep['err'].append(lot_id)
                continue
            cardinal.account.save_lot(fields)
            rep['ok'].append(lot_id)
        except Exception as e:
            logger.warning(f'_apply_markup_prices {lot_id} failed: {e}')
            rep['err'].append(lot_id)
    return rep
def _get_lot_price_currency(cardinal, lot_id):
    try:
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return (None, 'RUB')
        price = None
        for price_attr in ('price', 'cost', 'amount', 'price_rub'):
            if hasattr(fields, price_attr):
                try:
                    price = float(getattr(fields, price_attr))
                    break
                except Exception:
                    pass
        currency = getattr(fields, 'currency', None) or getattr(fields, 'cur', None) or 'RUB'
        return (price, getattr(currency, 'name', str(currency)) or 'RUB')
    except Exception:
        return (None, 'RUB')
def _collect_unit_price_targets(cardinal, cfg, unit_price, base_unit_price=None):
    rows = []
    skipped = 0
    star_lots = cfg.get('star_lots') or []
    if star_lots:
        lot_ids = []
        qty_map = {}
        for it in star_lots:
            try:
                lid = int(it.get('lot_id'))
                lot_ids.append(lid)
                qty_map[lid] = int(it.get('qty')) if it.get('qty') else None
            except Exception:
                continue
    else:
        lot_ids = list(_get_my_lots_by_category(cardinal, FNP_STARS_CATEGORY_ID).keys())
        qty_map = {}
    seen = set()
    for lot_id in lot_ids:
        if lot_id in seen:
            continue
        seen.add(lot_id)
        try:
            if not _is_stars_lot(cardinal, int(lot_id)):
                skipped += 1
                continue
            fields = cardinal.account.get_lot_fields(int(lot_id))
            if not fields:
                skipped += 1
                continue
            title = getattr(fields, 'title', None) or getattr(fields, 'name', None) or ''
            old_price = None
            for price_attr in ('price', 'cost', 'amount', 'price_rub'):
                if hasattr(fields, price_attr):
                    try:
                        old_price = float(getattr(fields, price_attr))
                        break
                    except Exception:
                        pass
            if old_price is None:
                skipped += 1
                continue
            currency = getattr(fields, 'currency', None) or getattr(fields, 'cur', None) or 'RUB'
            qty = qty_map.get(int(lot_id))
            if qty is None:
                q = _extract_qty_from_title(title)
                qty = int(q) if q else None
            if not qty or int(qty) <= 0:
                skipped += 1
                continue
            base_price = float(base_unit_price) * float(qty) if base_unit_price is not None else None
            new_price = float(unit_price) * float(qty)
            curr_name = getattr(currency, 'name', str(currency)).upper()
            if curr_name in ('RUB', 'RUR', '₽'):
                if base_price is not None:
                    base_price = float(int(round(base_price)))
                new_price = float(int(round(new_price)))
            else:
                if base_price is not None:
                    base_price = round(base_price, 2)
                new_price = round(new_price, 2)
            rows.append({'lot_id': int(lot_id), 'title': title, 'qty': int(qty), 'currency': currency, 'old_price': float(old_price), 'new_price': float(new_price), 'diff': round(float(new_price) - float(old_price), 2), 'base_price': base_price})
        except Exception:
            skipped += 1
            continue
    return (rows, skipped)
def _unit_price_preview_text(unit_price, rows, skipped):
    lines = [f'<b>Цена за 1⭐: {unit_price}</b>', '']
    if not rows:
        lines.append('Лотов для пересчёта не найдено.')
        if skipped:
            lines.append(f'Пропущено: <b>{skipped}</b>')
        return '\n'.join(lines)
    lines.append('<b>Предпросмотр (первые 20):</b>')
    lines.append('')
    currencies = {getattr(r['currency'], 'name', str(r['currency'])) for r in rows}
    can_total = len(currencies) == 1
    total_old = 0.0
    total_new = 0.0
    for r in rows[:20]:
        lot_id = r['lot_id']
        qty = r.get('qty')
        cur = r.get('currency')
        oldp = r['old_price']
        newp = r['new_price']
        diff = r['diff']
        if can_total:
            total_old += float(oldp)
            total_new += float(newp)
        lines.append(f"• LOT <code>{lot_id}</code> — <b>{qty}⭐</b>: {_format_currency(oldp, cur)} → <b>{_format_currency(newp, cur)}</b> ({('+' if diff >= 0 else '')}{_format_currency(diff, cur)})")
    more = len(rows) - 20
    if more > 0:
        lines.append(f'… и ещё {more} лот(ов)')
    if skipped:
        lines.append('')
        lines.append(f'Пропущено: <b>{skipped}</b> (нет qty/цены или не звёздная категория)')
    if can_total and rows:
        lines.append('')
        lines.append(f"Итого: {_format_currency(total_old, rows[0]['currency'])} → <b>{_format_currency(total_new, rows[0]['currency'])}</b>")
    lines.append('')
    lines.append('Применить новые цены ко всем лотам по цене за 1⭐?')
    return '\n'.join(lines)
def _kb_unit_price_preview():
    kb = K()
    kb.row(B('✅ Применить', callback_data=CBT_UNIT_PRICE_APPLY), B('✏️ Изменить цену 1⭐', callback_data=CBT_UNIT_PRICE_CHANGE))
    kb.add(B('❌ Отмена', callback_data=CBT_FSM_CANCEL))
    return kb
def _start_unit_price(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {'step': 'unit_star_price_value'}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    cur = _get_cfg(chat_id).get('unit_star_price')
    cur_txt = f'{cur}' if isinstance(cur, (int, float)) else '—'
    m = bot.send_message(chat_id, f'Введите <b>цену за 1⭐</b> (число). Сейчас: <b>{cur_txt}</b>\nПример: 1.25\n(или /cancel)', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _cb_unit_price_change(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Введите новую цену за 1⭐ сообщением ниже.')
    except Exception:
        pass
    st = _fsm.get(chat_id) or {}
    st['step'] = 'unit_star_price_value'
    _fsm[chat_id] = st
    m = cardinal.telegram.bot.send_message(chat_id, 'Введите новую цену за 1⭐ (или /cancel):', reply_markup=_kb_cancel_fsm())
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _cb_unit_price_apply(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Применяю…')
    except Exception:
        pass
    st = _fsm.get(chat_id) or {}
    rows = st.get('unit_rows')
    unit_price = st.get('unit_price')
    if not rows or unit_price is None:
        cardinal.telegram.bot.send_message(chat_id, '⚠️ Нет данных для применения. Запустите заново через «⭐ Цена за 1⭐».')
        return
    rep = _apply_markup_prices(cardinal, rows)
    okn = len(rep['ok'])
    ern = len(rep['err'])
    total = len(rows)
    _set_cfg(chat_id, unit_star_price=float(unit_price))
    _notify_price_changes(cardinal, chat_id, rows, '💹 Цены лотов обновлены')
    msg = f'✅ Готово: обновлено {okn} из {total} лот(ов) по цене 1⭐={unit_price}.'
    if ern:
        msg += f'\n⚠️ Ошибок: {ern}. См. логи.'
    cardinal.telegram.bot.send_message(chat_id, msg)
    _fsm.pop(chat_id, None)
    try:
        _open_pricing(cardinal.telegram.bot, call)
    except Exception:
        pass
def _start_markup(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {'step': 'markup_percent'}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, 'Введите наценку в процентах (например, <b>15</b> или <b>12.5</b>). Можно отрицательное значение для скидки.\n(или /cancel)', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _cb_markup_change(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Измените процент сообщением ниже.')
    except Exception:
        pass
    st = _fsm.get(chat_id) or {}
    st['step'] = 'markup_percent'
    _fsm[chat_id] = st
    m = cardinal.telegram.bot.send_message(chat_id, 'Введите новый процент наценки (или /cancel):', reply_markup=_kb_cancel_fsm())
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _cb_markup_apply(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Применяю…')
    except Exception:
        pass
    st = _fsm.get(chat_id) or {}
    rows = st.get('markup_rows')
    percent = st.get('markup_percent')
    if not rows or percent is None:
        cardinal.telegram.bot.send_message(chat_id, '⚠️ Нет данных для применения. Запустите заново через «💹 Наценка лотов».')
        return
    rep = _apply_markup_prices(cardinal, rows)
    okn = len(rep['ok'])
    ern = len(rep['err'])
    total = len(rows)
    _set_cfg(chat_id, markup_percent=float(percent))
    _notify_price_changes(cardinal, chat_id, rows, '💹 Цены лотов обновлены по наценке')
    msg = f'✅ Готово: обновлено {okn} из {total} лот(ов).'
    if ern:
        msg += f'\n⚠️ Ошибок: {ern}. См. логи.'
    cardinal.telegram.bot.send_message(chat_id, msg)
    _fsm.pop(chat_id, None)
    try:
        _open_pricing(cardinal.telegram.bot, call)
    except Exception:
        pass
def _order_record_update(chat_id, oid, **updates):
    if not oid:
        return
    try:
        oid_s = str(oid)
        cfg = _get_cfg(chat_id)
        recs = dict(cfg.get('order_records') or {})
        rec = dict(recs.get(oid_s) or {})
        now = int(time.time())
        rec.setdefault('oid', oid_s)
        rec.setdefault('chat_id', str(chat_id))
        rec.setdefault('created_ts', now)
        rec.update({k: v for k, v in updates.items() if v is not None})
        rec['updated_ts'] = now
        recs[oid_s] = rec
        if len(recs) > ORDER_RECORDS_LIMIT:
            old = sorted(recs.items(), key=lambda kv: int((kv[1] or {}).get('updated_ts') or 0))
            recs = dict(old[-ORDER_RECORDS_LIMIT:])
        _set_cfg(chat_id, order_records=recs)
    except Exception as e:
        logger.debug(f'order record update failed: {e}')
def _resp_indicates_delivery(resp):
    if not isinstance(resp, dict):
        return False
    if resp.get('ok'):
        return True
    status = str(resp.get('order_status') or _fragment_order_status(resp.get('json')) or '').upper()
    if status in {'PENDING', 'BLOCKCHAIN_SENT', 'OK', 'SUCCESS', 'COMPLETED', 'COMPLETE', 'DONE'}:
        return True
    if resp.get('fragment_order_id'):
        return True
    raw = resp.get('json')
    try:
        dump = json.dumps(raw, ensure_ascii=False).lower() if isinstance(raw, (dict, list)) else str(raw or '').lower()
    except Exception:
        dump = str(raw or '').lower()
    combined = dump + ' ' + str(resp.get('text') or '').lower()
    if any((bad in combined for bad in ('user not found', 'invalid username', 'not enough', 'insufficient', 'low ton balance', 'no jetton wallet'))):
        return False
    return any((x in combined for x in ('blockchain_sent', 'pending', 'transaction_id', 'transactionid', 'tx_hash', '"tx"', 'stars_order')))
def _is_pending_delivery(resp):
    st = str((resp or {}).get('order_status') or _fragment_order_status((resp or {}).get('json')) or '').upper()
    return st in {'PENDING', 'BLOCKCHAIN_SENT'}
def _should_auto_refund(cfg, oid, resp=None, reason=''):
    if not (cfg.get('auto_refund', False) and oid):
        return False
    if str(oid) in _done_oids:
        logger.warning(f'[REFUND] skip #{oid}: order already marked as sent')
        return False
    if _resp_indicates_delivery(resp):
        logger.warning(f'[REFUND] skip #{oid}: Fragment response looks delivered/pending')
        return False
    low = (reason or '').lower()
    if 'pending' in low or 'blockchain' in low or 'заявк' in low or ('принял' in low):
        logger.warning(f'[REFUND] skip #{oid}: reason looks pending/delivered: {reason}')
        return False
    return True
def _mark_order_sent_record(chat_id, oid, qty, username, resp):
    if not oid:
        return
    pending = _is_pending_delivery(resp)
    _order_record_update(chat_id, oid, status='sent_pending' if pending else 'sent', qty=int(qty or 0), username=str(username or '').lstrip('@'), sent_ts=int(time.time()), stars_sent=not pending, fragment_status=(resp or {}).get('order_status'), fragment_id=(resp or {}).get('fragment_order_id'), review_reminder_ts=0)
def _order_record_review_ready(rec):
    if not isinstance(rec, dict):
        return False
    if str(rec.get('status') or '').lower() != 'sent':
        return False
    if rec.get('stars_sent') is False:
        return False
    st = str(rec.get('fragment_status') or '').upper()
    if st in {'PENDING', 'BLOCKCHAIN_SENT'}:
        return False
    return int(rec.get('sent_ts') or rec.get('finalized_ts') or 0) > 0
def _order_health_check(cardinal, chat_id, *, manual=False):
    cfg = _get_cfg(chat_id)
    watch_enabled = _cfg_bool(cfg, 'order_watch_enabled', True)
    review_enabled = _cfg_bool(cfg, 'order_review_reminder_enabled', True)
    if not watch_enabled and (not review_enabled):
        return (0, 0, 0)
    now = time.time()
    wait_sent = review_sent = fixed = 0
    wait_sec = int(cfg.get('order_wait_reminder_sec') or ORDER_WAIT_REMINDER_DEFAULT)
    review_sec = int(cfg.get('order_review_reminder_sec') or ORDER_REVIEW_REMINDER_DEFAULT)
    if watch_enabled:
        for item in list(_q(chat_id)):
            try:
                if item.get('finalized') or not _allowed_stages(item):
                    continue
                cid = item.get('chat_id') or chat_id
                stage = str(item.get('stage') or '')
                ts = float(item.get('stage_ts') or item.get('turn_ts') or item.get('created_ts') or now)
                last = float(item.get('last_reminder_ts') or 0)
                if now - ts >= wait_sec and now - last >= wait_sec:
                    qty = int(item.get('qty') or 50)
                    oid = item.get('order_id') or '—'
                    msg = f'⏳ Напоминание по заказу #{oid}: я всё ещё жду Telegram-тег для {qty}⭐. Пришлите @username одной строкой.' if stage == 'await_username' else f"⏳ Напоминание по заказу #{oid}: ник @{item.get('candidate') or '—'} принят, но я жду подтверждение '+'."
                    _safe_send(cardinal, cid, msg)
                    item['last_reminder_ts'] = now
                    wait_sent += 1
                    _order_record_update(cid, item.get('order_id'), status=stage, qty=qty, username=item.get('candidate'), last_wait_reminder_ts=int(now))
            except Exception as e:
                logger.debug(f'order watch queue item skipped: {e}')
    if review_enabled:
        recs = dict(cfg.get('order_records') or {})
        changed = False
        for oid, rec in list(recs.items()):
            try:
                if not _order_record_review_ready(rec):
                    continue
                if int(rec.get('review_reminder_ts') or 0) > 0:
                    continue
                sent_ts = int(rec.get('sent_ts') or rec.get('finalized_ts') or 0)
                if now - sent_ts < review_sec:
                    continue
                cid = rec.get('chat_id') or chat_id
                qty = int(rec.get('qty') or 0)
                uname = str(rec.get('username') or '').lstrip('@')
                _safe_send(cardinal, cid, f'⭐ Если всё пришло по заказу #{oid} ({qty}⭐ на @{uname}), пожалуйста, подтвердите заказ и оставьте отзыв. Спасибо!')
                rec['review_reminder_ts'] = int(now)
                rec['updated_ts'] = int(now)
                recs[oid] = rec
                changed = True
                review_sent += 1
            except Exception as e:
                logger.debug(f'review reminder skipped: {e}')
        if changed:
            _set_cfg(chat_id, order_records=recs)
    try:
        _set_cfg(chat_id, last_order_watch_ts=int(now))
    except Exception:
        pass
    return (wait_sent, review_sent, fixed)
_AUTO_MAINT_STARTED = False
def _start_auto_maintenance(cardinal):
    global _AUTO_MAINT_STARTED
    if _AUTO_MAINT_STARTED:
        return
    _AUTO_MAINT_STARTED = True
    def loop():
        while True:
            try:
                data = _load_settings()
                for cid, cfg in list(data.items()):
                    if cid in (SETTINGS_META_KEY, LEGACY_SETTINGS_KEY):
                        continue
                    if not isinstance(cfg, dict):
                        continue
                    auto_price_on = _cfg_bool(cfg, 'auto_price_fragment_enabled', False)
                    autodump_on = _cfg_bool(cfg, 'autodump_enabled', False)
                    watch_on = _cfg_bool(cfg, 'order_watch_enabled', True)
                    review_on = _cfg_bool(cfg, 'order_review_reminder_enabled', True)
                    if auto_price_on or autodump_on:
                        _maybe_auto_price_update(cardinal, cid)
                        _maybe_autodump_update(cardinal, cid)
                    if watch_on or review_on:
                        try:
                            last = int(cfg.get('last_order_watch_ts') or 0)
                            interval = int(cfg.get('order_watch_interval_sec') or ORDER_WATCH_INTERVAL_DEFAULT)
                            if time.time() - last >= max(60, interval):
                                _order_health_check(cardinal, cid)
                        except Exception as e:
                            logger.debug(f'order health check skipped: {e}')
            except Exception as e:
                logger.debug(f'auto maintenance skipped: {e}')
            time.sleep(60)
    threading.Thread(target=loop, name='FTS-AUTO-MAINT', daemon=True).start()
_CARDINAL_REF = None
def init_cardinal(cardinal):
    global _CARDINAL_REF
    _CARDINAL_REF = cardinal
    try:
        data, changed, notes = _migrate_settings_data(_load_settings())
        if changed:
            _save_settings(data)
        _order_log('info', 'config_migrated', chat_id='startup', schema=SETTINGS_SCHEMA_VERSION, notes=';'.join(notes) if notes else 'ok')
    except Exception as e:
        logger.warning(f'Startup settings self-check failed: {e}')
    tg = cardinal.telegram
    bot = tg.bot
    _start_auto_maintenance(cardinal)
    try:
        cardinal.add_telegram_commands(UUID, [('fnp', 'Открыть панель FNP Stars', True), ('fnphelp', 'Инструкция FNP Stars', True), ('stars_thc', 'Открыть панель FNP Stars (прямая команда)', True)])
    except Exception:
        pass
    logger.info('🚀 Плагин по продаже звёзд запущен.')
    threading.Thread(target=_queue_watchdog, args=(cardinal,), daemon=True, name='FTS-QUEUE-WATCHDOG').start()
    def _send_home(m):
        return bot.send_message(m.chat.id, _about_text(), parse_mode='HTML', reply_markup=_home_kb(), disable_web_page_preview=True)
    tg.msg_handler(_send_home, commands=['fnp', 'stars_thc'])
    fsm_steps = {'set_min_balance', 'star_add_qty', 'star_add_lotid', 'msg_edit_value', 'unit_star_price_value', 'markup_percent', 'set_jwt', 'saves_import', 'star_price_value', 'autodump_floor_value', 'set_autodump_interval', 'set_order_watch_interval', 'set_order_wait_reminder', 'set_review_reminder_time'}
    tg.msg_handler(lambda m: _handle_fsm(m, cardinal), func=lambda m: m.chat.id in _fsm and _fsm[m.chat.id].get('step') in fsm_steps, content_types=['text', 'document'])
    tg.cbq_handler(lambda c: _open_home(bot, c), func=lambda c: c.data.startswith(f'{CBT.EDIT_PLUGIN}:{UUID}') or c.data.startswith(f'{CBT.PLUGIN_SETTINGS}:{UUID}') or c.data == f'{UUID}:0' or (c.data == CBT_HOME))
    tg.cbq_handler(lambda c: _open_settings(bot, c), func=lambda c: c.data == CBT_SETTINGS)
    tg.cbq_handler(lambda c: _open_token(bot, c), func=lambda c: c.data == CBT_TOKEN)
    tg.cbq_handler(lambda c: _open_stars(bot, c), func=lambda c: c.data == CBT_STARS)
    tg.cbq_handler(lambda c: _fsm_cancel(cardinal, c), func=lambda c: c.data == CBT_FSM_CANCEL)
    tg.cbq_handler(lambda c: _toggle_plugin(bot, c), func=lambda c: c.data == CBT_TOGGLE_PLUGIN)
    tg.cbq_handler(lambda c: _toggle_lots(bot, c), func=lambda c: c.data == CBT_TOGGLE_LOTS)
    tg.cbq_handler(lambda c: _toggle_refund(bot, c), func=lambda c: c.data == CBT_TOGGLE_REFUND)
    tg.cbq_handler(lambda c: _toggle_deact(bot, c), func=lambda c: c.data == CBT_TOGGLE_DEACT)
    tg.cbq_handler(lambda c: _refresh(bot, c), func=lambda c: c.data == CBT_REFRESH)
    tg.cbq_handler(lambda c: _ask_set_min_balance(bot, c), func=lambda c: c.data == CBT_SET_MIN_BAL)
    tg.msg_handler(lambda m: _cancel_cmd(cardinal, m.chat.id), commands=['cancel'])
    tg.cbq_handler(lambda c: _del_jwt(bot, c), func=lambda c: c.data == CBT_DEL_JWT)
    tg.cbq_handler(lambda c: _ask_set_jwt(bot, c), func=lambda c: c.data == CBT_SET_JWT)
    tg.cbq_handler(lambda c: _star_add(bot, c), func=lambda c: c.data == CBT_STAR_ADD)
    tg.cbq_handler(lambda c: _star_act_all(bot, c), func=lambda c: c.data == CBT_STAR_ACT_ALL)
    tg.cbq_handler(lambda c: _star_deact_all(bot, c), func=lambda c: c.data == CBT_STAR_DEACT_ALL)
    tg.cbq_handler(lambda c: _star_toggle(bot, c), func=lambda c: c.data.startswith(CBT_STAR_TOGGLE_P))
    tg.cbq_handler(lambda c: _star_dump_floor_start(bot, c), func=lambda c: c.data.startswith(CBT_AUTODUMP_FLOOR_P))
    tg.cbq_handler(lambda c: _star_delete(bot, c), func=lambda c: c.data.startswith(CBT_STAR_DEL_P))
    tg.cbq_handler(lambda c: _cb_star_autoadd(cardinal, c), func=lambda c: c.data == CBT_STAR_AUTOADD)
    tg.cbq_handler(lambda c: _cb_confirm_send(cardinal, c), func=lambda c: c.data == CBT_CONFIRM_SEND)
    tg.cbq_handler(lambda c: _cb_change_username(cardinal, c), func=lambda c: c.data == CBT_CHANGE_USERNAME)
    tg.cbq_handler(lambda c: _cb_cancel_flow(cardinal, c), func=lambda c: c.data == CBT_CANCEL_FLOW)
    tg.cbq_handler(lambda c: _go_main_menu(cardinal, c), func=lambda c: c.data == CBT_BACK_PLUGINS)
    tg.cbq_handler(lambda c: _open_messages(bot, c), func=lambda c: c.data == CBT_MESSAGES)
    tg.cbq_handler(lambda c: _msg_edit_start(bot, c), func=lambda c: c.data.startswith(CBT_MSG_EDIT_P))
    tg.cbq_handler(lambda c: _msg_reset(bot, c), func=lambda c: c.data.startswith(CBT_MSG_RESET_P))
    tg.cbq_handler(lambda c: _toggle_preorder_username(bot, c), func=lambda c: c.data == CBT_TOGGLE_PREORDER)
    tg.cbq_handler(lambda c: _open_pricing(bot, c), func=lambda c: c.data == CBT_PRICING)
    tg.cbq_handler(lambda c: _cb_auto_price_fragment(cardinal, c), func=lambda c: c.data == CBT_AUTO_PRICE_FRAGMENT)
    tg.cbq_handler(lambda c: _open_autodump(bot, c), func=lambda c: c.data == CBT_AUTODUMP)
    tg.cbq_handler(lambda c: _toggle_autodump(bot, c), func=lambda c: c.data == CBT_TOGGLE_AUTODUMP)
    tg.cbq_handler(lambda c: _cb_autodump_run(cardinal, c), func=lambda c: c.data == CBT_AUTODUMP_RUN)
    tg.cbq_handler(lambda c: _ask_autodump_interval(bot, c), func=lambda c: c.data == CBT_AUTODUMP_INTERVAL)
    tg.cbq_handler(lambda c: _toggle_autodump_notifications(bot, c), func=lambda c: c.data == CBT_TOGGLE_AUTODUMP_NOTIFY)
    tg.cbq_handler(lambda c: _toggle_auto_price_fragment(bot, c), func=lambda c: c.data == CBT_TOGGLE_AUTO_PRICE)
    tg.cbq_handler(lambda c: _toggle_balance_lot_filter(bot, c), func=lambda c: c.data == CBT_TOGGLE_BALANCE_FILTER)
    tg.cbq_handler(lambda c: _cb_balance_lot_filter_run(cardinal, c), func=lambda c: c.data == CBT_BALANCE_FILTER_RUN)
    tg.cbq_handler(lambda c: _start_markup(bot, c), func=lambda c: c.data == CBT_MARKUP)
    tg.cbq_handler(lambda c: _cb_markup_apply(cardinal, c), func=lambda c: c.data == CBT_MARKUP_APPLY)
    tg.cbq_handler(lambda c: _cb_markup_change(cardinal, c), func=lambda c: c.data == CBT_MARKUP_CHANGE)
    tg.cbq_handler(lambda c: _open_mini_settings(bot, c), func=lambda c: c.data == CBT_MINI_SETTINGS)
    tg.cbq_handler(lambda c: _open_order_tools(bot, c), func=lambda c: c.data == CBT_ORDER_TOOLS)
    tg.cbq_handler(lambda c: _open_notifications(bot, c), func=lambda c: c.data == CBT_NOTIFICATIONS)
    tg.cbq_handler(lambda c: _toggle_price_notifications(bot, c), func=lambda c: c.data == CBT_TOGGLE_PRICE_NOTIFY)
    tg.cbq_handler(lambda c: _open_saves(bot, c), func=lambda c: c.data == CBT_SAVES)
    tg.cbq_handler(lambda c: _ask_import_saves(bot, c), func=lambda c: c.data == CBT_SAVES_IMPORT)
    tg.cbq_handler(lambda c: _download_saves(bot, c), func=lambda c: c.data == CBT_SAVES_DOWNLOAD)
    tg.cbq_handler(lambda c: _star_price_start(bot, c), func=lambda c: c.data.startswith(CBT_STAR_PRICE_P))
    tg.cbq_handler(lambda c: _cb_markup_reset(cardinal, c), func=lambda c: c.data == CBT_MARKUP_RESET)
    tg.cbq_handler(lambda c: _send_logs(bot, c), func=lambda c: c.data == CBT_LOGS)
    tg.cbq_handler(lambda c: _open_stats(bot, c), func=lambda c: c.data == CBT_STATS)
    tg.cbq_handler(lambda c: _open_stats(bot, c, c.data.split(':')[-1]), func=lambda c: c.data.startswith(CBT_STATS_RANGE_P))
    tg.cbq_handler(lambda c: _cb_update_plugin(cardinal, c), func=lambda c: c.data == CBT_UPDATE_PLUGIN)
    tg.cbq_handler(lambda c: _cb_update_plugin_yes(cardinal, c), func=lambda c: c.data == CBT_UPDATE_PLUGIN_YES)
    tg.cbq_handler(lambda c: _cb_update_plugin_no(bot, c), func=lambda c: c.data == CBT_UPDATE_PLUGIN_NO)
    tg.cbq_handler(lambda c: _open_delete_confirm(bot, c), func=lambda c: c.data == CBT_DELETE_ASK)
    tg.cbq_handler(lambda c: _cb_delete_yes(cardinal, c), func=lambda c: c.data == CBT_DELETE_YES)
    tg.cbq_handler(lambda c: _cb_delete_no(bot, c), func=lambda c: c.data == CBT_DELETE_NO)
    tg.cbq_handler(lambda c: _toggle_liteserver_retry(bot, c), func=lambda c: c.data == CBT_TOGGLE_LITESERVER_RETRY)
    tg.cbq_handler(lambda c: _toggle_username_check(bot, c), func=lambda c: c.data == CBT_TOGGLE_USERNAME_CHECK)
    tg.cbq_handler(lambda c: _toggle_autosend_plus(bot, c), func=lambda c: c.data == CBT_TOGGLE_AUTOSEND_PLUS)
    tg.cbq_handler(lambda c: _toggle_order_watch(bot, c), func=lambda c: c.data == CBT_TOGGLE_ORDER_WATCH)
    tg.cbq_handler(lambda c: _ask_order_watch_interval(bot, c), func=lambda c: c.data == CBT_ORDER_WATCH_INTERVAL)
    tg.cbq_handler(lambda c: _ask_order_wait_reminder(bot, c), func=lambda c: c.data == CBT_ORDER_WAIT_REMINDER)
    tg.cbq_handler(lambda c: _toggle_review_reminder(bot, c), func=lambda c: c.data == CBT_TOGGLE_REVIEW_REMINDER)
    tg.cbq_handler(lambda c: _ask_review_reminder_time(bot, c), func=lambda c: c.data == CBT_REVIEW_REMINDER_TIME)
    tg.cbq_handler(lambda c: _cb_order_watch_run(cardinal, c), func=lambda c: c.data == CBT_ORDER_WATCH_RUN)
    tg.cbq_handler(lambda c: _toggle_queue_mode(bot, c), func=lambda c: c.data == CBT_TOGGLE_QUEUE_MODE)
    tg.cbq_handler(lambda c: _toggle_stars_currency(bot, c), func=lambda c: c.data == CBT_TOGGLE_STARS_CURRENCY)
    tg.cbq_handler(lambda c: _toggle_usdt_fallback(bot, c), func=lambda c: c.data == CBT_TOGGLE_USDT_FALLBACK)
    tg.cbq_handler(lambda c: _open_reset_settings_confirm(bot, c), func=lambda c: c.data == CBT_RESET_SETTINGS_ASK)
    tg.cbq_handler(lambda c: _cb_reset_settings_yes(cardinal, c), func=lambda c: c.data == CBT_RESET_SETTINGS_YES)
    tg.cbq_handler(lambda c: _cb_reset_settings_no(bot, c), func=lambda c: c.data == CBT_RESET_SETTINGS_NO)
    tg.cbq_handler(lambda c: _start_unit_price(bot, c), func=lambda c: c.data == CBT_UNIT_PRICE)
    tg.cbq_handler(lambda c: _cb_unit_price_apply(cardinal, c), func=lambda c: c.data == CBT_UNIT_PRICE_APPLY)
    tg.cbq_handler(lambda c: _cb_unit_price_change(cardinal, c), func=lambda c: c.data == CBT_UNIT_PRICE_CHANGE)
def _open_home(bot, call):
    _safe_edit(bot, call.message.chat.id, call.message.id, _about_text(), _home_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _go_main_menu(cardinal, call):
    bot = cardinal.telegram.bot
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    for attr in ('open_main_menu', 'show_main_menu', 'menu', 'open_menu', 'start_menu', 'home'):
        fn = getattr(cardinal.telegram, attr, None) or getattr(cardinal, attr, None)
        if callable(fn):
            try:
                fn(call.message.chat.id)
                return
            except Exception as e:
                logger.warning(f'open main menu via {attr} failed: {e}')
    _open_home(bot, call)
def _open_settings(bot, call):
    chat_id = call.message.chat.id
    try:
        text = _settings_text(chat_id)
        kb = _settings_kb(chat_id)
        ok = _safe_edit(bot, chat_id, call.message.id, text, kb)
        if not ok:
            _safe_send_tg(bot, chat_id, text, kb)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
    except Exception as e:
        logger.exception(f'open_settings failed: {e}')
        try:
            bot.answer_callback_query(call.id, 'Не удалось открыть настройки. Проверьте лог плагина.', show_alert=True)
        except Exception:
            pass
        try:
            _safe_send_tg(bot, chat_id, '⚠️ Не удалось открыть настройки. Ошибка записана в лог плагина.')
        except Exception:
            pass
def _plugin_version_key(value):
    nums = [int(x) for x in _re.findall('\\d+', str(value or ''))[:4]]
    nums.extend([0] * (4 - len(nums)))
    return tuple(nums[:4])
def _plugin_version_from_source(source):
    m = _re.search('(?m)^\\s*VERSION\\s*=\\s*["\\\']([^"\\\']+)["\\\']', source or '')
    return m.group(1).strip() if m else None
def _cleanup_plugin_bytecode(plugin_file):
    try:
        pycache_dir = os.path.join(os.path.dirname(plugin_file), '__pycache__')
        if not os.path.isdir(pycache_dir):
            return
        base = os.path.splitext(os.path.basename(plugin_file))[0]
        for fn in os.listdir(pycache_dir):
            if fn.startswith(base) and fn.endswith('.pyc'):
                try:
                    os.remove(os.path.join(pycache_dir, fn))
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f'Update pycache cleanup failed: {e}')
def _pending_update_file(plugin_file=None):
    return os.path.abspath(plugin_file or __file__) + '.update.pending'
def _download_plugin_update_from_github():
    plugin_file = os.path.abspath(__file__)
    pending_file = _pending_update_file(plugin_file)
    result = {'ok': False, 'changed': False, 'current_version': VERSION, 'remote_version': None, 'pending_file': pending_file, 'error': None}
    try:
        if not GITHUB_UPDATE_URL.lower().startswith('https://'):
            raise RuntimeError('ссылка обновления должна использовать HTTPS')
        response = _HTTP.get(GITHUB_UPDATE_URL, headers={'Accept': 'text/plain, application/octet-stream;q=0.9, */*;q=0.1', 'User-Agent': f'{NAME}/{VERSION} self-updater', 'Cache-Control': 'no-cache'}, timeout=45)
        response.raise_for_status()
        payload = response.content
        if not payload or len(payload) < 10000:
            raise RuntimeError(f'GitHub вернул слишком маленький файл ({len(payload)} байт)')
        if len(payload) > 5 * 1024 * 1024:
            raise RuntimeError('файл обновления слишком большой')
        try:
            source = payload.decode('utf-8-sig')
        except UnicodeDecodeError as e:
            raise RuntimeError(f'файл обновления не в UTF-8: {e}') from e
        low_head = source[:500].lower()
        if '<html' in low_head or '<!doctype' in low_head:
            raise RuntimeError('вместо Python-файла GitHub вернул HTML-страницу')
        required = ('FTS-Plugin', 'def init_cardinal', 'BIND_TO_PRE_INIT', 'BIND_TO_NEW_MESSAGE')
        missing = [item for item in required if item not in source]
        if missing:
            raise RuntimeError('скачан не тот файл плагина: нет ' + ', '.join(missing))
        if UUID not in source and 'ZmEwYzJmM2EtN2E4NS00YzA5LWEzYjItOWYzYTliOGY4YTc1' not in source:
            raise RuntimeError('UUID скачанного плагина не совпадает')
        remote_version = _plugin_version_from_source(source)
        if not remote_version:
            raise RuntimeError('в скачанном файле не найдена VERSION')
        result['remote_version'] = remote_version
        compile(source, plugin_file, 'exec')
        current_key = _plugin_version_key(VERSION)
        remote_key = _plugin_version_key(remote_version)
        if remote_key <= current_key:
            try:
                if os.path.exists(pending_file):
                    os.remove(pending_file)
            except Exception:
                pass
            result.update(ok=True, changed=False)
            return result
        with open(pending_file, 'wb') as f:
            f.write(source.encode('utf-8'))
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(pending_file, os.stat(plugin_file).st_mode)
        except Exception:
            pass
        result.update(ok=True, changed=True)
        return result
    except Exception as e:
        result['error'] = str(e)
        logger.exception(f'GitHub plugin update check failed: {e}')
        try:
            if os.path.exists(pending_file):
                os.remove(pending_file)
        except Exception:
            pass
        return result
def _install_pending_plugin_update(remote_version=None):
    plugin_file = os.path.abspath(__file__)
    pending_file = _pending_update_file(plugin_file)
    backup_file = plugin_file + '.pre-update.bak'
    result = {'ok': False, 'changed': False, 'current_version': VERSION, 'remote_version': remote_version, 'backup_file': backup_file, 'error': None}
    try:
        if not os.path.isfile(pending_file):
            raise RuntimeError('файл обновления не найден. Сначала нажмите «Проверить обновление».')
        with open(pending_file, 'rb') as f:
            payload = f.read()
        if not payload or len(payload) < 10000:
            raise RuntimeError(f'файл обновления слишком маленький ({len(payload)} байт)')
        if len(payload) > 5 * 1024 * 1024:
            raise RuntimeError('файл обновления слишком большой')
        try:
            source = payload.decode('utf-8-sig')
        except UnicodeDecodeError as e:
            raise RuntimeError(f'файл обновления не в UTF-8: {e}') from e
        remote_version = _plugin_version_from_source(source)
        if not remote_version:
            raise RuntimeError('в файле обновления не найдена VERSION')
        result['remote_version'] = remote_version
        if _plugin_version_key(remote_version) <= _plugin_version_key(VERSION):
            try:
                os.remove(pending_file)
            except Exception:
                pass
            result.update(ok=True, changed=False)
            return result
        compile(source, plugin_file, 'exec')
        if UUID not in source and 'ZmEwYzJmM2EtN2E4NS00YzA5LWEzYjItOWYzYTliOGY4YTc1' not in source:
            raise RuntimeError('UUID скачанного плагина не совпадает')
        if os.path.isfile(SETTINGS_FILE):
            os.makedirs(os.path.dirname(SETTINGS_BAK), exist_ok=True)
            shutil.copy2(SETTINGS_FILE, SETTINGS_BAK)
        tmp_file = plugin_file + '.update.tmp'
        with open(tmp_file, 'wb') as f:
            f.write(source.encode('utf-8'))
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp_file, os.stat(plugin_file).st_mode)
        except Exception:
            pass
        shutil.copy2(plugin_file, backup_file)
        os.replace(tmp_file, plugin_file)
        try:
            os.remove(pending_file)
        except Exception:
            pass
        _cleanup_plugin_bytecode(plugin_file)
        result.update(ok=True, changed=True)
        logger.warning(f'Plugin updated from GitHub after confirm: {VERSION} -> {remote_version}; backup={backup_file}')
        return result
    except Exception as e:
        result['error'] = str(e)
        logger.exception(f'GitHub plugin confirmed update failed: {e}')
        return result
def _cb_update_plugin(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Проверяю обновление на GitHub…')
    except Exception:
        pass
    _safe_edit(bot, chat_id, call.message.id, '⏬ <b>Проверяю новую версию…</b>\n\nСкачиваю файл из GitHub и проверяю его. Установка начнётся только после подтверждения.', None)
    result = _download_plugin_update_from_github()
    if result.get('ok') and result.get('changed'):
        kb = K()
        kb.row(B('✅ Обновить', callback_data=CBT_UPDATE_PLUGIN_YES), B('❌ Отмена', callback_data=CBT_UPDATE_PLUGIN_NO))
        kb.row(B('🌐 GitHub', url=GITHUB_URL), B('◀️ Назад', callback_data=CBT_HOME))
        text = f"🆕 <b>Найдена новая версия плагина.</b>\n\nТекущая версия: <code>{_h(result.get('current_version'))}</code>\nНовая версия: <code>{_h(result.get('remote_version'))}</code>\n\nФайл уже скачан во временный буфер и прошёл базовую проверку.\nУстановить обновление сейчас?"
    elif result.get('ok'):
        kb = K()
        kb.row(B('🌐 GitHub', url=GITHUB_URL), B('◀️ Назад', callback_data=CBT_HOME))
        text = f"✅ <b>Обновление не требуется.</b>\n\nУстановлена версия: <code>{_h(result.get('current_version'))}</code>\nВерсия на GitHub: <code>{_h(result.get('remote_version') or 'не определена')}</code>\n\nКонфиг не изменён."
    else:
        kb = K()
        kb.row(B('🌐 GitHub', url=GITHUB_URL), B('◀️ Назад', callback_data=CBT_HOME))
        text = f"❌ <b>Не удалось проверить обновление.</b>\n\nОшибка: <code>{_h(result.get('error') or 'неизвестная ошибка')}</code>\n\nТекущий файл и конфиг не изменены."
    if not _safe_edit(bot, chat_id, call.message.id, text, kb):
        _safe_send_tg(bot, chat_id, text, kb)
def _cb_update_plugin_yes(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Устанавливаю обновление…')
    except Exception:
        pass
    _safe_edit(bot, chat_id, call.message.id, '⏬ <b>Устанавливаю обновление…</b>\n\nСохраняю резервную копию и заменяю файл плагина.', None)
    result = _install_pending_plugin_update()
    kb = K()
    kb.row(B('🌐 GitHub', url=GITHUB_URL), B('◀️ Назад', callback_data=CBT_HOME))
    if result.get('ok') and result.get('changed'):
        text = f"✅ <b>Плагин обновлён.</b>\n\nВерсия: <code>{_h(result.get('current_version'))}</code> → <code>{_h(result.get('remote_version'))}</code>\n💾 Старый конфиг сохранён.\n🛟 Предыдущий файл плагина сохранён как <code>{_h(os.path.basename(str(result.get('backup_file') or '')))}</code>.\n\n🔁 Для загрузки новой версии выполните: <code>/restart</code>"
    elif result.get('ok'):
        text = f"✅ <b>Обновление не требуется.</b>\n\nУстановлена версия: <code>{_h(result.get('current_version'))}</code>\nВерсия на GitHub: <code>{_h(result.get('remote_version') or 'не определена')}</code>\n\nФайл плагина не изменён."
    else:
        text = f"❌ <b>Не удалось установить обновление.</b>\n\nОшибка: <code>{_h(result.get('error') or 'неизвестная ошибка')}</code>\n\nТекущий файл и конфиг не изменены."
    if not _safe_edit(bot, chat_id, call.message.id, text, kb):
        _safe_send_tg(bot, chat_id, text, kb)
def _cb_update_plugin_no(bot, call):
    chat_id = call.message.chat.id
    try:
        pending_file = _pending_update_file()
        if os.path.exists(pending_file):
            os.remove(pending_file)
    except Exception:
        pass
    try:
        bot.answer_callback_query(call.id, 'Обновление отменено.')
    except Exception:
        pass
    kb = K()
    kb.row(B('🔄 Проверить снова', callback_data=CBT_UPDATE_PLUGIN), B('◀️ Назад', callback_data=CBT_HOME))
    text = '❌ <b>Обновление отменено.</b>\n\nФайл плагина и конфиг не изменены.'
    if not _safe_edit(bot, chat_id, call.message.id, text, kb):
        _safe_send_tg(bot, chat_id, text, kb)
def _delete_confirm_text():
    return '⚠️ <b>Удаление плагина</b>\n\nВы точно хотите удалить <b>FTS-Plugin</b>?\n\nБудут удалены:\n• файлы плагина\n• настройки и логи\n\n<b>Действие необратимо.</b>\nПосле удаления выполните перезапуск: напишите команду <code>/restart</code>.'
def _delete_confirm_kb():
    kb = K()
    kb.row(B('✅ Да, удалить', callback_data=CBT_DELETE_YES), B('❌ Нет', callback_data=CBT_DELETE_NO))
    return kb
def _open_delete_confirm(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _delete_confirm_text(), _delete_confirm_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _self_delete_from_disk():
    errors = []
    try:
        if _CARDINAL_REF is not None:
            _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, False)
    except Exception as e:
        errors.append(f'Не удалось выключить лоты: {e}')
    try:
        shutil.rmtree(PLUGIN_FOLDER, ignore_errors=True)
    except Exception as e:
        errors.append(f'Не удалось удалить папку настроек {PLUGIN_FOLDER}: {e}')
    plugin_file = os.path.abspath(__file__)
    try:
        try:
            pycache_dir = os.path.join(os.path.dirname(plugin_file), '__pycache__')
            if os.path.isdir(pycache_dir):
                base = os.path.splitext(os.path.basename(plugin_file))[0]
                for fn in os.listdir(pycache_dir):
                    if fn.startswith(base) and fn.endswith('.pyc'):
                        try:
                            os.remove(os.path.join(pycache_dir, fn))
                        except Exception:
                            pass
        except Exception:
            pass
        os.remove(plugin_file)
    except Exception as e:
        errors.append(f'Не удалось удалить файл плагина {plugin_file}: {e}')
    return (len(errors) == 0, errors)
def _cb_delete_yes(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Удаляю…')
    except Exception:
        pass
    ok, errors = _self_delete_from_disk()
    kb = K()
    kb.add(B('🔙 К списку плагинов', callback_data=CBT_PLUGINS_LIST_OPEN))
    if ok:
        text = '✅ <b>Плагин удалён.</b>\n\n🔁 Чтобы применилось, напишите команду: <code>/restart</code>'
    else:
        text = '⚠️ <b>Удаление выполнено частично.</b>\n\nЧто пошло не так:\n' + '\n'.join([f'• {e}' for e in errors[:10]]) + '\n\n🔁 После завершения удаления всё равно выполните перезапуск командой: <code>/restart</code>'
    _safe_edit(bot, chat_id, call.message.id, text, kb)
def _cb_delete_no(bot, call):
    _open_home(bot, call)
def _open_token(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _token_text(chat_id), _token_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _open_stars(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _stars_text(chat_id), _stars_kb(chat_id))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _send_logs(bot, call):
    chat_id = call.message.chat.id
    path = os.getenv('FTS_RAW_LOG_FILE') or os.path.join(PLUGIN_FOLDER, 'log.txt')
    try:
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as _:
                pass
        with open(path, 'rb') as f:
            bot.send_document(chat_id, ('log.txt', f.read()), caption='Логи FTS-Plugin')
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
    except Exception as e:
        try:
            bot.answer_callback_query(call.id, f'Не удалось отправить лог: {e}', show_alert=True)
        except Exception:
            pass
CBT_VER_PREFIX = f'{UUID}:ver:'
def _stats_kb(selected):
    ranges = [('24ч', '24h'), ('7д', '7d'), ('30д', '30d'), ('Всё', 'all')]
    kb = K()
    row = []
    for label, key in ranges:
        mark = '• ' if key == selected else ''
        row.append(B(f'{mark}{label}', callback_data=f'{CBT_STATS_RANGE_P}{key}'))
    kb.row(*row)
    kb.add(B('◀️ Назад', callback_data=CBT_SETTINGS))
    return kb
def _log_path():
    return os.getenv('FTS_RAW_LOG_FILE') or LOG_FILE_LOCAL
def _read_log_tail(max_bytes=1500000):
    path = _log_path()
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            sz = f.tell()
            f.seek(max(0, sz - max_bytes), os.SEEK_SET)
            return f.read().decode('utf-8', errors='ignore').splitlines()
    except Exception as e:
        logger.debug(f'stats read log failed: {e}')
        return []
def _parse_line_ts(line):
    try:
        s = line[:19]
        tt = time.strptime(s, '%Y-%m-%d %H:%M:%S')
        return time.mktime(tt)
    except Exception:
        return None
def _stats_collect(lines, since_ts):
    stats = {'ok': 0, 'fail': 0, 'qty_ok': 0, 'qty_fail': 0, 'pending': 0, 'per_user': defaultdict(lambda: {'qty': 0, 'cnt': 0}), 'per_day': defaultdict(lambda: {'qty': 0, 'ok': 0, 'fail': 0}), 'refunds_ok': 0, 'refunds_fail': 0, 'auto_deact': 0, 'preorder': 0, 'queue_merge': 0, 'ignore': 0, 'price_updates': 0, 'autodump_updates': 0, 'last_ok': None, 'last_fail': None, 'fail_reasons': defaultdict(int), 'currency': defaultdict(lambda: {'ok': 0, 'fail': 0, 'qty': 0})}
    last = None
    for ln in lines:
        ts = _parse_line_ts(ln)
        if since_ts and ts and (ts < since_ts):
            continue
        low = ln.lower()
        if '[ignore]' in low:
            stats['ignore'] += 1
        if '[queue] merged' in low:
            stats['queue_merge'] += 1
        if '[autodeact]' in low:
            stats['auto_deact'] += 1
        if '[preorder]' in low:
            stats['preorder'] += 1
        if 'цены лотов обновлены' in low or 'автоцены применены' in low:
            stats['price_updates'] += 1
        if 'автодемп' in low and ('изменил' in low or 'применён' in low):
            stats['autodump_updates'] += 1
        m = _re.search('SEND start:\\s*(\\d+)\\s*⭐\\s*→\\s*@?([A-Za-z0-9_]{4,32})(?:.*?currency=([a-z0-9_]+))?', ln)
        if m:
            last = {'qty': int(m.group(1)), 'user': m.group(2).lower(), 'cur': m.group(3) or 'ton', 'ts': ts}
            continue
        m = _re.search('SEND OK\\s+(\\d+)\\s*⭐.*?@([A-Za-z0-9_]{4,32})', ln)
        if m:
            q = int(m.group(1))
            u = m.group(2).lower()
            cur = (last or {}).get('cur', 'ton')
            stats['ok'] += 1
            stats['qty_ok'] += q
            stats['per_user'][u]['qty'] += q
            stats['per_user'][u]['cnt'] += 1
            stats['currency'][cur]['ok'] += 1
            stats['currency'][cur]['qty'] += q
            if ts:
                stats['last_ok'] = ts
            day = time.strftime('%Y-%m-%d', time.localtime(ts or time.time()))
            stats['per_day'][day]['qty'] += q
            stats['per_day'][day]['ok'] += 1
            continue
        m = _re.search('SEND FAIL\\s+(\\d+)\\s*⭐.*?@([A-Za-z0-9_]{4,32}):\\s*(.+?)(?:\\s*\\|\\s*status=(\\d+))?$', ln)
        if m:
            q = int(m.group(1))
            cur = (last or {}).get('cur', 'ton')
            stats['fail'] += 1
            stats['qty_fail'] += q
            stats['currency'][cur]['fail'] += 1
            reason = (m.group(3) or 'ошибка').strip()[:120]
            stats['fail_reasons'][reason] += 1
            if ts:
                stats['last_fail'] = ts
            day = time.strftime('%Y-%m-%d', time.localtime(ts or time.time()))
            stats['per_day'][day]['fail'] += 1
            continue
        m = _re.search('SEND result:\\s*ok=(True|False).*?currency=([a-z0-9_]+)?.*?order_status=([A-Za-z_\\-]+|-)?', ln)
        if m and last:
            cur = m.group(2) or last.get('cur', 'ton')
            st = (m.group(3) or '').upper()
            if m.group(1) == 'True':
                stats['ok'] += 1
                stats['qty_ok'] += last['qty']
                stats['per_user'][last['user']]['qty'] += last['qty']
                stats['per_user'][last['user']]['cnt'] += 1
                stats['currency'][cur]['ok'] += 1
                stats['currency'][cur]['qty'] += last['qty']
                if st in {'PENDING', 'BLOCKCHAIN_SENT'}:
                    stats['pending'] += 1
                if ts:
                    stats['last_ok'] = ts
                day = time.strftime('%Y-%m-%d', time.localtime(ts or time.time()))
                stats['per_day'][day]['qty'] += last['qty']
                stats['per_day'][day]['ok'] += 1
            else:
                stats['fail'] += 1
                stats['qty_fail'] += last['qty']
                stats['currency'][cur]['fail'] += 1
                if ts:
                    stats['last_fail'] = ts
                day = time.strftime('%Y-%m-%d', time.localtime(ts or time.time()))
                stats['per_day'][day]['fail'] += 1
        m = _re.search('REFUND\\s+#?([A-Za-z0-9\\-]+)\\s*->\\s*(OK|FAIL)', ln)
        if m:
            if m.group(2) == 'OK':
                stats['refunds_ok'] += 1
            else:
                stats['refunds_fail'] += 1
    stats['per_user'] = dict(stats['per_user'])
    stats['per_day'] = dict(stats['per_day'])
    stats['fail_reasons'] = dict(stats['fail_reasons'])
    stats['currency'] = dict(stats['currency'])
    return stats
def _fmt_human_ts(ts):
    if not ts:
        return '—'
    return time.strftime('%d.%m.%Y %H:%M:%S', time.localtime(ts))
def _stats_text(chat_id, range_key):
    now = time.time()
    ranges = {'24h': (now - 86400, 'за 24 часа'), '7d': (now - 604800, 'за 7 дней'), '30d': (now - 2592000, 'за 30 дней'), 'all': (None, 'за всё время')}
    since_ts, label = ranges.get(range_key, ranges['7d'])
    s = _stats_collect(_read_log_tail(), since_ts)
    total = s['ok'] + s['fail']
    conv = s['ok'] / total * 100.0 if total else 0.0
    avg = s['qty_ok'] / s['ok'] if s['ok'] else 0.0
    top = sorted(s['per_user'].items(), key=lambda kv: kv[1]['qty'], reverse=True)[:7]
    top_lines = [f"{i}) @{u} — {int(v['qty'])}⭐ ({v['cnt']} заказ.)" for i, (u, v) in enumerate(top, 1)]
    days = sorted(s['per_day'].items())[-10:]
    day_lines = [f"{d}: {int(v.get('qty', 0))}⭐ / ✅{v.get('ok', 0)} ❌{v.get('fail', 0)}" for d, v in days] if days else ['—']
    fails = sorted(s['fail_reasons'].items(), key=lambda kv: kv[1], reverse=True)[:5]
    fail_lines = [f'• {cnt}× — {_h(reason)}' for reason, cnt in fails] if fails else ['—']
    cur_lines = [f"• {_h(_stars_currency_label(cur))}: ✅{v.get('ok', 0)} / ❌{v.get('fail', 0)} / {int(v.get('qty', 0))}⭐" for cur, v in sorted(s['currency'].items())]
    cfg = _get_cfg(chat_id)
    currency = _normalize_stars_currency(cfg.get('stars_currency'))
    advice = []
    if conv and conv < 90:
        advice.append('низкая конверсия — посмотри последние причины ошибок')
    if any(('jetton' in r.lower() or 'комисс' in r.lower() or 'low ton' in r.lower() for r in s['fail_reasons'])):
        advice.append('для USDT проверь USDT-jetton wallet и запас TON на комиссию')
    if not advice and total:
        advice.append('система работает стабильно')
    return f"<b>📊 Умная статистика ({label})</b>\n\n<b>Продажи</b>\n• Успешно: <b>{s['ok']}</b> / Ошибок: <b>{s['fail']}</b> / Конверсия: <b>{conv:.1f}%</b>\n• Звёзд отправлено: <b>{int(s['qty_ok'])}⭐</b>; средний заказ: <b>{avg:.0f}⭐</b>\n• PENDING/BLOCKCHAIN_SENT: <b>{s['pending']}</b>\n• Последний успех: <code>{_fmt_human_ts(s['last_ok'])}</code>\n• Последняя ошибка: <code>{_fmt_human_ts(s['last_fail'])}</code>\n\n<b>Валюты и баланс</b>\n• Сейчас выбрано: <b>{_stars_currency_emoji(currency)} {_stars_currency_label(currency)}</b>\n• Баланс: <code>{_wallet_balance_text(cfg)}</code>\n" + ('\n'.join(cur_lines) if cur_lines else '—') + f"\n\n<b>Автоматика</b>\n• Автоцены: <b>{s['price_updates']}</b> срабатыв.\n• Автодемп: <b>{s['autodump_updates']}</b> срабатыв.\n• Автодеактиваций: <b>{s['auto_deact']}</b>; возвратов: ✅{s['refunds_ok']} / ❌{s['refunds_fail']}\n\n<b>Топ покупателей</b>\n" + ('\n'.join(top_lines) if top_lines else '—') + '\n\n<b>Главные ошибки</b>\n' + '\n'.join(fail_lines) + '\n\n<b>Активность по дням</b>\n' + '\n'.join(day_lines) + '\n\n<b>Вывод</b>\n• ' + '\n• '.join(advice)
def _open_stats(bot, call, range_key=None):
    chat_id = call.message.chat.id
    rk = range_key or '7d'
    text = _stats_text(chat_id, rk)
    _safe_edit(bot, chat_id, call.message.id, text, _stats_kb(rk))
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _star_add(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {'step': 'star_add_qty'}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    bot.send_message(chat_id, 'Введите <b>количество звёзд</b> (целое число от 50 до 1_000_000):', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
def _star_act_all(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    items = cfg.get('star_lots') or []
    if not items:
        bot.answer_callback_query(call.id, 'Список пуст. Запустите «Автодобавление лотов» или добавьте LOT вручную.', show_alert=True)
        return
    rep = {'ok': [], 'skip': [], 'err': []}
    if _CARDINAL_REF is not None:
        rep = _apply_star_lots_state(_CARDINAL_REF, items, True)
    for it in items:
        it['active'] = True
    managed_ids = _merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [x.get('lot_id') for x in items])
    _set_cfg(chat_id, star_lots=items, lots_active=True, managed_lot_ids=managed_ids, last_auto_deact_reason=None, last_lot_toggle_report=f'star_act_all: {_lot_report_short(rep)}', last_lot_toggle_ts=int(time.time()))
    try:
        bot.answer_callback_query(call.id, 'Лоты включены.')
    except Exception:
        pass
    _open_stars(bot, call)
def _star_deact_all(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    items = cfg.get('star_lots') or []
    if not items:
        bot.answer_callback_query(call.id, 'Список пуст. Запустите «Автодобавление лотов» или добавьте LOT вручную.', show_alert=True)
        return
    rep = {'ok': [], 'skip': [], 'err': []}
    if _CARDINAL_REF is not None:
        rep = _apply_star_lots_state(_CARDINAL_REF, items, False)
    for it in items:
        it['active'] = False
    managed_ids = _merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [x.get('lot_id') for x in items], _ids_from_report(rep))
    _set_cfg(chat_id, star_lots=items, lots_active=False, managed_lot_ids=managed_ids, last_lot_toggle_report=f'star_deact_all: {_lot_report_short(rep)}', last_lot_toggle_ts=int(time.time()))
    try:
        bot.answer_callback_query(call.id, 'Лоты выключены.')
    except Exception:
        pass
    _open_stars(bot, call)
def _star_toggle(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    lot_id = int(call.data.split(':')[-1])
    items = cfg.get('star_lots') or []
    found = None
    for it in items:
        if int(it.get('lot_id', 0)) == lot_id:
            found = it
            break
    if not found:
        bot.answer_callback_query(call.id, 'Лот не найден.', show_alert=True)
        return
    enabled = not bool(found.get('active'))
    ok = True
    if _CARDINAL_REF is not None:
        if enabled:
            ok = _activate_lot(_CARDINAL_REF, lot_id, trusted=True)
        else:
            ok = _deactivate_lot(_CARDINAL_REF, lot_id, trusted=True)
    found['active'] = enabled if ok else bool(found.get('active'))
    managed_ids = _merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [lot_id])
    _set_cfg(chat_id, star_lots=items, lots_active=any((bool(x.get('active')) for x in items)), managed_lot_ids=managed_ids, last_auto_deact_reason=None if enabled and ok else cfg.get('last_auto_deact_reason'), last_lot_toggle_report=f'star_toggle lot={lot_id} enabled={enabled} ok={ok}', last_lot_toggle_ts=int(time.time()))
    try:
        bot.answer_callback_query(call.id, 'Лот включён.' if enabled and ok else 'Лот выключен.' if ok else 'Не удалось изменить лот, смотри лог.', show_alert=not ok)
    except Exception:
        pass
    _open_stars(bot, call)
def _star_delete(bot, call):
    chat_id = call.message.chat.id
    lot_id = int(call.data.split(':')[-1])
    cfg = _get_cfg(chat_id)
    items = [x for x in cfg.get('star_lots') or [] if int(x.get('lot_id', 0)) != lot_id]
    _set_cfg(chat_id, star_lots=items)
    _open_stars(bot, call)
_MAINT_EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv('FTS_MAINT_WORKERS', '1')), thread_name_prefix='FTS-MAINT')
_MAINT_JOBS = {}
_MAINT_JOBS_LOCK = threading.Lock()
def _schedule_maint_job(key, fn, *args, **kwargs):
    with _MAINT_JOBS_LOCK:
        old = _MAINT_JOBS.get(key)
        if old is not None and (not old.done()):
            return False
        fut = _MAINT_EXECUTOR.submit(fn, *args, **kwargs)
        _MAINT_JOBS[key] = fut
        def _cleanup(_f):
            with _MAINT_JOBS_LOCK:
                cur = _MAINT_JOBS.get(key)
                if cur is _f:
                    _MAINT_JOBS.pop(key, None)
        fut.add_done_callback(_cleanup)
        return True
def _autoadd_report_text(rep):
    lines = []
    lines.append('<b>🤖 Автодобавление лотов</b>')
    lines.append('')
    lines.append(f"Категория: <code>{_h(rep.get('category_id'))}</code>")
    lines.append(f"Найдено лотов в категории: <b>{_h(rep.get('found', 0))}</b>")
    if rep.get('fallback'):
        lines.append('Источник: <b>fallback через профиль/Cardinal</b> — пригодится для выключенных лотов.')
    lines.append(f"Добавлено: <b>{_h(rep.get('added', 0))}</b>")
    lines.append(f"Обновлено: <b>{_h(rep.get('updated', 0))}</b>")
    lines.append(f"Без изменений: <b>{_h(rep.get('unchanged', 0))}</b>")
    lines.append(f"Пропущено: <b>{_h(rep.get('skipped', 0))}</b>")
    try:
        lines.append(f"Время: <code>{float(rep.get('elapsed_sec', 0)):.2f}s</code>")
    except Exception:
        lines.append(f"Время: <code>{_h(rep.get('elapsed_sec', 0))}s</code>")
    preview = rep.get('preview') or []
    if preview:
        lines.append('')
        lines.append('<b>Что получилось (первые 25):</b>')
        for row in preview[:25]:
            qty, lot_id, active, status = row
            st = '🟢 ON' if active else '🔴 OFF'
            lines.append(f'• <b>{_h(qty)}</b>⭐ — LOT <code>{_h(lot_id)}</code> — {st} — <i>{_h(status)}</i>')
        more = rep.get('preview_more', 0)
        if more:
            lines.append(f'… и ещё {_h(more)} лот(ов)')
    return '\n'.join(lines)
def _autoadd_star_lots(cardinal, chat_id, category_id):
    t0 = time.time()
    category_id = int(category_id)
    logger.info(f'[AUTOADD] start: chat_id={chat_id} category_id={category_id}')
    cfg = _get_cfg(chat_id)
    existing_list = cfg.get('star_lots') or []
    existing_map = {}
    for it in existing_list:
        try:
            lid = int(it.get('lot_id'))
            existing_map[lid] = {'lot_id': lid, 'qty': int(it.get('qty') or 0) if it.get('qty') is not None else None, 'active': bool(it.get('active', False)), 'autodump_min_price': it.get('autodump_min_price')}
        except Exception:
            continue
    raw_lots = _get_my_subcategory_lots_safe(cardinal, category_id)
    lot_pairs = []
    for lot in raw_lots:
        try:
            lid = getattr(lot, 'id', None)
            if lid is not None:
                lot_pairs.append((int(lid), lot))
        except Exception:
            continue
    fallback_used = False
    if not lot_pairs:
        try:
            fallback_map = _get_my_lots_by_category(cardinal, category_id) or {}
            lot_pairs = [(int(lid), lot) for lid, lot in fallback_map.items()]
            fallback_used = bool(lot_pairs)
        except Exception as e:
            logger.warning(f'[AUTOADD] profile fallback failed: {e}')
    logger.info(f'[AUTOADD] found lots in subcategory {category_id}: direct={len(raw_lots)} usable={len(lot_pairs)} fallback={fallback_used}')
    added = updated = unchanged = skipped = errors = 0
    preview_rows = []
    seen_ids = set()
    for lot_id, lot in lot_pairs:
        try:
            if lot_id in seen_ids:
                continue
            seen_ids.add(lot_id)
            title = (getattr(lot, 'title', None) or getattr(lot, 'name', None) or getattr(lot, 'description', None) or '').strip()
            active_now = bool(getattr(lot, 'active', True))
            qty = _extract_qty_from_title(title)
            if qty is None:
                try:
                    fields = cardinal.account.get_lot_fields(int(lot_id))
                    title2 = (getattr(fields, 'title', None) or getattr(fields, 'name', None) or getattr(fields, 'description', None) or '').strip()
                    if title2:
                        title = title2
                        active_now = bool(getattr(fields, 'active', active_now))
                        qty = _extract_qty_from_title(title2)
                except Exception:
                    pass
            if qty is None:
                skipped += 1
                logger.info(f'[AUTOADD] skip lot={lot_id}: qty_not_found title={title!r}')
                continue
            qty = int(qty)
            if qty < FTS_MIN_STARS:
                skipped += 1
                existing_map.pop(lot_id, None)
                preview_rows.append((qty, lot_id, active_now, f'пропущен (меньше {FTS_MIN_STARS}⭐)'))
                logger.info(f'[AUTOADD] skip lot={lot_id}: qty<{FTS_MIN_STARS} qty={qty} title={title!r}')
                continue
            if lot_id not in existing_map:
                existing_map[lot_id] = {'lot_id': lot_id, 'qty': qty, 'active': active_now}
                added += 1
                preview_rows.append((qty, lot_id, active_now, 'добавлен'))
                logger.info(f'[AUTOADD] add: lot={lot_id} qty={qty} active={active_now} title={title!r}')
            else:
                cur = existing_map[lot_id]
                before_qty = int(cur.get('qty') or 0)
                before_active = bool(cur.get('active', False))
                if before_qty == qty and before_active == active_now:
                    unchanged += 1
                    preview_rows.append((qty, lot_id, active_now, 'без изменений'))
                else:
                    cur['qty'] = qty
                    cur['active'] = active_now
                    updated += 1
                    preview_rows.append((qty, lot_id, active_now, 'обновлён'))
                    logger.info(f'[AUTOADD] update: lot={lot_id} qty {before_qty}->{qty} active {before_active}->{active_now} title={title!r}')
        except Exception as e:
            errors += 1
            logger.warning(f'[AUTOADD] error lot={lot_id}: {e}')
    out_list = sorted([v for v in existing_map.values() if isinstance(v, dict) and v.get('lot_id') and (int(v.get('qty') or 0) >= FTS_MIN_STARS)], key=lambda x: (int(x.get('qty') or 0), int(x.get('lot_id') or 0)))
    managed_ids = _merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [x.get('lot_id') for x in out_list], seen_ids)
    _set_cfg(chat_id, star_lots=out_list, managed_lot_ids=managed_ids)
    elapsed = time.time() - t0
    logger.info(f'[AUTOADD] done: chat_id={chat_id} category_id={category_id} found={len(lot_pairs)} added={added} updated={updated} unchanged={unchanged} skipped={skipped} errors={errors} fallback={fallback_used} elapsed={elapsed:.2f}s')
    preview_sorted = sorted(preview_rows, key=lambda r: (int(r[0]), int(r[1])))
    return {'category_id': category_id, 'found': len(lot_pairs), 'added': added, 'updated': updated, 'unchanged': unchanged, 'skipped': skipped, 'errors': errors, 'fallback': fallback_used, 'elapsed_sec': elapsed, 'preview': preview_sorted[:25], 'preview_more': max(0, len(preview_sorted) - 25)}
def _cb_star_autoadd(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Запускаю автоскан…')
    except Exception:
        pass
    job_key = f'autoadd:{chat_id}'
    def _run():
        try:
            bot.send_message(chat_id, f'🔎 Сканирую лоты категории <code>{FNP_STARS_CATEGORY_ID}</code>…', parse_mode='HTML')
        except Exception:
            pass
        rep = _autoadd_star_lots(cardinal, chat_id, FNP_STARS_CATEGORY_ID)
        try:
            bot.send_message(chat_id, _autoadd_report_text(rep), parse_mode='HTML', disable_web_page_preview=True)
        except Exception as e:
            logger.warning(f'[AUTOADD] send report failed: {e}')
        try:
            _safe_edit(bot, chat_id, call.message.id, _stars_text(chat_id), _stars_kb(chat_id))
        except Exception:
            pass
    ok = _schedule_maint_job(job_key, _run)
    if not ok:
        try:
            bot.answer_callback_query(call.id, 'Уже выполняется автоскан. Подожди завершения.', show_alert=True)
        except Exception:
            pass
def _toggle_price_notifications(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'price_change_notifications', True)
    _set_cfg(chat_id, price_change_notifications=v)
    try:
        bot.answer_callback_query(call.id, 'Уведомления цен включены.' if v else 'Уведомления цен выключены.')
    except Exception:
        pass
    _open_notifications(bot, call)
def _toggle_auto_price_fragment(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'auto_price_fragment_enabled', False)
    _set_cfg(chat_id, auto_price_fragment_enabled=v)
    try:
        bot.answer_callback_query(call.id, 'Автоизменение цен включено.' if v else 'Автоизменение цен выключено.')
    except Exception:
        pass
    _open_pricing(bot, call)
def _toggle_autodump(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'autodump_enabled', False)
    _set_cfg(chat_id, autodump_enabled=v)
    try:
        bot.answer_callback_query(call.id, 'Автодемп включён.' if v else 'Автодемп выключен.')
    except Exception:
        pass
    _open_autodump(bot, call)
def _toggle_autodump_notifications(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'autodump_notifications', True)
    _set_cfg(chat_id, autodump_notifications=v)
    try:
        bot.answer_callback_query(call.id, 'Уведомления автодемпа включены.' if v else 'Уведомления автодемпа выключены.')
    except Exception:
        pass
    try:
        _open_autodump(bot, call)
    except Exception:
        _open_notifications(bot, call)
def _ask_autodump_interval(bot, call):
    chat_id = call.message.chat.id
    cur = int(_get_cfg(chat_id).get('autodump_interval_sec', 1800)) // 60
    _fsm[chat_id] = {'step': 'set_autodump_interval'}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, f'Введите интервал автодемпа в минутах от 10 до 1440 (сейчас {cur}).\nПример: 30\n(или /cancel)', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _cb_autodump_run(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Проверяю цены конкурентов…')
    except Exception:
        pass
    ok, msg = _apply_autodump(cardinal, chat_id, manual=True)
    bot.send_message(chat_id, msg, parse_mode='HTML')
    try:
        _open_autodump(bot, call)
    except Exception:
        pass
def _toggle_liteserver_retry(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get('retry_liteserver', LITESERVER_RETRY_DEFAULT))
    _set_cfg(chat_id, retry_liteserver=new_state)
    try:
        bot.answer_callback_query(call.id, 'LiteServer-ретрай включён.' if new_state else 'LiteServer-ретрай выключен.')
    except Exception:
        pass
    _open_mini_settings(bot, call)
def _toggle_autosend_plus(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get('auto_send_without_plus', False))
    _set_cfg(chat_id, auto_send_without_plus=new_state)
    try:
        bot.answer_callback_query(call.id, "Автоотправка без '+' включена." if new_state else "Теперь требуется подтверждение '+'.")
    except Exception:
        pass
    _open_mini_settings(bot, call)
def _ask_order_timer(bot, call, *, step, cfg_key, title, default_sec, min_min, max_min, example):
    chat_id = call.message.chat.id
    cur_min = max(1, int((_get_cfg(chat_id).get(cfg_key, default_sec) or default_sec) // 60))
    _fsm[chat_id] = {'step': step, 'timer_cfg_key': cfg_key, 'timer_title': title, 'timer_min': min_min, 'timer_max': max_min}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, f'Введите время в минутах для «{title}» от {min_min} до {max_min}.\nСейчас: {cur_min} мин. Пример: {example}\n(или /cancel)', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _ask_order_watch_interval(bot, call):
    _ask_order_timer(bot, call, step='set_order_watch_interval', cfg_key='order_watch_interval_sec', title='интервал проверки заказов', default_sec=ORDER_WATCH_INTERVAL_DEFAULT, min_min=1, max_min=1440, example=5)
def _ask_order_wait_reminder(bot, call):
    _ask_order_timer(bot, call, step='set_order_wait_reminder', cfg_key='order_wait_reminder_sec', title='напоминание, если заказ долго ждёт', default_sec=ORDER_WAIT_REMINDER_DEFAULT, min_min=2, max_min=1440, example=15)
def _ask_review_reminder_time(bot, call):
    _ask_order_timer(bot, call, step='set_review_reminder_time', cfg_key='order_review_reminder_sec', title='напоминание об отзыве после отправки', default_sec=ORDER_REVIEW_REMINDER_DEFAULT, min_min=5, max_min=10080, example=30)
def _toggle_order_watch(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'order_watch_enabled', True)
    _set_cfg(chat_id, order_watch_enabled=v)
    try:
        bot.answer_callback_query(call.id, 'Проверка зависших заказов включена.' if v else 'Проверка зависших заказов выключена.')
    except Exception:
        pass
    _open_order_tools(bot, call)
def _toggle_review_reminder(bot, call):
    chat_id = call.message.chat.id
    v = not _cfg_bool(_get_cfg(chat_id), 'order_review_reminder_enabled', True)
    _set_cfg(chat_id, order_review_reminder_enabled=v)
    try:
        bot.answer_callback_query(call.id, 'Напоминание об отзыве включено.' if v else 'Напоминание об отзыве выключено.')
    except Exception:
        pass
    _open_order_tools(bot, call)
def _cb_order_watch_run(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Проверяю зависшие заказы…')
    except Exception:
        pass
    def _run():
        w, r, _ = _order_health_check(cardinal, chat_id, manual=True)
        try:
            bot.send_message(chat_id, f'✅ Проверка заказов завершена. Напоминаний по ожиданию: {w}; просьб об отзыве: {r}.')
        except Exception:
            pass
    if not _schedule_job(f'order-watch:{chat_id}', _run):
        try:
            bot.answer_callback_query(call.id, 'Проверка уже выполняется.', show_alert=True)
        except Exception:
            pass
def _toggle_queue_mode(bot, call):
    chat_id = call.message.chat.id
    cur = _queue_mode(chat_id)
    nxt = 2 if cur == 1 else 3 if cur == 2 else 1
    _set_cfg(chat_id, queue_mode=nxt)
    try:
        bot.answer_callback_query(call.id, f'Очередь: {_queue_mode_label(nxt, _queue_timeout_sec(chat_id))}')
    except Exception:
        pass
    _open_mini_settings(bot, call)
def _toggle_stars_currency(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cur = _normalize_stars_currency(cfg.get('stars_currency'))
    nxt = FTS_CURRENCY_USDT_TON if cur == FTS_CURRENCY_TON else FTS_CURRENCY_TON
    _set_cfg(chat_id, stars_currency=nxt)
    try:
        bot.answer_callback_query(call.id, f'Оплата звёзд: {_stars_currency_label(nxt)}')
    except Exception:
        pass
    try:
        _open_settings(bot, call)
    except Exception:
        _open_mini_settings(bot, call)
def _toggle_usdt_fallback(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not _cfg_bool(cfg, 'usdt_fallback_to_ton', False)
    _set_cfg(chat_id, usdt_fallback_to_ton=new_state)
    try:
        bot.answer_callback_query(call.id, 'Автопереход USDT → TON включён.' if new_state else 'Автопереход USDT → TON выключен.')
    except Exception:
        pass
    _open_mini_settings(bot, call)
def _toggle_username_check(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get('skip_username_check', False))
    _set_cfg(chat_id, skip_username_check=new_state)
    try:
        bot.answer_callback_query(call.id, 'Проверка @username при вводе отключена (проверим при отправке).' if new_state else 'Проверка @username при вводе включена.')
    except Exception:
        pass
    _open_mini_settings(bot, call)
def _toggle_lots(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    if not cfg.get('plugin_enabled', True):
        bot.answer_callback_query(call.id, 'Плагин выключен. Сначала включите его в настройках.', show_alert=True)
        return
    _state_text, state_bool = _lots_state_summary(cfg)
    current = state_bool is True
    desired = not current
    star_lots = cfg.get('star_lots') or []
    known_ids = _managed_lot_ids_from_cfg(cfg)
    rep = {'ok': [], 'skip': [], 'err': []}
    if _CARDINAL_REF is not None:
        if star_lots:
            rep = _apply_star_lots_state(_CARDINAL_REF, star_lots, desired)
            for it in star_lots:
                it['active'] = bool(desired)
            known_ids = _merge_lot_ids(known_ids, [x.get('lot_id') for x in star_lots], _ids_from_report(rep))
        else:
            rep = _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, desired, known_lot_ids=known_ids)
            known_ids = _merge_lot_ids(known_ids, _ids_from_report(rep))
            if desired and (not known_ids) and (not (rep.get('ok') or rep.get('skip'))):
                bot.answer_callback_query(call.id, 'Не нашёл лоты для включения. Запустите «Автодобавление лотов» в настройке лотов или добавьте LOT вручную.', show_alert=True)
                _open_settings(bot, call)
                return
    updates = {'lots_active': desired, 'managed_lot_ids': known_ids, 'last_lot_toggle_report': f'settings_toggle enabled={desired}: {_lot_report_short(rep)}', 'last_lot_toggle_ts': int(time.time())}
    if star_lots:
        updates['star_lots'] = star_lots
    if desired:
        updates['last_auto_deact_reason'] = None
    _set_cfg(chat_id, **updates)
    suffix = f' ({_lot_report_short(rep)})' if _CARDINAL_REF is not None else ''
    bot.answer_callback_query(call.id, ('Лоты включены.' if desired else 'Лоты выключены.') + suffix, show_alert=False)
    _open_settings(bot, call)
def _toggle_refund(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cfg = _set_cfg(chat_id, auto_refund=not bool(cfg.get('auto_refund', False)))
    bot.answer_callback_query(call.id, 'Автовозврат включён.' if cfg['auto_refund'] else 'Автовозврат выключен.', show_alert=False)
    _open_settings(bot, call)
def _toggle_deact(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cfg = _set_cfg(chat_id, auto_deactivate=not bool(cfg.get('auto_deactivate', True)))
    bot.answer_callback_query(call.id, 'Автодеактивация включена.' if cfg['auto_deactivate'] else 'Автодеактивация выключена.', show_alert=False)
    _open_settings(bot, call)
def _toggle_balance_lot_filter(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cfg = _set_cfg(chat_id, balance_lot_filter_enabled=not bool(cfg.get('balance_lot_filter_enabled', True)))
    try:
        bot.answer_callback_query(call.id, 'Фильтр по балансу включён.' if cfg['balance_lot_filter_enabled'] else 'Фильтр по балансу выключен.')
    except Exception:
        pass
    _open_pricing(bot, call)
def _cb_balance_lot_filter_run(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Проверяю лоты по балансу…')
    except Exception:
        pass
    ok, msg = _apply_balance_lot_filter(cardinal, chat_id, _get_cfg(chat_id), force=True)
    bot.send_message(chat_id, ('✅ ' if ok else '⚠️ ') + _h(msg), parse_mode='HTML')
    try:
        _open_pricing(bot, call)
    except Exception:
        pass
def _refresh(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    if cfg.get('fragment_jwt'):
        ver, bal, usdt, raw = _check_fragment_wallet(cfg['fragment_jwt'])
        if ver is not None or bal is not None or usdt is not None or (raw is not None):
            _set_cfg(chat_id, wallet_version=ver, balance_ton=round(bal, 6) if isinstance(bal, (int, float)) else None, balance_usdt=round(usdt, 6) if isinstance(usdt, (int, float)) else None, last_wallet_raw=raw)
    if _CARDINAL_REF is not None:
        cfg2 = _get_cfg(chat_id)
        _maybe_auto_price_update(_CARDINAL_REF, chat_id)
        _maybe_autodump_update(_CARDINAL_REF, chat_id)
        _apply_balance_lot_filter(_CARDINAL_REF, chat_id, cfg2)
    _open_settings(bot, call)
    try:
        bot.answer_callback_query(call.id, 'Обновлено.', show_alert=False)
    except Exception:
        pass
def _ask_set_min_balance(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cur_code = _normalize_stars_currency(cfg.get('stars_currency'))
    key, label, default = ('min_balance_usdt', 'USDT', FNP_MIN_BALANCE_USDT) if cur_code == FTS_CURRENCY_USDT_TON else ('min_balance_ton', 'TON', FNP_MIN_BALANCE_TON)
    _fsm[chat_id] = {'step': 'set_min_balance', 'balance_key': key, 'balance_label': label}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    cur = cfg.get(key, default)
    bot.send_message(chat_id, f'Введите новый порог баланса в {label} (сейчас {cur}). Можно с точкой или запятой. Пример: 5.5\n(или /cancel)', reply_markup=_kb_cancel_fsm())
def _cancel_cmd(cardinal, chat_id):
    st = _fsm.get(chat_id) or {}
    if st.get('step') in {'set_jwt', 'saves_import'}:
        _cleanup_fsm_msgs(cardinal.telegram.bot, chat_id, st)
    _fsm.pop(chat_id, None)
    if _has_queue(chat_id):
        _pop_current(chat_id, keep_prompted=False)
    _safe_send(cardinal, chat_id, '❌ Отменено. Текущий шаг сброшен.')
    if _has_queue(chat_id):
        nxt = _current(chat_id)
        qty = int(nxt.get('qty', 50))
        _safe_send(cardinal, chat_id, f'Следующий заказ в очереди: {qty}⭐.\nПришлите тег в формате @username одной строкой.')
def _star_price_start(bot, call):
    chat_id = call.message.chat.id
    try:
        lot_id = int(call.data.split(':')[-1])
    except Exception:
        try:
            bot.answer_callback_query(call.id, 'Некорректный LOT_ID', show_alert=True)
        except Exception:
            pass
        return
    if _CARDINAL_REF is not None and (not _is_stars_lot(_CARDINAL_REF, lot_id)):
        try:
            bot.answer_callback_query(call.id, f'LOT {lot_id} не из категории {FNP_STARS_CATEGORY_ID}', show_alert=True)
        except Exception:
            pass
        return
    price, cur = _get_lot_price_currency(_CARDINAL_REF, lot_id) if _CARDINAL_REF else (None, 'RUB')
    price_txt = f'{price:.2f}' if isinstance(price, (int, float)) else '—'
    _fsm[chat_id] = {'step': 'star_price_value', 'lot_id': lot_id, 'currency': cur}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, f'LOT {lot_id}\nТекущая цена: <b>{price_txt} {cur}</b>\n\nВведите <b>новую цену</b> (число). Пример: 149 или 149.99\n(или /cancel)', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
def _star_dump_floor_start(bot, call):
    chat_id = call.message.chat.id
    try:
        lot_id = int(call.data.split(':')[-1])
    except Exception:
        try:
            bot.answer_callback_query(call.id, 'Некорректный LOT_ID', show_alert=True)
        except Exception:
            pass
        return
    cfg = _get_cfg(chat_id)
    floor = None
    qty = None
    for it in cfg.get('star_lots') or []:
        if int(it.get('lot_id', 0)) == lot_id:
            floor = it.get('autodump_min_price')
            qty = it.get('qty')
            break
    price, cur = _get_lot_price_currency(_CARDINAL_REF, lot_id) if _CARDINAL_REF else (None, 'RUB')
    curtxt = getattr(cur, 'name', str(cur))
    _fsm[chat_id] = {'step': 'autodump_floor_value', 'lot_id': lot_id, 'currency': curtxt}
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, f"LOT {lot_id} ({qty or '?'}⭐)\nТекущая цена: <b>{(_format_currency(price, curtxt) if price is not None else '—')}</b>\nПорог демпа сейчас: <b>{(_format_currency(floor, curtxt) if isinstance(floor, (int, float)) else 'не задан')}</b>\n\nВведите минимальную цену, ниже которой автодемп не опустит лот. 0 или '-' — убрать порог.\n(или /cancel)", parse_mode='HTML', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or {}
    st['prompt_msg_id'] = getattr(m, 'message_id', None)
    _fsm[chat_id] = st
CLEAN_USER_MSGS = bool(int(os.getenv('FTS_Plugin_CLEAN_USER_MSGS', '0')))
def _ask_set_jwt(bot, call):
    chat_id = call.message.chat.id
    st = {'step': 'set_jwt'}
    _fsm[chat_id] = st
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
    m = bot.send_message(chat_id, 'Пришлите готовый JWT текстом или файлом (.txt / .json). В JSON токен может лежать в ключах token/jwt/access/authorization. (или /cancel)', reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id) or st
    _track_fsm_mid(st, getattr(m, 'message_id', None))
    _fsm[chat_id] = st
def _del_jwt(bot, call):
    chat_id = call.message.chat.id
    _set_cfg(chat_id, fragment_jwt=None, wallet_version=None, balance_ton=None, balance_usdt=None, last_wallet_raw=None)
    try:
        bot.answer_callback_query(call.id, 'Токен удалён.')
    except Exception:
        pass
    _open_token(bot, call)
def _clean_jwt_text(s):
    try:
        s = (s or '').strip().strip('"').strip("'")
        s = _re.sub('^(?:JWT|Bearer)\\s+', '', s, flags=_re.I)
        s = _re.sub('\\s+', '', s)
        return s
    except Exception:
        return s or ''
def _is_jwt_like(s):
    if not s or s.count('.') < 2:
        return False
    return bool(_re.fullmatch('[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+', s))
def _find_jwt_in_json(obj):
    CAND_KEYS = {'token', 'jwt', 'access', 'authorization', 'Authorization', 'auth', 'detail'}
    try:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in CAND_KEYS and isinstance(v, str):
                    cand = _clean_jwt_text(v)
                    if _is_jwt_like(cand) or len(cand) > 16:
                        return cand
            for subk in ('data', 'result', 'payload'):
                if subk in obj:
                    v = _find_jwt_in_json(obj[subk])
                    if v:
                        return v
            for v in obj.values():
                vv = _find_jwt_in_json(v)
                if vv:
                    return vv
        elif isinstance(obj, list):
            for v in obj:
                vv = _find_jwt_in_json(v)
                if vv:
                    return vv
    except Exception:
        pass
    return None
def _handle_fsm(message, cardinal):
    chat_id = message.chat.id
    text = (message.text or '').strip()
    state = _fsm.get(chat_id) or {}
    if state.get('step') == 'set_jwt':
        _track_fsm_mid(state, getattr(message, 'message_id', None))
    if state.get('step') == 'msg_edit_value':
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            try:
                pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
                if pmid:
                    cardinal.telegram.bot.delete_message(chat_id, pmid)
            except Exception:
                pass
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        key = state.get('msg_key')
        cfg = _get_cfg(chat_id)
        tpls = cfg.get('templates') or _default_templates()
        tpls[key] = text
        _set_cfg(chat_id, templates=tpls)
        try:
            pmid = state.get('prompt_msg_id')
            if pmid:
                cardinal.telegram.bot.delete_message(chat_id, pmid)
            cardinal.telegram.bot.delete_message(chat_id, message.message_id)
        except Exception:
            pass
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, '✅ Шаблон обновлён.')
        try:
            _open_messages(cardinal.telegram.bot, type('obj', (), {'message': type('m', (), {'chat': type('c', (), {'id': chat_id})(), 'id': message.message_id})(), 'id': ''}))
        except Exception:
            pass
        return
    if state.get('step') == 'unit_star_price_value':
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            m_cancel = cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(m_cancel, 'message_id', None))
            return
        t = text.replace(',', '.').strip()
        try:
            unit_price = float(t)
            if unit_price <= 0:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите положительное число. Пример: 1.25')
            return
        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        cfg = _get_cfg(chat_id)
        if _CARDINAL_REF is None:
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Внутренняя ошибка: нет ссылки на Cardinal.')
            return
        rows, skipped = _collect_unit_price_targets(_CARDINAL_REF, cfg, unit_price)
        preview = _unit_price_preview_text(unit_price, rows, skipped)
        st = _fsm.get(chat_id) or {}
        st['unit_price'] = unit_price
        st['unit_rows'] = rows
        st['step'] = 'unit_price_preview'
        _fsm[chat_id] = st
        cardinal.telegram.bot.send_message(chat_id, preview, parse_mode='HTML', reply_markup=_kb_unit_price_preview())
        return
    if state.get('step') == 'markup_percent':
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            m_cancel = cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(m_cancel, 'message_id', None))
            return
        t = text.replace(',', '.').strip()
        try:
            percent = float(t)
            if not -90.0 <= percent <= 500.0:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите число (проценты), например 10 или 12.5. Диапазон: от -90 до 500.')
            return
        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        cfg = _get_cfg(chat_id)
        if _CARDINAL_REF is None:
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Внутренняя ошибка: нет ссылки на Cardinal.')
            return
        rows = _collect_markup_targets(_CARDINAL_REF, cfg, percent)
        preview = _markup_preview_text(percent, rows)
        st = _fsm.get(chat_id) or {}
        st['markup_percent'] = percent
        st['markup_rows'] = rows
        st['step'] = 'markup_preview'
        _fsm[chat_id] = st
        cardinal.telegram.bot.send_message(chat_id, preview, parse_mode='HTML', reply_markup=_kb_markup_preview())
        return
    if state.get('step') == 'star_price_value':
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            m_cancel = cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(m_cancel, 'message_id', None))
            return
        lot_id = int(state.get('lot_id'))
        cur = state.get('currency') or 'RUB'
        t = text.replace(',', '.').strip()
        try:
            new_price = float(t)
            if new_price <= 0:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите положительное число (цена), например 149 или 149.99.')
            return
        old_price, cur_detected = _get_lot_price_currency(cardinal, lot_id)
        cur = cur_detected or cur
        try:
            fields = cardinal.account.get_lot_fields(lot_id)
            if not fields:
                raise RuntimeError('Лот недоступен.')
            if not _is_stars_lot(cardinal, lot_id):
                raise RuntimeError(f'Лот не из категории {FNP_STARS_CATEGORY_ID}.')
            set_ok = False
            for price_attr in ('price', 'cost', 'amount', 'price_rub'):
                if hasattr(fields, price_attr):
                    setattr(fields, price_attr, float(new_price))
                    set_ok = True
                    break
            if not set_ok:
                raise RuntimeError('Не удалось изменить цену в полях лота.')
            cardinal.account.save_lot(fields)
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, f"✅ Цена обновлена для LOT {lot_id}: {(_format_currency(old_price, cur) if old_price is not None else '')} → <b>{_format_currency(new_price, cur)}</b>", parse_mode='HTML')
        except Exception as e:
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, f'❌ Не удалось сохранить цену: {e}')
        finally:
            _fsm.pop(chat_id, None)
            try:
                _open_stars(cardinal.telegram.bot, type('obj', (), {'message': type('m', (), {'chat': type('c', (), {'id': chat_id})(), 'id': message.message_id})(), 'id': ''}))
            except Exception:
                pass
        return
    if state.get('step') == 'autodump_floor_value':
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        lot_id = int(state.get('lot_id'))
        cur = state.get('currency') or 'RUB'
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            return
        raw = text.replace(',', '.').strip().lower()
        try:
            val = None if raw in ('0', '-', 'нет', 'off', 'none') else float(raw)
            if val is not None and val < 0:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите число ≥ 0, 0 или '-' для удаления порога.")
            return
        cfg = _get_cfg(chat_id)
        items = cfg.get('star_lots') or []
        ok = False
        for it in items:
            if int(it.get('lot_id', 0)) == lot_id:
                if val is None:
                    it.pop('autodump_min_price', None)
                else:
                    it['autodump_min_price'] = round(float(val), 2)
                ok = True
                break
        _set_cfg(chat_id, star_lots=items)
        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, f'✅ Порог демпа для LOT {lot_id}: <b>{_format_currency(val, cur)}</b>' if val is not None else f'✅ Порог демпа для LOT {lot_id} удалён.', parse_mode='HTML')
        try:
            _open_stars(cardinal.telegram.bot, type('obj', (), {'message': type('m', (), {'chat': type('c', (), {'id': chat_id})(), 'id': message.message_id})(), 'id': ''}))
        except Exception:
            pass
        return
    if state.get('step') == 'star_add_qty':
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        try:
            qty = int(text.strip())
            if qty < 50 or qty > 1000000:
                raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите целое число от 50 до 1 000 000, либо /cancel.')
            return
        state['new_qty'] = qty
        state['step'] = 'star_add_lotid'
        _fsm[chat_id] = state
        cardinal.telegram.bot.send_message(chat_id, 'Теперь введите <b>LOT_ID</b> (целое положительное), или /cancel:', parse_mode='HTML', reply_markup=_kb_cancel_fsm())
        return
    if state.get('step') == 'star_add_lotid':
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        try:
            lot_id = int(text.strip())
            if lot_id <= 0:
                raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите положительное целое (LOT_ID), либо /cancel.')
            return
        if _CARDINAL_REF is not None and (not _is_stars_lot(_CARDINAL_REF, lot_id)):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, f'❌ LOT {lot_id} не относится к категории {FNP_STARS_CATEGORY_ID}. Добавление отклонено.')
            return
        qty = int(state.get('new_qty'))
        cfg = _get_cfg(chat_id)
        items = cfg.get('star_lots') or []
        updated = False
        for it in items:
            if int(it.get('lot_id', 0)) == lot_id:
                it['qty'] = qty
                it['active'] = True
                updated = True
                break
        if not updated:
            items.append({'qty': qty, 'lot_id': lot_id, 'active': True})
        _set_cfg(chat_id, star_lots=items, lots_active=True, managed_lot_ids=_merge_lot_ids(_managed_lot_ids_from_cfg(cfg), [lot_id]), last_auto_deact_reason=None)
        _fsm.pop(chat_id, None)
        if _CARDINAL_REF is not None:
            _activate_lot(_CARDINAL_REF, lot_id, trusted=True)
        cardinal.telegram.bot.send_message(chat_id, f'✅ Добавлено: {qty} ⭐ (LOT {lot_id}). Управляйте в «⭐ Звёзды».')
        return
    if state.get('step') in {'set_order_watch_interval', 'set_order_wait_reminder', 'set_review_reminder_time'}:
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        try:
            mins = int(float(text.replace(',', '.').strip()))
            min_m = int(state.get('timer_min') or 1)
            max_m = int(state.get('timer_max') or 1440)
            if mins < min_m or mins > max_m:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, f"⚠️ Введите число минут от {state.get('timer_min') or 1} до {state.get('timer_max') or 1440}.")
            return
        key = state.get('timer_cfg_key') or 'order_watch_interval_sec'
        title = state.get('timer_title') or 'таймер'
        _set_cfg(chat_id, **{key: mins * 60})
        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, f'✅ Сохранено: {title} — {mins} мин.')
        try:
            _open_order_tools(cardinal.telegram.bot, type('obj', (), {'message': type('m', (), {'chat': type('c', (), {'id': chat_id})(), 'id': getattr(message, 'message_id', 0)})(), 'id': ''}))
        except Exception:
            pass
        return
    if state.get('step') == 'set_autodump_interval':
        pmid = (_fsm.get(chat_id) or {}).get('prompt_msg_id')
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        try:
            mins = int(float(text.replace(',', '.').strip()))
            if mins < 10 or mins > 1440:
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите число минут от 10 до 1440. Пример: 30')
            return
        _set_cfg(chat_id, autodump_interval_sec=mins * 60)
        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, f'✅ Интервал автодемпа сохранён: {mins} мин.')
        try:
            _open_autodump(cardinal.telegram.bot, type('obj', (), {'message': type('m', (), {'chat': type('c', (), {'id': chat_id})(), 'id': getattr(message, 'message_id', 0)})(), 'id': ''}))
        except Exception:
            pass
        return
    if state.get('step') == 'set_min_balance':
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        t = text.replace(',', '.').strip()
        try:
            val = float(t)
            if val < 0:
                raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Введите неотрицательное число. Пример: 4.2')
            return
        key = state.get('balance_key') or 'min_balance_ton'
        label = state.get('balance_label') or ('USDT' if key == 'min_balance_usdt' else 'TON')
        _set_cfg(chat_id, **{key: val})
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, f'✅ Порог сохранён: {val} {label}')
        _log('info', f'MIN BALANCE set to {val} {label}')
        return
    if state.get('step') == 'saves_import':
        _track_fsm_mid(state, getattr(message, 'message_id', None))
        if text.lower() in ('/cancel', 'cancel', 'отмена'):
            _cleanup_fsm_msgs(cardinal.telegram.bot, chat_id, state)
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Импорт отменён.')
            return
        raw_text = ''
        if getattr(message, 'document', None):
            try:
                if message.document.file_size and message.document.file_size > 5000000:
                    _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
                    cardinal.telegram.bot.send_message(chat_id, '⚠️ Файл слишком большой (>5MB). Пришлите меньший settings.json или /cancel.')
                    return
                f_info = cardinal.telegram.bot.get_file(message.document.file_id)
                file_bytes = cardinal.telegram.bot.download_file(f_info.file_path)
                raw_text = file_bytes.decode('utf-8-sig', errors='ignore')
            except Exception as e:
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
                cardinal.telegram.bot.send_message(chat_id, f'⚠️ Не удалось прочитать файл: {e}')
                return
        else:
            raw_text = message.text or ''
        ok, msg = _import_settings_payload(raw_text)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
        if not ok:
            cardinal.telegram.bot.send_message(chat_id, f'⚠️ {msg}\nПришлите корректный JSON или /cancel.')
            return
        _cleanup_fsm_msgs(cardinal.telegram.bot, chat_id, state)
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, msg)
        try:
            cardinal.telegram.bot.send_message(chat_id, _saves_text(chat_id), parse_mode='HTML', reply_markup=_saves_kb())
        except Exception:
            pass
        return
    if state.get('step') == 'set_jwt':
        if (message.text or '').strip().lower() in ('/cancel', 'cancel', 'отмена'):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, '❌ Отменено.')
            return
        jwt_val = None
        file_bytes = None
        filename = None
        mime = None
        if getattr(message, 'document', None):
            try:
                filename = (message.document.file_name or '').lower()
                mime = (message.document.mime_type or '').lower()
                if message.document.file_size and message.document.file_size > 2000000:
                    _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
                    cardinal.telegram.bot.send_message(chat_id, '⚠️ Файл слишком большой (>2MB). Пришлите меньший или вставьте токен текстом.')
                    return
                f_info = cardinal.telegram.bot.get_file(message.document.file_id)
                file_bytes = cardinal.telegram.bot.download_file(f_info.file_path)
            except Exception as e:
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
                cardinal.telegram.bot.send_message(chat_id, f'⚠️ Не удалось прочитать файл: {e}')
                return
        if file_bytes is not None:
            try:
                text_data = file_bytes.decode('utf-8-sig', errors='ignore')
            except Exception:
                text_data = file_bytes.decode('utf-8', errors='ignore')
            content = (text_data or '').strip()
            is_json = (filename or '').endswith('.json') or (mime or '').endswith('json') or content[:1] in '{['
            if is_json:
                try:
                    obj = json.loads(content)
                    jwt_val = _find_jwt_in_json(obj)
                except Exception:
                    jwt_val = None
            if not jwt_val:
                cleaned = _clean_jwt_text(content)
                parts = _re.findall('[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+', cleaned)
                jwt_val = parts[0] if parts else cleaned
        else:
            part = _clean_jwt_text(message.text or '')
            acc = _clean_jwt_text((state.get('jwt_acc') or '') + part)
            if not _is_jwt_like(acc) and len(acc) < 16:
                state['jwt_acc'] = acc
                _fsm[chat_id] = state
                cardinal.telegram.bot.send_message(chat_id, 'Принял часть токена. Пришлите оставшиеся части (или /cancel).')
                return
            jwt_val = acc
        jwt_val = _clean_jwt_text(jwt_val or '')
        if not jwt_val or len(jwt_val) < 16:
            if getattr(message, 'document', None):
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, 'message_id', None))
            cardinal.telegram.bot.send_message(chat_id, '⚠️ Похоже на некорректный токен. Пришлите валидный JWT текстом или файлом .txt/.json, либо /cancel.')
            return
        _set_cfg(chat_id, fragment_jwt=jwt_val)
        ver, bal, usdt, resp = _check_fragment_wallet(jwt_val)
        _set_cfg(chat_id, wallet_version=ver, balance_ton=round(bal, 6) if isinstance(bal, (int, float)) else None, balance_usdt=round(usdt, 6) if isinstance(usdt, (int, float)) else None, last_wallet_raw=resp)
        _cleanup_fsm_msgs(cardinal.telegram.bot, chat_id, state)
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, '✅ Токен сохранён.')
        try:
            cardinal.telegram.bot.send_message(chat_id, _token_text(chat_id), parse_mode='HTML', reply_markup=_token_kb())
        except Exception:
            pass
        return
def _reset_settings_confirm_text():
    return '⚠️ <b>Сброс сохранений</b>\n\nЭто действие очистит файл <code>settings.json</code>.\nВсе сохранения и настройки плагина будут сброшены к значениям по умолчанию.\n\n<b>Точно хотите сбросить сохранения?</b>'
def _reset_settings_confirm_kb():
    kb = K()
    kb.row(B('✅ Да, сбросить', callback_data=CBT_RESET_SETTINGS_YES), B('❌ Отмена', callback_data=CBT_RESET_SETTINGS_NO))
    return kb
def _open_reset_settings_confirm(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _reset_settings_confirm_text(), _reset_settings_confirm_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass
def _clear_settings_json_file():
    try:
        with _SETTINGS_IO_LOCK:
            try:
                ts = time.strftime('%Y%m%d-%H%M%S')
                if os.path.exists(SETTINGS_FILE):
                    shutil.copy2(SETTINGS_FILE, SETTINGS_FILE + f'.reset.{ts}.bak')
            except Exception:
                pass
            _atomic_write_json(SETTINGS_FILE, {})
        return (True, '✅ Сохранения сброшены (settings.json очищен).')
    except Exception as e:
        logger.error(f'Reset settings error: {e}')
        return (False, f'❌ Не удалось сбросить настройки: {e}')
def _cb_reset_settings_yes(cardinal, call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    try:
        bot.answer_callback_query(call.id, 'Сбрасываю…')
    except Exception:
        pass
    ok, msg = _clear_settings_json_file()
    try:
        _fsm.pop(chat_id, None)
    except Exception:
        pass
    bot.send_message(chat_id, msg, parse_mode='HTML' if '<' in msg else None)
    try:
        _open_saves(bot, call)
    except Exception:
        pass
def _cb_reset_settings_no(bot, call):
    try:
        bot.answer_callback_query(call.id, 'Отменено.')
    except Exception:
        pass
    _open_saves(bot, call)
_pending_orders = {}
FTS_GLOBAL_QUEUE = bool(int(os.getenv('FTS_GLOBAL_QUEUE', '1')))
_GLOBAL_QKEY = '__global_orders__'
_prompted_orders = {}
_prompted_oids = set()
_preorders = {}
_done_oids = set()
def _remove_order_everywhere(oid):
    if not oid:
        return
    s = str(oid)
    for key, q in list(_pending_orders.items()):
        for it in list(q):
            if str(it.get('order_id')) == s:
                it['finalized'] = True
                try:
                    q.remove(it)
                except ValueError:
                    pass
        if not q:
            try:
                del _pending_orders[key]
            except Exception:
                pass
def _mark_done(chat_id, oid):
    if not oid:
        return
    s = str(oid)
    _done_oids.add(s)
    _preorders.pop(s, None)
    _remove_order_everywhere(s)
_blocked_oids = set()
_failed_orders = {}
def _finalize_order(oid, chat_id, *, ok, reason=''):
    oid = str(oid)
    if ok:
        _done_oids.add(oid)
        try:
            rec = (_get_cfg(chat_id).get('order_records') or {}).get(oid, {})
            status = 'sent_pending' if str((rec or {}).get('status') or '').lower() == 'sent_pending' else 'sent'
        except Exception:
            status = 'sent'
        _order_record_update(chat_id, oid, status=status, finalized_ts=int(time.time()))
    else:
        _blocked_oids.add(oid)
        _failed_orders[oid] = {'chat_id': chat_id, 'reason': reason, 'ts': time.time()}
        _order_record_update(chat_id, oid, status='failed', failed_reason=reason, finalized_ts=int(time.time()))
    _remove_order_everywhere(oid)
def _set_order_qty(chat_id, order_id, qty):
    if not order_id or not qty or qty < 50:
        return
    try:
        for it in _q(chat_id):
            if str(it.get('order_id')) == str(order_id):
                it['qty'] = int(qty)
                break
        if str(order_id) in _preorders:
            _preorders[str(order_id)]['qty'] = int(qty)
    except Exception:
        pass
def _adopt_foreign_queue_for(chat_id):
    key = str(chat_id)
    if key in _pending_orders and _pending_orders[key]:
        return False
    for other_key, items in list(_pending_orders.items()):
        if other_key == key:
            continue
        for it in list(items):
            oid = it.get('order_id')
            if oid and str(oid) in _done_oids:
                try:
                    items.remove(it)
                except ValueError:
                    pass
        if not items:
            try:
                del _pending_orders[other_key]
            except Exception:
                pass
            continue
        if any((_allowed_stages(x) for x in items)):
            _pending_orders[key] = items
            del _pending_orders[other_key]
            logger.warning(f'[QUEUE] merged {other_key} -> {key}')
            return True
    return False
def _mark_prompted(chat_id, order_id):
    if order_id is None:
        return
    oid = str(order_id)
    _prompted_oids.add(oid)
    _prompted_orders.setdefault(str(chat_id), set()).add(oid)
def _was_prompted(chat_id, order_id):
    if order_id is None:
        return any((x.get('prompted') for x in _q(chat_id)))
    oid = str(order_id)
    if oid in _prompted_oids:
        return True
    return oid in _prompted_orders.get(str(chat_id), set())
def _unmark_prompted(chat_id, order_id, *, everywhere=False):
    if order_id is None:
        return
    oid = str(order_id)
    s = _prompted_orders.get(str(chat_id))
    if s:
        s.discard(oid)
    if everywhere:
        _prompted_oids.discard(oid)
def _q(chat_id):
    key = _GLOBAL_QKEY if FTS_GLOBAL_QUEUE else str(chat_id)
    return _pending_orders.setdefault(key, [])
def _current(chat_id):
    q = _q(chat_id)
    return q[0] if q else None
def _push(chat_id, item):
    oid = item.get('order_id')
    if oid and (str(oid) in _done_oids or str(oid) in _blocked_oids):
        logger.debug(f'[QUEUE] skip push for done/blocked order #{oid}')
        return
    q = _q(chat_id)
    if oid and any((str(x.get('order_id')) == str(oid) for x in q)):
        _order_log('debug', 'queue_merge_existing', oid=oid, chat_id=item.get('chat_id'), qty=item.get('qty'))
        for x in q:
            if str(x.get('order_id')) == str(oid):
                for k, v in item.items():
                    if v is not None:
                        x[k] = v
                x.setdefault('prompted', False)
                break
        return
    item.setdefault('prompted', False)
    q.append(item)
    _order_log('info', 'queue_push', oid=oid or 'noid', chat_id=item.get('chat_id'), qty=item.get('qty'), stage=item.get('stage'), qsize=len(q))
def _ensure_pending(chat_id, order_id, qty):
    if order_id and (str(order_id) in _done_oids or str(order_id) in _blocked_oids):
        return {'qty': int(qty or 50), 'order_id': order_id, 'chat_id': chat_id, 'stage': 'finalized', 'candidate': None, 'finalized': True, 'confirmed': True, 'prompted': False, 'preconfirmed': False, 'auto_attempted_for': None, 'queue_notified': False, 'turn_ts': None, 'created_ts': time.time(), 'stage_ts': time.time(), 'last_reminder_ts': 0.0}
    q = _q(chat_id)
    if order_id:
        for x in q:
            if str(x.get('order_id')) == str(order_id):
                if qty and int(qty) >= 50:
                    x['qty'] = int(qty)
                x.setdefault('chat_id', chat_id)
                x.setdefault('stage', 'await_username')
                x.setdefault('candidate', None)
                x.setdefault('finalized', False)
                x.setdefault('confirmed', False)
                x.setdefault('prompted', False)
                x.setdefault('preconfirmed', False)
                x.setdefault('auto_attempted_for', None)
                x.setdefault('queue_notified', False)
                x.setdefault('turn_ts', None)
                x.setdefault('created_ts', time.time())
                x.setdefault('stage_ts', time.time())
                x.setdefault('last_reminder_ts', 0.0)
                return x
    item = {'qty': int(qty or 50), 'order_id': order_id, 'chat_id': chat_id, 'stage': 'await_username', 'candidate': None, 'finalized': False, 'confirmed': False, 'prompted': False, 'preconfirmed': False, 'auto_attempted_for': None, 'queue_notified': False, 'turn_ts': None, 'created_ts': time.time(), 'stage_ts': time.time(), 'last_reminder_ts': 0.0}
    _push(chat_id, item)
    if order_id:
        for x in q:
            if str(x.get('order_id')) == str(order_id):
                return x
    return item
def _find_item_by_chat(chat_id):
    cid = str(chat_id)
    for it in _q(chat_id):
        if str(it.get('chat_id')) == cid and (not it.get('finalized')):
            return it
    return None
def _active_item_for_chat(chat_id):
    if FTS_GLOBAL_QUEUE:
        return _find_item_by_chat(chat_id)
    return _current(chat_id)
def _queue_pos_of(item):
    q = _q(item.get('chat_id'))
    for i, it in enumerate(q, 1):
        if it is item:
            return i
    oid = str(item.get('order_id') or '')
    for i, it in enumerate(q, 1):
        if str(it.get('order_id') or '') == oid:
            return i
    return 9999
def _notify_queued_once(cardinal, item):
    if item.get('queue_notified'):
        return
    pos = _queue_pos_of(item)
    if pos <= 1:
        return
    item['queue_notified'] = True
    _order_log('info', 'queued_notify', oid=item.get('order_id') or 'noid', chat_id=item.get('chat_id'), qty=item.get('qty'), qpos=pos)
    _safe_send(cardinal, item['chat_id'], _tpl(item['chat_id'], 'queued', pos=pos, qty=int(item.get('qty') or 50), order_id=item.get('order_id')))
def _notify_next_turn(cardinal, chat_id=None):
    nxt = _current(chat_id if chat_id is not None else '__any__')
    if not nxt:
        return
    nxt['turn_ts'] = time.time()
    _order_log('info', 'next_turn', oid=nxt.get('order_id') or 'noid', chat_id=nxt.get('chat_id'), qty=nxt.get('qty'), username=nxt.get('candidate'), qpos=1)
    cid = nxt.get('chat_id')
    if not cid:
        return
    oid = nxt.get('order_id')
    qty = int(nxt.get('qty') or 50)
    if oid:
        _unmark_prompted(cid, oid, everywhere=True)
    nxt['auto_attempted_for'] = None
    cand = (nxt.get('candidate') or '').lstrip('@').strip()
    if cand and _validate_username(cand):
        nxt.update(stage='await_confirm', candidate=cand, finalized=False, confirmed=False, prompted=True)
        _safe_send(cardinal, cid, f'⭐️ Ваша очередь дошла на {qty}⭐.\nНик принят: @{cand}.\nНачинаю отправку…')
        _schedule_confirm_send(cardinal, cid)
        return
    nxt.update(stage='await_username', candidate=None, finalized=False, confirmed=False, prompted=True)
    nxt['auto_attempted_for'] = None
    msg = _tpl(cid, 'your_turn', qty=qty, order_id=oid)
    if msg.strip():
        _safe_send(cardinal, cid, msg)
    _mark_prompted(cid, oid)
def _pop_current(chat_id, *, keep_prompted=True):
    q = _q(chat_id)
    item = q.pop(0) if q else None
    if item and item.get('order_id') and (not keep_prompted):
        _unmark_prompted(chat_id, item.get('order_id'), everywhere=True)
    return item
def _prompt_current_order_if_needed(cardinal, chat_id):
    cfg = _get_cfg_for_orders(chat_id)
    cur = _current(chat_id)
    if not cur:
        return
    if cur.get('finalized') or not _allowed_stages(cur):
        return
    oid = cur.get('order_id')
    qty = int(cur.get('qty') or 50)
    if cur.get('prompted') or _was_prompted(chat_id, oid):
        return
    if not _should_prompt_once(chat_id, oid, qty):
        return
    use_pre = _cfg_bool(cfg, 'preorder_username', False)
    cand = (cur.get('candidate') or '').lstrip('@').strip()
    if not use_pre and cand:
        cur.update(stage='await_confirm', candidate=cand, finalized=False, confirmed=False)
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_valid', qty=qty, username=cand, order_id=oid))
        if _auto_send_without_plus(chat_id):
            if cur.get('auto_attempted_for') != cand:
                cur['auto_attempted_for'] = cand
                _schedule_confirm_send(cardinal, chat_id)
            return
        cur['prompted'] = True
        if oid:
            _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
        else:
            _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
        _mark_prompted(chat_id, oid)
        return
    cur.update(stage='await_username', candidate=None, prompted=True, finalized=False, confirmed=False)
    cur['turn_ts'] = time.time()
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'purchase_created', qty=qty, order_id=oid))
    _mark_prompted(chat_id, oid)
def _update_current(chat_id, **updates):
    cur = _current(chat_id)
    if cur is not None:
        cur.update(updates)
def _has_queue(chat_id):
    return bool(_q(chat_id))
def _send_html_chunks(bot, chat_id, text, kb=None):
    from telebot.apihelper import ApiTelegramException
    MAX = 3800
    s = text or ''
    chunks = []
    while s:
        part = s[:MAX]
        if len(s) > MAX:
            cut = max(part.rfind('\n'), part.rfind('. '))
            if cut >= 0 and cut > MAX // 4:
                part = part[:cut + 1]
        chunks.append(part)
        s = s[len(part):]
    for i, part in enumerate(chunks):
        rm = kb if i == len(chunks) - 1 else None
        try:
            bot.send_message(chat_id, part, parse_mode='HTML', reply_markup=rm, disable_web_page_preview=True)
        except ApiTelegramException:
            bot.send_message(chat_id, part, reply_markup=rm, disable_web_page_preview=True)
def _safe_send(c, chat_id, text):
    try:
        c.send_message(chat_id, text)
    except Exception as e:
        logger.warning(f'send_message failed: {e}')
def _is_auto_reply(msg):
    try:
        if getattr(msg, 'is_autoreply', False):
            return True
        badge = getattr(msg, 'badge', None) or getattr(msg, 'badge_text', None) or ''
        if isinstance(badge, str) and 'автоответ' in badge.lower():
            return True
        if getattr(msg, 'by_bot', False) or getattr(msg, 'by_vertex', False):
            return True
    except Exception:
        pass
    return False
_QUEUE_TICK_SEC = float(os.getenv('FTS_QUEUE_TICK_SEC', '5'))
def _queue_watchdog(cardinal):
    while True:
        time.sleep(_QUEUE_TICK_SEC)
        try:
            for _ in range(5):
                if not _maybe_rotate_queue_head(cardinal, '__orders__'):
                    break
        except Exception as e:
            logger.debug(f'queue_watchdog failed: {e}')
def new_order_handler(cardinal, event):
    chat_id = _event_chat_id(event)
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id if chat_id is not None else '__orders__'):
            break
    try:
        chat_id = _event_chat_id(event)
        cfg = _get_cfg_for_orders(chat_id if chat_id is not None else '__orders__')
        if not cfg.get('plugin_enabled', True):
            return
        order = getattr(event, 'order', None)
        if order is not None and (not _order_is_stars(order)):
            _order_log('debug', 'ignore_non_stars_order', chat_id=chat_id)
            return
        title = getattr(order, 'title', None) or getattr(order, 'name', None) or ''
        qty = _extract_qty_from_title(title)
        order_id = getattr(order, 'id', None) or getattr(order, 'order_id', None) or getattr(event, 'order_id', None)
        _order_log('info', 'new_order', oid=order_id or 'noid', chat_id=chat_id, qty=qty if qty is not None else 'unknown', title=title or '-')
        if order_id and (str(order_id) in _done_oids or str(order_id) in _blocked_oids):
            _order_log('info', 'ignore_done_or_blocked', oid=order_id, chat_id=chat_id)
            return
        text_blob = ' '.join((str(x) for x in [title, getattr(order, 'description', None), getattr(order, 'buyer_message', None)] if x))
        if _is_gift_like_text(text_blob) or _mentions_account_login(text_blob):
            _log('info', f'[IGNORE] gift/account-login order ignored (#{order_id})')
            return
        if qty is not None and qty < FTS_MIN_STARS:
            _order_log('warn', 'min_qty_rejected', oid=order_id, chat_id=chat_id, qty=qty, min_qty=FTS_MIN_STARS)
            _safe_send(cardinal, chat_id, f'⚠️ Заказ на {qty}⭐ меньше минимального для авто-обработки ({FTS_MIN_STARS}⭐). Напишите продавцу или дождитесь ручной обработки.')
            if cfg.get('auto_refund', False) and order_id:
                _safe_send(cardinal, chat_id, '🔁 Пытаюсь оформить возврат…')
                ok_ref = _auto_refund_order(cardinal, order_id, chat_id, reason=f'qty<{FTS_MIN_STARS}')
                _log('info' if ok_ref else 'error', f"REFUND #{order_id} -> {('OK' if ok_ref else 'FAIL')}")
            if order_id:
                _finalize_order(str(order_id), chat_id, ok=False, reason=f'qty<{FTS_MIN_STARS}')
            return
        item = _ensure_pending(chat_id, order_id, qty if qty is not None else 50)
        _order_record_update(chat_id, order_id, status=item.get('stage'), qty=item.get('qty'), created_ts=int(time.time()))
        _order_log('info', 'queue_pending', oid=order_id or 'noid', chat_id=chat_id, qty=item.get('qty'), stage=item.get('stage'), qpos=_queue_pos_of(item))
        if FTS_GLOBAL_QUEUE and item is not _current(chat_id):
            _notify_queued_once(cardinal, item)
            return
        username = None
        for candidate in [getattr(order, 'title', None), getattr(order, 'description', None), getattr(order, 'buyer_message', None), getattr(event, 'message', None)]:
            u = _extract_username_from_order_text(candidate)
            if u:
                username = u
                break
        if not username:
            username = _extract_username_from_any(order) or _extract_username_from_any(event)
        try:
            my_user = (getattr(cardinal.account, 'username', None) or '').lstrip('@').lower()
            if username and username.lower() == my_user:
                username = None
        except Exception:
            pass
        jwt = cfg.get('fragment_jwt')
        use_pre = bool(cfg.get('preorder_username', False))
        if use_pre and username and order_id:
            _order_log('info', 'preorder_captured', oid=order_id, chat_id=chat_id, qty=qty, username=username)
            item.update(stage='await_paid', candidate=username.lstrip('@'), prompted=False, finalized=False, confirmed=False)
            _preorders[str(order_id)] = {'username': username.lstrip('@'), 'qty': qty}
            return
        if jwt and (qty is not None and qty >= 50):
            if use_pre and username and (not order_id):
                item.update(stage='await_confirm', candidate=username.lstrip('@'), prompted=True, finalized=False, confirmed=False, stage_ts=time.time())
                _safe_send(cardinal, chat_id, f'Ник из заказа: @{username}. Добавил отправку {qty}⭐ в фоновую очередь…')
                _schedule_confirm_send(cardinal, chat_id)
                return
            cand = (username or '').lstrip('@')
            item.update(stage='await_confirm' if cand else 'await_username', candidate=cand or None, prompted=True, finalized=False, confirmed=False)
            if item is _current(chat_id):
                _order_log('info', 'prompt_current', oid=order_id or 'noid', chat_id=chat_id, qty=qty or 50, username=cand or None, stage=item.get('stage'))
                if cand:
                    _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_valid', qty=qty or 50, username=cand))
                    _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                else:
                    _safe_send(cardinal, chat_id, _tpl(chat_id, 'purchase_created', qty=qty or 50))
            _mark_prompted(chat_id, order_id)
            return
        _log('info', f'ORDER #{order_id}: queued, waiting for username/system message.')
    except Exception as e:
        logger.exception(f'new_order_handler error: {e}')
_IMMUTABLE_META = {'CREDITS': _s64('QHRpbmVjaGVsb3ZlYw=='), 'UUID': _s64('ZmEwYzJmM2EtN2E4NS00YzA5LWEzYjItOWYzYTliOGY4YTc1'), 'CREATOR_URL': _s64('aHR0cHM6Ly90Lm1lL3RpbmVjaGVsb3ZlYw=='), 'GROUP_URL': _s64('aHR0cHM6Ly90Lm1lL2Rldl90aGNfY2hhdA=='), 'CHANNEL_URL': _s64('aHR0cHM6Ly90Lm1lL2J5X3RoYw=='), 'GITHUB_URL': _s64('aHR0cHM6Ly9naXRodWIuY29tL3RpbmVjaGVsb3ZlYy9GUEMtUGx1Z2luLVRlbGVncmFtLVN0YXJz'), 'INSTRUCTION_URL': _s64('aHR0cHM6Ly90ZWxldHlwZS5pbi9AdGluZWNoZWxvdmVjL0ZUUy1QbHVnaW4=')}
_IMMUTABLE_OK = True
_IMMUTABLE_REASON = ''
def _meta_guard():
    global _IMMUTABLE_OK, _IMMUTABLE_REASON
    if not _IMMUTABLE_OK:
        return False
    for k, expected in _IMMUTABLE_META.items():
        cur = globals().get(k, None)
        if cur != expected:
            _IMMUTABLE_OK = False
            _IMMUTABLE_REASON = f'{k} изменён'
            try:
                logger.critical(f'[ANTI-TAMPER] immutable field changed: {k} expected={expected!r} got={cur!r}')
            except Exception:
                pass
            return False
    return True
def _tamper_text():
    reason = _IMMUTABLE_REASON or 'обнаружены изменения в данных плагина'
    return f'''⛔️ <b>Плагин не работает.</b>\n\nПричина: <code>{reason}</code>\n\nПохоже, файл плагина был изменён или подменён.\nУстановите оригинальную версию или обратитесь к создателю:\n\n👤 <b>Создатель:</b> <a href="{_IMMUTABLE_META['CREATOR_URL']}">{_IMMUTABLE_META['CREDITS']}</a>\n👥 <b>Группа:</b> <a href="{_IMMUTABLE_META['GROUP_URL']}">dev chat</a>\n📣 <b>Канал:</b> <a href="{_IMMUTABLE_META['CHANNEL_URL']}">channel</a>\n🌐 <b>GitHub:</b> <a href="{_IMMUTABLE_META['GITHUB_URL']}">repo</a>\n📖 <b>Инструкция:</b> <a href="{_IMMUTABLE_META['INSTRUCTION_URL']}">open</a>\n'''
def _tamper_kb():
    kb = K()
    kb.row(B('👤 Создатель', url=_IMMUTABLE_META['CREATOR_URL']), B('🌐 GitHub', url=_IMMUTABLE_META['GITHUB_URL']))
    kb.row(B('👥 Группа', url=_IMMUTABLE_META['GROUP_URL']), B('📣 Канал', url=_IMMUTABLE_META['CHANNEL_URL']))
    kb.add(B('📖 Инструкция', url=_IMMUTABLE_META['INSTRUCTION_URL']))
    return kb
CREDITS = _IMMUTABLE_META['CREDITS']
UUID = _IMMUTABLE_META['UUID']
CREATOR_URL = _IMMUTABLE_META['CREATOR_URL']
GROUP_URL = _IMMUTABLE_META['GROUP_URL']
CHANNEL_URL = _IMMUTABLE_META['CHANNEL_URL']
GITHUB_URL = _IMMUTABLE_META['GITHUB_URL']
INSTRUCTION_URL = _IMMUTABLE_META['INSTRUCTION_URL']
def _do_confirm_send(cardinal, chat_id):
    pend = _active_item_for_chat(chat_id)
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id):
            break
    if not pend:
        _safe_send(cardinal, chat_id, 'Нет активного заказа. Если нужно — дождитесь нового сообщения о заказе.')
        return
    if FTS_GLOBAL_QUEUE:
        mode = _queue_mode(chat_id)
        head = _current(chat_id)
        if mode in (1, 3) and (not head or pend is not head):
            pend['preconfirmed'] = True
            pos = _queue_pos_of(pend)
            _safe_send(cardinal, chat_id, f'✅ Подтверждение принято. Сейчас ещё не ваша очередь (позиция {pos}). Когда очередь дойдёт — отправлю автоматически.')
            return
    oid = pend.get('order_id')
    if oid and str(oid) in _done_oids:
        if not FTS_GLOBAL_QUEUE or pend is _current(chat_id):
            _pop_current(chat_id, keep_prompted=False)
        return
    qty = int(pend.get('qty', 50))
    username = (pend.get('candidate') or '').strip()
    _order_log('info', 'confirm_start', oid=oid or 'noid', chat_id=chat_id, qty=qty, username=username or None, stage=pend.get('stage'))
    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get('fragment_jwt')
    if not jwt:
        _safe_send(cardinal, chat_id, '⚠️ Токен Fragment не привязан. Покупка невозможна.')
        _order_log('warn', 'confirm_abort_no_jwt', oid=oid or 'noid', chat_id=chat_id, qty=qty, username=username or None)
        _log('warn', 'SEND aborted: no JWT')
        return
    if not username or not _validate_username(username):
        _safe_send(cardinal, chat_id, '❌ Некорректный тег. Отправьте в формате @username (5–32, латиница/цифры/подчёркивание).')
        pend.update(stage='await_username', candidate=None)
        _log('warn', f"SEND aborted: invalid username '{username}'")
        return
    if qty < 50:
        _safe_send(cardinal, chat_id, 'Минимум 50⭐. Уточните количество или лот.')
        _log('warn', f'SEND aborted: qty {qty} < 50')
        return
    if not _check_username_exists_throttled(username, jwt, chat_id):
        _safe_send(cardinal, chat_id, f'❌ Ник "{username}" не найден. Пришлите верный тег в формате @username.')
        _log('warn', f'USERNAME not found (confirm): @{username}')
        return
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'sending', qty=qty, username=username.lstrip('@')))
    oid = pend.get('order_id')
    _set_sending(chat_id, True)
    try:
        resp = _send_stars_with_currency_fallback(cardinal, chat_id, cfg, jwt, username=username, quantity=qty, show_sender=False)
    finally:
        _set_sending(chat_id, False)
    if _resp_indicates_delivery(resp):
        order_url = f'https://funpay.com/orders/{oid}/' if oid else ''
        _send_order_result_message(cardinal, chat_id, qty, username.lstrip('@'), order_url, resp)
        _order_log('info', 'send_ok', oid=oid or 'noid', chat_id=chat_id, qty=qty, username=username, status=(resp or {}).get('status'), currency=(resp or {}).get('currency'), fragment_id=(resp or {}).get('fragment_order_id'))
        _mark_order_sent_record(chat_id, oid, qty, username, resp)
        _log('info', f'SEND OK {qty}⭐ -> @{username}')
        if oid:
            _finalize_order(oid, chat_id, ok=True)
        else:
            _pop_current(chat_id, keep_prompted=False)
        if _has_queue(chat_id):
            _notify_next_turn(cardinal, chat_id)
        return
    kind, human = _classify_send_failure((resp or {}).get('text', ''), (resp or {}).get('status', 0), username.lstrip('@'), jwt)
    if kind == 'username':
        pend.update(stage='await_username', finalized=False, candidate=None)
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid'))
        return
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'failed', reason=human))
    _order_log('error', 'send_fail', oid=oid or 'noid', chat_id=chat_id, qty=qty, username=username, status=(resp or {}).get('status'), reason=human)
    _log('error', f"SEND FAIL {qty}⭐ -> @{username}: {human} | status={(resp or {}).get('status')}")
    pend.update(finalized=True)
    if oid:
        _finalize_order(oid, chat_id, ok=False, reason=human)
    if _should_auto_refund(cfg, oid, resp, human):
        _safe_send(cardinal, chat_id, '🔁 Пытаюсь оформить возврат…')
        ok_ref = _auto_refund_order(cardinal, oid, chat_id, reason=human)
        _log('info' if ok_ref else 'error', f"REFUND #{oid} -> {('OK' if ok_ref else 'FAIL')}")
    else:
        _safe_send(cardinal, chat_id, '⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.')
    _maybe_auto_deactivate(cardinal, cfg, chat_id)
    if _has_queue(chat_id):
        _notify_next_turn(cardinal, chat_id)
def _cb_confirm_send(cardinal, call):
    try:
        cardinal.telegram.bot.answer_callback_query(call.id)
    except Exception:
        pass
    _schedule_confirm_send(cardinal, call.message.chat.id)
def _cb_change_username(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Измените ник сообщением.')
    except Exception:
        pass
    pend = _current(chat_id)
    if not pend:
        cardinal.telegram.bot.send_message(chat_id, 'Нет активного заказа.')
        return
    _update_current(chat_id, stage='await_username')
    cardinal.telegram.bot.send_message(chat_id, 'Введите новый тег в формате @username:')
def _cb_cancel_flow(cardinal, call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, 'Отменено.')
    except Exception:
        pass
    removed = _pop_current(call.message.chat.id, keep_prompted=False)
    if _has_queue(chat_id):
        nxt = _current(chat_id)
        qn = int(nxt.get('qty', 50))
        cardinal.telegram.bot.send_message(chat_id, f'Текущий заказ отменён. Следующий: {qn}⭐.\nПришлите тег в формате @username одной строкой.')
    else:
        cardinal.telegram.bot.send_message(chat_id, 'Текущий заказ отменён.')
def _allowed_stages(item):
    stage = str(item.get('stage'))
    if stage == 'await_paid':
        return not item.get('finalized')
    return stage in {'await_username', 'await_confirm'} and (not item.get('confirmed')) and (not item.get('finalized'))
def _pending_by_oid(chat_id, oid):
    if not oid:
        return None
    for x in _q(chat_id):
        if str(x.get('order_id')) == str(oid) and _allowed_stages(x):
            return x
    return None
def _apply_username_for_item(cardinal, chat_id, item, uname):
    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get('fragment_jwt')
    if not _validate_username(uname):
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid', order_id=item.get('order_id')))
        item.update(stage='await_username', candidate=None)
        return
    qty = int(item.get('qty') or 50)
    item.update(candidate=uname, stage='await_confirm', confirmed=False, stage_ts=time.time())
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_valid', qty=qty, username=uname, order_id=item.get('order_id')))
    if item.get('preconfirmed'):
        if not FTS_GLOBAL_QUEUE or _current(chat_id) is item:
            _schedule_confirm_send(cardinal, chat_id)
        return
    if _auto_send_without_plus(chat_id):
        if FTS_GLOBAL_QUEUE and _current(chat_id) is not item and (_queue_mode(chat_id) != 2):
            return
        if FTS_GLOBAL_QUEUE and _current(chat_id) is not item and item.get('order_id'):
            _schedule_confirm_send(cardinal, chat_id, str(item.get('order_id')))
            return
        _schedule_confirm_send(cardinal, chat_id)
        return
    _safe_send(cardinal, chat_id, 'Проверьте данные по заказу #{oid}:\n- Количество: {qty}⭐\n- Ник: @{uname}\n\nЕсли всё верно — ответьте "+".\\nЧтобы изменить — пришлите другой тег формата @username.'.replace('{oid}', str(item.get('order_id') or '—')).replace('{qty}', str(qty)).replace('{uname}', uname))
def _do_confirm_send_for_oid(cardinal, chat_id, oid):
    if str(oid) in _done_oids:
        _safe_send(cardinal, chat_id, f'Заказ #{oid} уже выполнен.')
        _remove_order_everywhere(oid)
        return
    head = _current(chat_id)
    if head and str(head.get('order_id')) == str(oid):
        _schedule_confirm_send(cardinal, chat_id)
        return
    item = _pending_by_oid(chat_id, oid)
    if not item:
        _safe_send(cardinal, chat_id, f'Не нашёл активный заказ #{oid} для подтверждения.')
        return
    if FTS_GLOBAL_QUEUE and _queue_mode(chat_id) in (1, 3):
        head = _current(chat_id)
        if not head or str(head.get('order_id')) != str(oid):
            item['preconfirmed'] = True
            pos = _queue_pos_of(item)
            _safe_send(cardinal, chat_id, f'✅ Подтверждение принято. Сейчас ещё не ваша очередь (позиция {pos}). Когда очередь дойдёт — отправлю автоматически.')
            return
    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get('fragment_jwt')
    qty = int(item.get('qty') or 50)
    username = (item.get('candidate') or '').strip()
    if not jwt:
        _safe_send(cardinal, chat_id, '⚠️ Токен Fragment не привязан. Покупка невозможна.')
        return
    if not username or not _validate_username(username):
        _safe_send(cardinal, chat_id, f'❌ Для #{oid} не указан корректный @username.')
        item.update(stage='await_username')
        return
    if qty < 50:
        _safe_send(cardinal, chat_id, f'Минимум 50⭐. Заказ #{oid}.')
        return
    _order_log('info', 'confirm_start', oid=oid, chat_id=chat_id, qty=qty, username=username, stage=item.get('stage'))
    _safe_send(cardinal, chat_id, _tpl(chat_id, 'sending', qty=qty, username=username))
    if not _check_username_exists_throttled(username, jwt, chat_id):
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid', order_id=oid))
        item.update(stage='await_username', finalized=False, candidate=None)
        return
    _set_sending(chat_id, True)
    resp = None
    try:
        resp = _send_stars_with_currency_fallback(cardinal, chat_id, cfg, jwt, username=username, quantity=qty, show_sender=False)
    finally:
        _set_sending(chat_id, False)
        if _resp_indicates_delivery(resp):
            order_url = f'https://funpay.com/orders/{oid}/'
            _send_order_result_message(cardinal, chat_id, qty, username, order_url, resp)
            _order_log('info', 'send_ok', oid=oid, chat_id=chat_id, qty=qty, username=username, status=(resp or {}).get('status'), currency=(resp or {}).get('currency'), fragment_id=(resp or {}).get('fragment_order_id'))
            _mark_order_sent_record(chat_id, oid, qty, username, resp)
            _finalize_order(oid, chat_id, ok=True)
            return
        kind, human = _classify_send_failure((resp or {}).get('text', ''), (resp or {}).get('status', 0), username.lstrip('@'), jwt)
        if kind == 'username':
            item.update(stage='await_username', finalized=False, candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid', order_id=oid))
            return
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'failed', reason=human))
        _order_log('error', 'send_fail', oid=oid, chat_id=chat_id, qty=qty, username=username, status=(resp or {}).get('status'), reason=human)
        item.update(finalized=True)
        _finalize_order(oid, chat_id, ok=False, reason=human)
        if _should_auto_refund(cfg, oid, resp, human):
            _safe_send(cardinal, chat_id, '🔁 Пытаюсь оформить возврат…')
            ok_ref = _auto_refund_order(cardinal, oid, chat_id, reason=human)
            _log('info' if ok_ref else 'error', f"REFUND #{oid} -> {('OK' if ok_ref else 'FAIL')}")
        else:
            _safe_send(cardinal, chat_id, '⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.')
        _maybe_auto_deactivate(cardinal, cfg, chat_id)
        return
def _list_pending_oids(chat_id):
    return [str(x.get('order_id')) for x in _q(chat_id) if _allowed_stages(x) and x.get('order_id')]
_INVIS_RE = _re.compile('[\\u200B-\\u200F\\u202A-\\u202E\\u2060-\\u206F\\uFEFF\\u00AD\\u034F\\u061C\\u180E\\uFE00-\\uFE0F]', _re.UNICODE)
def _strip_invisible(s):
    if not s:
        return ''
    s = str(s)
    s = s.replace('＋', '+')
    s = s.replace('\xa0', ' ')
    s = _INVIS_RE.sub('', s)
    return s
def new_message_handler(cardinal, event):
    chat_id = _event_chat_id(event)
    allowed_oids = set()
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id if chat_id is not None else '__orders__'):
            break
    try:
        my_user = (getattr(cardinal.account, 'username', None) or '').lower()
        author = (getattr(event.message, 'author', '') or '').lower()
        chat_id = _event_chat_id(event)
        text = _strip_invisible(event.message.text or '').strip()
        try:
            if chat_id is not None:
                for it in _q(chat_id):
                    oid = it.get('order_id')
                    if oid and _allowed_stages(it):
                        allowed_oids.add(str(oid))
        except Exception:
            allowed_oids = set()
        text = _strip_invisible(event.message.text or '').strip()
        if _is_auto_reply(event.message):
            try:
                chat_id = _event_chat_id(event)
            except Exception:
                chat_id = None
            cur = _current(chat_id) or {}
            oid = cur.get('order_id')
            suffix = []
            if chat_id is not None:
                suffix.append(f'CID:{chat_id}')
            if oid:
                suffix.append(f'OID:{oid}')
            extra = ' (' + ' '.join(suffix) + ')' if suffix else ''
            _log('info', f'[IGNORE] auto-reply skipped{extra}')
            return
        if author == 'funpay' and (_is_gift_like_text(text) or _mentions_account_login(text)):
            _log('info', '[IGNORE] gift/account-login system note')
            return
        if _is_sending(chat_id) and author != 'funpay':
            return
        cfg = _get_cfg_for_orders(chat_id)
        try:
            user_mid = getattr(event.message, 'message_id', None) or getattr(event.message, 'id', None)
        except Exception:
            user_mid = None
        if CLEAN_USER_MSGS:
            stage = (_current(chat_id) or {}).get('stage')
            waiting_input = stage in {'await_username', 'await_confirm'}
            if text and author not in {'funpay'} and (not waiting_input):
                _safe_delete(cardinal.telegram.bot, chat_id, user_mid)
        if not cfg.get('plugin_enabled', True):
            return
        while _has_queue(chat_id):
            head = _current(chat_id) or {}
            if head.get('finalized'):
                _pop_current(chat_id)
                continue
            if not _allowed_stages(head):
                _pop_current(chat_id)
                continue
            break
        if author == 'funpay' and _funpay_is_system_paid_message(text):
            qty, oid = _funpay_extract_qty_and_order_id(text)
            hint_uname = _extract_explicit_handle(text)
            _order_log('info', 'paid_message', oid=oid or 'noid', chat_id=chat_id, qty=qty if qty is not None else 'unknown', username=hint_uname)
            if oid and (str(oid) in _done_oids or str(oid) in _blocked_oids):
                return
            _set_order_qty(chat_id, oid, qty)
            if qty is not None and qty < 50:
                return
            known_here = any((str(x.get('order_id')) == str(oid) for x in _q(chat_id)))
            known_any = known_here or (oid and any((str(x.get('order_id')) == str(oid) for q in _pending_orders.values() for x in q))) or (oid and str(oid) in _preorders)
            if not known_any:
                _ensure_pending(chat_id, oid, qty)
                if _should_prompt_once(chat_id, oid, qty or 0):
                    _safe_send(cardinal, chat_id, _tpl(chat_id, 'purchase_created', qty=qty or 50))
                    _mark_prompted(chat_id, oid)
                return
            pending = None
            for x in _q(chat_id):
                if str(x.get('order_id')) == str(oid):
                    pending = x
                    break
            if pending is None:
                pending = _ensure_pending(chat_id, oid, qty)
            else:
                old_chat = pending.get('chat_id')
                if str(old_chat) != str(chat_id):
                    pending['chat_id'] = chat_id
                    logger.warning(f'[QUEUE] bind order #{oid}: chat_id {old_chat} -> {chat_id}')
            use_pre = _cfg_bool(cfg, 'preorder_username', False)
            jwt = cfg.get('fragment_jwt')
            uname = None
            real_qty = int(qty or 50)
            if use_pre and pending and (str(pending.get('stage')) == 'await_paid') and pending.get('candidate') and jwt:
                uname = str(pending['candidate']).lstrip('@')
                real_qty = int(pending.get('qty') or qty or 50)
            elif use_pre and oid and _preorders.get(str(oid)) and jwt:
                pr = _preorders[str(oid)]
                uname = str(pr.get('username', '')).lstrip('@')
                real_qty = int(pr.get('qty') or real_qty)
            if not uname and hint_uname:
                uname = hint_uname.lstrip('@')
            if uname and jwt and (not use_pre):
                item = pending or _ensure_pending(chat_id, oid, real_qty)
                item.update(qty=int(real_qty), candidate=str(uname).lstrip('@'), stage='await_confirm', finalized=False, confirmed=False, prompted=True)
                _mark_prompted(chat_id, oid)
                _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_valid', qty=real_qty, username=uname))
                if oid:
                    _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                else:
                    _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                return
            if uname and jwt and use_pre:
                item = pending or _ensure_pending(chat_id, oid, real_qty)
                item.update(qty=int(real_qty), candidate=str(uname).lstrip('@'), stage='await_confirm', finalized=False, confirmed=False, prompted=True, stage_ts=time.time())
                _update_current(chat_id, prompted=True)
                _mark_prompted(chat_id, oid)
                _order_record_update(chat_id, oid, status='await_confirm', qty=real_qty, username=uname)
                _safe_send(cardinal, chat_id, f'Ник из заказа: @{uname}. Добавил отправку {real_qty}⭐ в фоновую очередь…')
                if oid:
                    _schedule_confirm_send(cardinal, chat_id, str(oid))
                else:
                    _schedule_confirm_send(cardinal, chat_id)
                return
            if not _was_prompted(chat_id, oid):
                _ensure_pending(chat_id, oid, qty)
                if _should_prompt_once(chat_id, oid, qty or 0):
                    _safe_send(cardinal, chat_id, _tpl(chat_id, 'purchase_created', qty=qty or 50))
                    _mark_prompted(chat_id, oid)
                    _update_current(chat_id, prompted=True, stage='await_username')
                return
            return
        if not text:
            return
        if author == my_user and (_current(chat_id) or {}).get('stage') not in {'await_username', 'await_confirm'}:
            return
        if author == 'funpay':
            u = _extract_username_from_text(text)
            if not u or u.lower() == my_user.lstrip('@'):
                return
        pend = _find_item_by_chat(chat_id) if FTS_GLOBAL_QUEUE else _current(chat_id)
        if not pend:
            return
        if not _has_queue(chat_id):
            logger.warning(f'[QUEUE] no head for chat_id={chat_id}; queues={list(_pending_orders.keys())[:5]}')
        if not pend:
            return
        nick_items = [x for x in _q(chat_id) if str(x.get('stage')) in {'await_username', 'await_confirm'} and (not x.get('finalized'))]
        nick_oids = [str(x.get('order_id')) for x in nick_items if x.get('order_id')]
        many_nick_orders = len(nick_oids) > 1
        m_plus = _re.match('^\\s*(?:\\+{1,2}|ok|да)\\s*$', text, _re.I)
        if m_plus and author != 'funpay':
            _order_log('info', 'plus_received', oid=pend.get('order_id') if pend else 'noid', chat_id=chat_id, qty=pend.get('qty') if pend else None, username=pend.get('candidate') if pend else None)
            _schedule_confirm_send(cardinal, chat_id)
            return
        username = _extract_username_from_order_text(text)
        if not username:
            if pend:
                pend.update(stage='await_username', candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid'))
            return
        uname = username.lstrip('@')
        _order_log('info', 'username_received', oid=pend.get('order_id') or 'noid', chat_id=chat_id, qty=pend.get('qty'), username=uname, author=author or 'buyer')
        m_username_oid = _re.search('#([A-Za-z0-9]{6,})', text)
        if m_username_oid:
            target_oid = m_username_oid.group(1)
            item = _pending_by_oid(chat_id, target_oid)
            if not item:
                _safe_send(cardinal, chat_id, f'Не нашёл активный заказ #{target_oid} для ника @{uname}.')
                return
            _apply_username_for_item(cardinal, chat_id, item, uname)
            return
        if not _validate_username(uname):
            if pend:
                pend.update(stage='await_username', candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_invalid'))
            return
        jwt = cfg.get('fragment_jwt')
        qty = int(pend.get('qty', 0)) or 50
        if 'qty' not in pend:
            cfg_tmp = _get_cfg_for_orders(chat_id)
            enabled_qty = [int(o['qty']) for o in cfg_tmp.get('star_lots') or [] if o.get('active')]
            if len(enabled_qty) == 1:
                qty = enabled_qty[0]
        if pend:
            pend.update(qty=int(qty), candidate=uname, stage='await_confirm', finalized=False, confirmed=False, stage_ts=time.time())
            _order_record_update(chat_id, pend.get('order_id'), status='await_confirm', qty=qty, username=uname)
        _safe_send(cardinal, chat_id, _tpl(chat_id, 'username_valid', qty=qty, username=uname))
        if _auto_send_without_plus(chat_id):
            if FTS_GLOBAL_QUEUE and pend is not _current(chat_id):
                return
            _schedule_confirm_send(cardinal, chat_id)
            return
        _safe_send(cardinal, chat_id, f'Проверьте данные:\n- Количество: {qty}⭐\n- Ник: @{uname}\n\nЕсли всё верно — ответьте "+".\nЧтобы изменить — пришлите другой тег в формате @username.')
        if str(pend.get('stage')) == 'await_confirm' and text.lower() in {'+', '++', 'да', 'ок', 'ok'}:
            _schedule_confirm_send(cardinal, chat_id)
            return
    except Exception as e:
        logger.exception(f'new_message_handler error: {e}')
BIND_TO_PRE_INIT = [init_cardinal]
BIND_TO_NEW_MESSAGE = [new_message_handler]
try:
    BIND_TO_NEW_ORDER = [new_order_handler]
except Exception:
    pass
BIND_TO_DELETE = None
