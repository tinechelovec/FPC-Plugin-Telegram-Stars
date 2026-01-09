from __future__ import annotations
from typing import TYPE_CHECKING, Optional, List, Dict, Any
import os
import json
import logging
import requests
import re as _re
import time
import random
import shutil
import threading

from concurrent.futures import ThreadPoolExecutor
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from telebot.types import InlineKeyboardMarkup as K, InlineKeyboardButton as B
from telebot.apihelper import ApiTelegramException
from collections import defaultdict

try:
    from FunPayAPI.updater.events import NewMessageEvent, NewOrderEvent
except Exception:
    from FunPayAPI.updater.events import NewMessageEvent
    NewOrderEvent = None

try:
    import FunPayAPI
except Exception:
    FunPayAPI = None

import tg_bot.CBT as CBT

if TYPE_CHECKING:
    from cardinal import Cardinal

logger = logging.getLogger("FTS-Plugin")

LOG_TAG = "[FTS-Plugin]"

class _AnsiColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG:   "\033[90m",
        logging.INFO:    "\033[36m",
        logging.WARNING: "\033[33m",
        logging.ERROR:   "\033[31m",
        logging.CRITICAL:"\033[41m",
    }
    RESET = "\033[0m"

    def format(self, record):
        base = super().format(record)
        color = self.COLORS.get(record.levelno, "")
        reset = self.RESET if color else ""
        return f"{color}{base}{reset}"

HUMAN_LOGS   = bool(int(os.getenv("FTS_HUMAN_LOGS", "1")))
HUMAN_DEDUP  = bool(int(os.getenv("FTS_HUMAN_DEDUP", "1")))

_USERNAME_CHECK_GAP = float(os.getenv("FTS_USERNAME_CHECK_GAP", "0.8"))
_USERNAME_CHECK_JITTER = float(os.getenv("FTS_USERNAME_CHECK_JITTER", "0.4"))
_last_username_check_ts: Dict[str, float] = {}
LITESERVER_RETRY_DEFAULT = bool(int(os.getenv("FTS_Plugin_RETRY_LITESERVER", "1")))
LITESERVER_RETRY_SLEEP_MIN = float(os.getenv("FTS_Plugin_RETRY_LITESERVER_SLEEP_MIN", "0.8"))
LITESERVER_RETRY_SLEEP_MAX = float(os.getenv("FTS_Plugin_RETRY_LITESERVER_SLEEP_MAX", "1.8"))
QUEUE_TIMEOUT_DEFAULT = int(os.getenv("FTS_QUEUE_TIMEOUT_SEC", "300"))

class _Ansi:
    R = "\033[31m"; Y = "\033[33m"; C = "\033[36m"; G = "\033[32m"
    DIM = "\033[90m"; BOLD = "\033[1m"; RESET = "\033[0m"

class _HumanLog:
    import re as _re
    SEEN_AUTOREPLY_BY_OID: set[str] = set()

    RULES = [
        (_re.compile(r"\[IGNORE\]\s*auto-reply skipped.*?(?:OID:([A-Z0-9\-]+))?", _re.I),
         lambda m: ("Автоответ найден — пропустили сообщение.", m.group(1) or "")),
        (_re.compile(r"\[IGNORE\]\s*gift/account-login system note", _re.I),
         lambda m: ("Системное примечание с «подарком»/«заходом на аккаунт» — игнорируем.", "")),
        (_re.compile(r"\[QUEUE\]\s*merged\s+(.+?)\s*->\s*([^\s|]+)", _re.I),
         lambda m: (f"Объединили очереди: {m.group(1)} → {m.group(2)}", "")),
        (_re.compile(r"ORDER\s+#([A-Z0-9\-]+):\s*queued,.*", _re.I),
         lambda m: (f"Заказ #{m.group(1)} добавлен в очередь — ждём @username или системное «заказ оплачен».", m.group(1))),
        (_re.compile(r"SEND start:\s*(\d+)\s*⭐\s*→\s*@?([A-Za-z0-9_]{5,32})", _re.I),
         lambda m: (f"Начали отправку: {m.group(1)}⭐ на @{m.group(2)}", "")),
        (_re.compile(r"SEND result:\s*ok=(True|False).*?status=(\d+)", _re.I),
         lambda m: (f"Отправка завершена — {'успех' if m.group(1)=='True' else 'ошибка'}, HTTP {m.group(2)}.", "")),
        (_re.compile(r"SEND exception:\s*(.+)", _re.I),
         lambda m: (f"Ошибка при отправке: {m.group(1)}", "")),
        (_re.compile(r"ORDER FAIL\s+#([A-Z0-9\-]+)\s+(\d+)\s*⭐\s*@([A-Za-z0-9_]{5,32}):\s*(.+?)\s*\|\s*status=(\d+)", _re.I),
         lambda m: (f"Не удалось выполнить заказ #{m.group(1)}: {m.group(4)} (HTTP {m.group(5)}). "
                    f"Кол-во: {m.group(2)}⭐, ник @{m.group(3)}.", m.group(1))),
        (_re.compile(r"\[AUTODEACT\].*?Баланс\s+([0-9.]+)\s*<\s*([0-9.]+).*?категории\s+(\d+)", _re.I),
         lambda m: (f"Лоты категории {m.group(3)} отключены: баланс {m.group(1)} TON ниже порога {m.group(2)} TON.", "")),
        (_re.compile(r"MIN BALANCE set to\s*([0-9.]+)\s*TON", _re.I),
         lambda m: (f"Порог баланса обновлён: {m.group(1)} TON.", "")),
        (_re.compile(r"\[PREORDER\]\s*Захватили ник\s*@([A-Za-z0-9_]{5,32}).*?#([A-Z0-9\-]+)", _re.I),
         lambda m: (f"Ник из заказа захвачен: @{m.group(1)} для #{m.group(2)} — ждём оплату.", m.group(2))),
    ]

    @classmethod
    def _fmt_like_classic(cls, record, text: str, color_code: str) -> str:
        ts = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(record.created))
        lvl_letter = {
            logging.INFO: "I",
            logging.WARNING: "W",
            logging.ERROR: "E",
            logging.DEBUG: "D",
            logging.CRITICAL: "C"
        }.get(record.levelno, "I")
        return (
            f"{color_code}[{ts}]> "
            f"{_Ansi.BOLD}{lvl_letter}{_Ansi.RESET}{color_code}: {LOG_TAG} {text}{_Ansi.RESET}"
        )

    @classmethod
    def humanize(cls, record: logging.LogRecord) -> tuple[str, bool]:
        raw = record.getMessage() or ""
        oid_for_dedup = ""
        text = raw

        for pat, fn in cls.RULES:
            m = pat.search(raw)
            if m:
                try:
                    text, oid_for_dedup = fn(m)
                except Exception:
                    pass
                break

        text = text.replace("[FTS-Plugin]", "").strip()

        color = _Ansi.C
        if record.levelno >= logging.ERROR:   color = _Ansi.R
        elif record.levelno == logging.WARNING: color = _Ansi.Y
        elif record.levelno == logging.DEBUG: color = _Ansi.DIM

        if HUMAN_DEDUP and text.startswith("Автоответ найден") and oid_for_dedup:
            key = f"auto:{oid_for_dedup}"
            if key in cls.SEEN_AUTOREPLY_BY_OID:
                return ("", True)
            cls.SEEN_AUTOREPLY_BY_OID.add(key)

        return (cls._fmt_like_classic(record, text, color), False)

class _HumanFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg, suppressed = _HumanLog.humanize(record)
        if suppressed:
            return False
        record._humanized = msg
        return True

class _HumanFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        msg = getattr(record, "_humanized", None)
        if msg is None:
            msg, _ = _HumanLog.humanize(record)
        return msg

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
        h.setFormatter(_AnsiColorFormatter("%(asctime)s %(levelname)s " + LOG_TAG + " %(message)s"))
        logger.addHandler(h)

    raw_path = os.getenv("FTS_RAW_LOG_FILE")
    if raw_path:
        f = logging.FileHandler(raw_path, encoding="utf-8")
        f.setFormatter(_AnsiColorFormatter("%(asctime)s %(levelname)s " + LOG_TAG + " %(message)s"))
        logger.addHandler(f)

_setup_logging()

if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(_AnsiColorFormatter("%(asctime)s %(levelname)s " + LOG_TAG + " %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

_STATE_LOCK = threading.RLock()
_SEND_EXECUTOR = ThreadPoolExecutor(
    max_workers=int(os.getenv("FTS_SEND_WORKERS", "1")),
    thread_name_prefix="FTS-SEND"
)
_ACTIVE_JOBS: dict[str, Any] = {}
_ACTIVE_JOBS_LOCK = threading.Lock()

def _schedule_job(key: str, fn, *args, **kwargs) -> bool:
    with _ACTIVE_JOBS_LOCK:
        old = _ACTIVE_JOBS.get(key)
        if old is not None and not old.done():
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

def _schedule_confirm_send(cardinal: "Cardinal", chat_id: Any, oid: Optional[str] = None) -> None:
    job_key = f"send:{chat_id}:{oid or 'cur'}"

    def _run():
        try:
            if oid:
                _do_confirm_send_for_oid(cardinal, chat_id, oid)
            else:
                _do_confirm_send(cardinal, chat_id)
        except Exception as e:
            logger.exception(f"background send failed: {e}")

    _schedule_job(job_key, _run)

def _log(level: str, msg: str):
    if level == "info":
        logger.info(f"{msg}")
    elif level == "warn":
        logger.warning(f"{msg}")
    elif level == "error":
        logger.error(f"{msg}")
    else:
        logger.debug(f"{msg}")

NAME        = "FTS-Plugin"
VERSION     = "1.6.1"
DESCRIPTION = "Плагин по продаже звезд."
CREDITS     = "@tinechelovec"
UUID        = "fa0c2f3a-7a85-4c09-a3b2-9f3a9b8f8a75"
SETTINGS_PAGE = False

CREATOR_URL = "https://t.me/tinechelovec"
GROUP_URL = "https://t.me/dev_thc_chat"
CHANNEL_URL = "https://t.me/by_thc"
GITHUB_URL = "https://github.com/tinechelovec/FPC-Plugin-Telegram-Stars"
INSTRUCTION_URL = "https://teletype.in/@tinechelovec/FTS-Plugin"

FRAGMENT_BASE          = os.getenv("FRAGMENT_BASE", "https://api.fragment-api.com/v1")
FRAGMENT_AUTH_URL      = os.getenv("FRAGMENT_AUTH_URL", f"{FRAGMENT_BASE}/auth/authenticate/")
FRAGMENT_WALLET_URL    = os.getenv("FRAGMENT_WALLET_URL", f"{FRAGMENT_BASE}/misc/wallet/")
FRAGMENT_USER_URLS     = [
    os.getenv("FNP_FRAGMENT_USER_URL", f"{FRAGMENT_BASE}/misc/user/user/"),
    f"{FRAGMENT_BASE}/misc/user/"
]
FRAGMENT_ORDER_STARS   = os.getenv("FRAGMENT_ORDER_STARS", f"{FRAGMENT_BASE}/order/stars/")

FNP_STARS_CATEGORY_ID = int(os.getenv("FTS_Plugin_CATEGORY_ID", "2418"))
FNP_MIN_BALANCE_TON   = float(os.getenv("FTS_Plugin_MIN_BALANCE_TON", "5.0"))

PLUGIN_FOLDER  = "storage/plugins/FTS-Plugin"
SETTINGS_FILE  = os.path.join(PLUGIN_FOLDER, "settings.json")
os.makedirs(PLUGIN_FOLDER, exist_ok=True)
if not os.path.exists(SETTINGS_FILE):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f, indent=4, ensure_ascii=False)

LOG_FILE_LOCAL = os.path.join(PLUGIN_FOLDER, "lot.txt")
try:
    _fh_local = logging.FileHandler(LOG_FILE_LOCAL, encoding="utf-8")
    _fh_local.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [FTS-Plugin] %(message)s"))
    logger.addHandler(_fh_local)
except Exception as e:
    logger.debug(f"Local file logging init failed: {e}")

def _load_settings() -> dict:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Load settings error: {e}")
        return {}

def _cfg_bool(cfg: dict, key: str, default: bool = False) -> bool:
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off", ""):
            return False
    return default

def _auto_send_without_plus(chat_id: Any) -> bool:
    try:
        cfg = _get_cfg_for_orders(chat_id)
        return _cfg_bool(cfg, "auto_send_without_plus", False)
    except Exception:
        return True

def _save_settings(data: dict) -> None:
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Save settings error: {e}")

_anti_dup_prompts: Dict[str, float] = {}

def _should_prompt_once(chat_id: Any, order_id: Optional[str], qty: int, window_sec: int = 20) -> bool:
    key = f"{chat_id}:{order_id or 'noid'}:{int(qty)}"
    now = time.time()
    last = _anti_dup_prompts.get(key, 0.0)
    if now - last < window_sec:
        return False
    _anti_dup_prompts[key] = now
    return True

def _default_templates() -> dict:
    return {
        "purchase_created": "Спасибо за покупку {qty}⭐!\nНапишите ваш Telegram-тег одной строкой в формате @username.\nПример: @username",
        "username_received": "Принял тег: @{username}. Проверяю…",
        "username_invalid": "❌ Некорректный или несуществующий тег.\nОтправьте верный Telegram-тег в формате @username (5–32, латиница/цифры/подчёркивание), а затем подтвердите ответом «+».\nПример: @username",
        "username_valid": "✅ Тег принят: @{username}.",
        "sending": "Отправляю {qty}⭐ на @{username}…",
        "sent": "✅ Готово: отправлено {qty}⭐ на @{username}. {order_url}",
        "failed": "❌ Не удалось отправить звёзды: {reason}",
        "queued": "🕒 Заказ принят. Сейчас вы в очереди: позиция {pos}.\nЯ напишу, когда дойдёт ваша очередь.",
        "your_turn": "⭐️ До вас дошла очередь на {qty}⭐.\nПришлите ваш Telegram-тег одной строкой: @username",
    }

def _fmt_tpl(tpl: str, **kw) -> str:
    try:
        return tpl.format(**kw)
    except Exception:
        return tpl

def _tpl(chat_id: Any, key: str, **kw) -> str:
    cfg_owner = _get_cfg_for_orders(chat_id)
    tpls = (cfg_owner.get("templates") if isinstance(cfg_owner, dict) else None) or {}

    if not tpls:
        cfg_local = _get_cfg(chat_id)
        tpls = (cfg_local.get("templates") if isinstance(cfg_local, dict) else None) or {}

    default = _default_templates().get(key, "")
    raw = tpls.get(key, default)
    return _fmt_tpl(raw, **kw)

def _get_cfg(chat_id: Any) -> dict:
    data = _load_settings()
    key = str(chat_id)
    cfg = data.get(key) or {}
    cfg.setdefault("plugin_enabled", True)
    cfg.setdefault("lots_active", False)
    cfg.setdefault("auto_refund", False)
    cfg.setdefault("auto_deactivate", True)
    cfg.setdefault("manual_refund_enabled", False)
    cfg.setdefault("manual_refund_priority", True)
    cfg.setdefault("preorder_username", False)
    cfg["preorder_username"] = _cfg_bool(cfg, "preorder_username", False)
    cfg.setdefault("markup_percent", 0.0)
    cfg.setdefault("fragment_jwt", None)
    cfg.setdefault("wallet_version", None)
    cfg.setdefault("balance_ton", None)
    cfg.setdefault("last_wallet_raw", None)
    cfg.setdefault("templates", _default_templates())
    cfg.setdefault("category_id", FNP_STARS_CATEGORY_ID)
    cfg["category_id"] = FNP_STARS_CATEGORY_ID
    cfg.setdefault("min_balance_ton", FNP_MIN_BALANCE_TON)
    cfg.setdefault("star_lots", [])
    cfg.setdefault("retry_liteserver", LITESERVER_RETRY_DEFAULT)
    cfg.setdefault("auto_send_without_plus", False)
    cfg["auto_send_without_plus"] = _cfg_bool(cfg, "auto_send_without_plus", False)
    cfg.setdefault("skip_username_check", False)
    cfg["skip_username_check"] = _cfg_bool(cfg, "skip_username_check", False)
    cfg.setdefault("queue_mode", 1)
    cfg.setdefault("queue_timeout_sec", QUEUE_TIMEOUT_DEFAULT)
    try:
        cfg["queue_mode"] = int(cfg.get("queue_mode") or 1)
    except Exception:
        cfg["queue_mode"] = 1
    try:
        cfg["queue_timeout_sec"] = int(float(cfg.get("queue_timeout_sec") or QUEUE_TIMEOUT_DEFAULT))
    except Exception:
        cfg["queue_timeout_sec"] = QUEUE_TIMEOUT_DEFAULT
    data[key] = cfg
    _save_settings(data)
    return cfg

def _get_cfg_for_orders(chat_id: Any) -> dict:
    cfg = _get_cfg(chat_id)
    if cfg.get("fragment_jwt"):
        return cfg
    data = _load_settings()
    for k, v in data.items():
        if isinstance(v, dict) and v.get("fragment_jwt"):
            return v
    return cfg

def _skip_username_check(chat_id: Any) -> bool:
    try:
        cfg = _get_cfg(chat_id)
        return _cfg_bool(cfg, "skip_username_check", False)
    except Exception:
        return False

def _set_cfg(chat_id: Any, **updates) -> dict:
    data = _load_settings()
    key = str(chat_id)
    cfg = data.get(key) or {}
    cfg.update(updates)
    data[key] = cfg
    _save_settings(data)
    return cfg

def _state_on(v: bool) -> str:
    return "🟢 Включено" if v else "🔴 Выключено"

def _safe_edit(bot, chat_id: Any, msg_id: int, text: str, kb=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id,
                              parse_mode="HTML", reply_markup=kb,
                              disable_web_page_preview=True)
    except ApiTelegramException as e:
        low = str(e).lower()
        if ("message is not modified" in low or
            "message to edit not found" in low or
            "chat not found" in low or
            "bot was blocked" in low):
            logger.debug(f"edit_message skipped: {e}")
            return
        logger.debug(f"edit_message failed: {e}")
    except Exception as e:
        logger.debug(f"edit_message failed: {e}")

def _safe_delete(bot, chat_id: Any, msg_id: Optional[int]):
    try:
        if msg_id:
            bot.delete_message(chat_id, msg_id)
    except ApiTelegramException as e:
        low = str(e).lower()
        if ("message to delete not found" in low or
            "chat not found" in low or
            "bot was blocked" in low):
            logger.debug(f"delete_message skipped: {e}")
            return
        logger.debug(f"delete_message failed: {e}")
    except Exception as e:
        logger.debug(f"delete_message failed: {e}")

def _about_text() -> str:
    if not _meta_guard():
        return _tamper_text()

    return (
        "🧩 <b>Плагин:</b> FTS Plugin\n"
        f"📦 <b>Версия:</b> <code>{VERSION}</code>\n"
        f"👤 <b>Автор:</b> <a href=\"{CREATOR_URL}\">{CREDITS}</a>\n\n"
        "Выберите раздел ниже."
    )

def _settings_text(chat_id: Any) -> str:
    cfg = _get_cfg(chat_id)
    prio = "выше автовозврата" if cfg.get("manual_refund_priority", True) else "ниже автовозврата"
    token_state = "привязан ✅" if cfg.get("fragment_jwt") else "не создан ❌"
    wallet_ver  = cfg.get("wallet_version") or "—"
    balance_ton = cfg.get("balance_ton")
    balance_txt = f"{balance_ton} TON" if balance_ton is not None else "—"
    lot_count   = len(cfg.get("star_lots") or [])
    reason = cfg.get("last_auto_deact_reason")
    state_txt, _ = _lots_state_summary(cfg)
    lots_line = f"• Лоты: <b>{state_txt}</b>"
    if (state_txt != "🟢 Включены") and reason:
        lots_line += f" <i>(авто-выкл: {reason})</i>"
    return (
        f"<b>Текущие настройки</b>\n\n"
        f"• Плагин: <b>{_state_on(cfg.get('plugin_enabled', True))}</b>\n"
        f"{lots_line}\n"
        f"• Автовозврат: <b>{_state_on(cfg.get('auto_refund', False))}</b>\n"
        f"• Автодеактивация: <b>{_state_on(cfg.get('auto_deactivate', True))}</b>\n"
        f"• Ручной возврат (!бэк): <b>{_state_on(cfg.get('manual_refund_enabled', False))}</b> (<i>{prio}</i>)\n"
        f"• Ник из заказа: <b>{_state_on(cfg.get('preorder_username', False))}</b> (<i>без проверки существования</i>)\n"
        f"• Наценка на звёзды: <code>{cfg.get('markup_percent', 0.0)}%</code>\n"
        f"• Порог баланса (TON): <code>{cfg.get('min_balance_ton', FNP_MIN_BALANCE_TON)}</code>\n"
        f"• Токен (JWT): <b>{token_state}</b>\n"
        f"• Баланс: <code>{balance_txt}</code>\n"
        f"• Категория (FunPay): <code>{FNP_STARS_CATEGORY_ID}</code>\n"
        f"• ⭐ Звёздных лотов: <b>{lot_count}</b>\n"
        "\nВыберите действие:"
    )

def _token_text(chat_id: Any) -> str:
    cfg = _get_cfg(chat_id)
    token_state = "Токен привязан ✅" if cfg.get("fragment_jwt") else "Пока не создан токен ❌"
    balance_ton = cfg.get("balance_ton")
    balance_txt = f"{balance_ton} TON" if balance_ton is not None else "—"
    return (
        f"<b>Токен (JWT)</b>\n\n"
        f"• Состояние: <b>{token_state}</b>\n"
        f"• Баланс: <code>{balance_txt}</code>\n\n"
        "Создайте токен, следуя шагам ниже."
    )

def _toggle_plugin(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("plugin_enabled", True))
    _set_cfg(chat_id, plugin_enabled=new_state)
    try:
        bot.answer_callback_query(call.id, "Плагин включён." if new_state else "Плагин выключен.")
    except Exception:
        pass
    _open_settings(bot, call)

def _toggle_preorder_username(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("preorder_username", False))
    _set_cfg(chat_id, preorder_username=new_state)
    try:
        bot.answer_callback_query(call.id, "Ник из заказа включён." if new_state else "Ник из заказа выключен.")
    except Exception:
        pass
    _open_settings(bot, call)

def _stars_text(chat_id: Any) -> str:
    cfg = _get_cfg(chat_id)
    items = cfg.get("star_lots") or []
    header = f"<b>⚙️ Настройка лотов</b>\n\nТекущая наценка: <b>{cfg.get('markup_percent', 0.0)}%</b>\n\n"
    if not items:
        body = "Пока нет лотов со звёздами.\nНажмите «➕ Добавить лот»."
    else:
        rows = []
        for it in sorted(items, key=lambda x: (int(x.get('qty', 0)), int(x.get('lot_id', 0)))):
            rows.append(
                f"• <b>{it.get('qty')}</b> ⭐ → LOT <code>{it.get('lot_id')}</code> — " +
                ("🟢 активен" if it.get('active') else "🔴 выключен")
            )
        body = "\n".join(rows)
    return header + body

def _normalize_wallet_version(s: str) -> str:
    if not s:
        return "W5"
    t = s.strip().upper().replace(" ", "").replace(".", "")
    if t in ("W5", "V5R1"):
        return "W5"
    if t in ("V4R2",):
        return "V4R2"
    if "V4" in t and "R2" in t:
        return "V4R2"
    if "5" in t and ("R1" in t or "V5" in t or "W5" in t):
        return "W5"
    return "W5"

def _extract_qty_from_title(title: str) -> Optional[int]:
    if not title:
        return None

    m = _re.search(r"(\d{2,7})\s*(?:зв[её]зд\w*|stars?|⭐️|⭐)", title, _re.I)
    if m:
        try:
            v = int(m.group(1))
            return v if v >= 50 else None
        except Exception:
            pass

    nums = []
    for x in _re.findall(r"\d{2,7}", title):
        try:
            n = int(x)
            if n >= 50:
                nums.append(n)
        except Exception:
            pass
    return max(nums) if nums else None

_sending_chats: set[str] = set()

def _is_sending(chat_id: Any) -> bool:
    with _STATE_LOCK:
        return str(chat_id) in _sending_chats

def _set_sending(chat_id: Any, v: bool):
    k = str(chat_id)
    with _STATE_LOCK:
        if v:
            _sending_chats.add(k)
        else:
            _sending_chats.discard(k)

def _extract_username_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    s = str(text)

    m = _re.search(r'(?i)(?:по|by)\s*username\s*[,:\-]?\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)

    m = _re.search(r'(?i)\b(?:ник|username)\s*[:=]\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)

    s2 = _re.sub(r'(?i)покупатель\s+[A-Za-z0-9_]{5,32}\s+оплатил(?:\s+заказ)?[^.\n]*\.?', ' ', s)

    m = _re.search(r'@([A-Za-z0-9_]{5,32})', s2)
    if m:
        return m.group(1)

    return None

def _extract_username_from_order_text(text: str) -> Optional[str]:
    if not text:
        return None
    s = str(text)

    u = _extract_username_from_text(s)
    if u:
        return u

    m = _re.search(r'(?i)(?:https?://)?t\.me/(?:@)?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)

    m = _re.search(r'(?i)\b(?:tg|тг|telegram|телеграм|телега)\b\s*[,:\-=]?\s*@?([A-Za-z0-9_]{5,32})', s)
    if m:
        return m.group(1)

    m = _re.search(r'(?i)\b(?:для|to)\b\s*@?([A-Za-z0-9_]{5,32})\b', s)
    if m:
        cand = m.group(1)
        if _validate_username(cand) and _re.search(r"[A-Za-z]", cand):
            return cand

    m = _re.fullmatch(r'\s*@?([A-Za-z0-9_]{5,32})\s*[.!?,;:]*\s*', s)
    if m:
        cand = m.group(1)
        if _re.search(r"[A-Za-z]", cand):
            return cand

    return None

def _extract_explicit_handle(text: str) -> Optional[str]:
    if not text:
        return None
    m = _re.search(r'@([A-Za-z0-9_]{5,32})', text)
    return m.group(1) if m else None

def _extract_username_from_any(x, depth: int = 0) -> Optional[str]:
    if depth > 2 or x is None:
        return None

    if isinstance(x, str):
        s = str(x)

        m = _re.search(r'@([A-Za-z0-9_]{4,32})', s)
        if m:
            return m.group(1)

        m = _re.search(
            r'(?i)(?:по|by)\s*username\s*[,:\-]?\s*@?([A-Za-z0-9_]{4,32})',
            s
        )
        if m:
            return m.group(1)

        m = _re.search(
            r'(?i)\b(?:ник|username)\s*[:=]\s*@?([A-Za-z0-9_]{4,32})',
            s
        )
        if m:
            return m.group(1)

        return None

    if isinstance(x, dict):
        for k, v in x.items():
            if not isinstance(v, str):
                continue
            key_l = str(k).lower()
            if any(t in key_l for t in ("telegram", "tg", "ник", "handle", "stars", "звезд", "звезда")):
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
            if name.startswith("_"):
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

def _check_username_exists(username: str, jwt: Optional[str]) -> bool:
    if not username:
        return False
    uname = username.lstrip("@").strip()

    urls = []
    for base in FRAGMENT_USER_URLS:
        base = (base or "").rstrip("/")
        if base:
            urls.append(f"{base}/{uname}/")

    urls.append(f"{FRAGMENT_BASE}/misc/user/{uname}/")

    headers_with_jwt = {"Accept": "application/json"}
    if jwt:
        headers_with_jwt["Authorization"] = f"JWT {jwt}"

    for url in urls:
        try:
            r = requests.get(url, headers=headers_with_jwt, timeout=8)
            if r.status_code == 200:
                try:
                    data = r.json()
                    if isinstance(data, dict) and (data.get("username") or data.get("user") or data.get("id")):
                        return True
                except Exception:
                    pass

            r2 = requests.get(url, headers={"Accept": "application/json"}, timeout=8)
            if r2.status_code == 200:
                try:
                    data = r2.json()
                    if isinstance(data, dict) and (data.get("username") or data.get("user") or data.get("id")):
                        return True
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"_check_username_exists {url} failed: {e}")
    return False

def _check_username_exists_throttled(username: str, jwt: Optional[str], chat_id: Any = None) -> bool:
    key = str(chat_id) if chat_id is not None else "__global__"
    now = time.time()
    last = _last_username_check_ts.get(key, 0.0)
    wait = (last + _USERNAME_CHECK_GAP) - now
    if wait > 0:
        time.sleep(min(wait + random.random() * _USERNAME_CHECK_JITTER, _USERNAME_CHECK_GAP + _USERNAME_CHECK_JITTER))
    _last_username_check_ts[key] = time.time()
    return _check_username_exists(username, jwt)

def _extract_wallet_info(data: dict) -> tuple[Optional[str], Optional[float]]:
    if not isinstance(data, dict):
        return None, None
    ver = None
    bal = None
    for key in ("wallet_version", "walletVersion", "version"):
        if key in data and isinstance(data[key], (str, int, float)):
            ver = str(data[key]); break
    for key in ("balance_ton", "balanceTon", "balance", "ton_balance"):
        if key in data:
            try:
                bal = float(data[key]); break
            except Exception:
                pass
    if bal is None:
        for outer in ("wallet", "ton"):
            node = data.get(outer)
            if isinstance(node, dict) and "balance" in node:
                try:
                    bal = float(node["balance"]); break
                except Exception:
                    pass
    if bal is None:
        for key in ("nanoton", "nanoTon", "nanotons", "balance_nano", "balanceNano"):
            if key in data:
                try:
                    v = float(data[key])
                    bal = v / 1e9 if v > 1e6 else v
                    break
                except Exception:
                    pass
    return ver, bal

def _check_fragment_wallet(jwt: str) -> tuple[Optional[str], Optional[float], Optional[dict]]:
    try:
        r = requests.get(
            FRAGMENT_WALLET_URL,
            headers={"Accept": "application/json", "Authorization": f"JWT {jwt}"},
            timeout=20,
        ); r.raise_for_status()
        data = r.json()
        ver, bal = _extract_wallet_info(data if isinstance(data, dict) else {})
        return ver, bal, data if isinstance(data, dict) else {"raw": data}
    except Exception as e:
        logger.warning(f"Fragment wallet check failed: {e}")
        return None, None, None
    
_LS_RE = _re.compile(r"(?:\blite\s*server\b|liteserver)", _re.I)

def _is_liteserver_transient_failure(resp_text: str, status: int = 0, resp_json: Any = None) -> bool:
    txt = resp_text or ""
    low = txt.lower()

    if not _LS_RE.search(txt):
        if isinstance(resp_json, dict):
            dump = json.dumps(resp_json, ensure_ascii=False)
            if not _LS_RE.search(dump):
                return False
            low = dump.lower()
        else:
            return False

    if "seqno" in low:
        return False
    if any(w in low for w in (
        "not enough", "insufficient", "balance",
        "username", "user not found", "invalid",
        "too many requests", "429",
        "version"
    )):
        return False

    if status and status not in (0, 408, 500, 502, 503, 504):
        return False

    return True

def _order_stars_with_retry(
    jwt: str,
    username: str,
    quantity: int,
    show_sender: bool = False,
    webhook_url: Optional[str] = None,
    retry_enabled: bool = False,
) -> dict:
    resp = _order_stars(
        jwt,
        username=username,
        quantity=quantity,
        show_sender=show_sender,
        webhook_url=webhook_url
    )

    if resp.get("ok") or not retry_enabled:
        return resp

    if _is_liteserver_transient_failure(
        resp.get("text", ""),
        int(resp.get("status") or 0),
        resp.get("json")
    ):
        delay = random.uniform(LITESERVER_RETRY_SLEEP_MIN, LITESERVER_RETRY_SLEEP_MAX)
        _log("warn", f"SEND retry: liteserver transient error, attempt=2, sleep={delay:.2f}s")
        time.sleep(delay)

        resp2 = _order_stars(
            jwt,
            username=username,
            quantity=quantity,
            show_sender=show_sender,
            webhook_url=webhook_url
        )
        resp2["_retried"] = True
        return resp2

    return resp

def _order_stars(jwt: str, username: str, quantity: int, show_sender: bool = False, webhook_url: Optional[str] = None) -> dict:
    try:
        u = username.lstrip("@").strip()
        payload = {"username": u, "quantity": quantity, "show_sender": bool(show_sender)}
        if webhook_url:
            payload["webhook_url"] = webhook_url

        _log("info", f"SEND start: {quantity}⭐ → @{u}")
        r = requests.post(
            FRAGMENT_ORDER_STARS,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"JWT {jwt}"
            },
            timeout=120,
        )

        resp_json = None
        ct = (r.headers.get("Content-Type") or "").lower()
        if "application/json" in ct:
            try:
                resp_json = r.json()
            except Exception:
                resp_json = None

        ok_flags = set()
        if isinstance(resp_json, dict):
            for k in ("ok", "success", "sent", "purchased", "done"):
                v = resp_json.get(k)
                if isinstance(v, bool) and v:
                    ok_flags.add(k)
            status_val = str(resp_json.get("status", "")).lower()
            if status_val in {"ok", "success", "completed", "done"}:
                ok_flags.add("status")
            if any(k in resp_json for k in ("tx", "transaction", "order_id", "orderId")):
                ok_flags.add("tx")

        ok = bool(ok_flags)

        if resp_json is None:
            ok = False

        body_text = (r.text or "")[:400]
        _log("info" if ok else "error", f"SEND result: ok={ok} status={r.status_code} flags={','.join(sorted(ok_flags)) or '-'} body={body_text}")

        return {
            "ok": ok,
            "status": r.status_code,
            "text": r.text,
            "json": resp_json
        }
    except Exception as e:
        _log("error", f"SEND exception: {e}")
        return {"ok": False, "status": 0, "text": str(e), "json": None}

def _authenticate_fragment(api_key: str, phone_number: str, version: str, mnemonics: List[str]) -> tuple[Optional[str], Optional[dict], int]:
    try:
        payload = {"api_key": api_key, "phone_number": phone_number, "version": version, "mnemonics": mnemonics}
        r = requests.post(FRAGMENT_AUTH_URL, json=payload,
                          headers={"Content-Type": "application/json", "Accept": "application/json"}, timeout=120)
        try: data = r.json()
        except Exception: data = {"raw": r.text}
        if r.status_code >= 400:
            return None, data, r.status_code
        token_candidates = []
        if isinstance(data, dict):
            for key in ("token","jwt","access","authorization","Authorization","auth","detail"):
                val = data.get(key)
                if isinstance(val, str) and len(val) > 10:
                    token_candidates.append(val)
            for subkey in ("data","result","payload"):
                sub = data.get(subkey)
                if isinstance(sub, dict):
                    for key in ("token","jwt","access"):
                        val = sub.get(key)
                        if isinstance(val, str) and len(val) > 10:
                            token_candidates.append(val)
        jwt = token_candidates[0] if token_candidates else None
        return jwt, data, r.status_code
    except Exception as e:
        logger.warning(f"Authenticate failed: {e}")
        return None, {"error": str(e)}, 0
    
def _queue_mode(chat_id: Any) -> int:
    try:
        cfg = _get_cfg_for_orders(chat_id)
        return int(cfg.get("queue_mode") or 1)
    except Exception:
        return 1

def _queue_timeout_sec(chat_id: Any) -> int:
    try:
        cfg = _get_cfg_for_orders(chat_id)
        v = cfg.get("queue_timeout_sec", int(os.getenv("FTS_QUEUE_TIMEOUT_SEC", "300")))
        return max(30, int(float(v)))
    except Exception:
        return int(os.getenv("FTS_QUEUE_TIMEOUT_SEC", "300"))

def _queue_mode_label(mode: int, timeout_sec: int) -> str:
    if mode == 1:
        return "СТРОГАЯ"
    if mode == 2:
        return "ПРОПУСК ГОТОВЫХ"
    if mode == 3:
        return f"ТАЙМАУТ→В КОНЕЦ ({timeout_sec//60}М)"
    return "СТРОГАЯ"

def _maybe_rotate_queue_head(cardinal: "Cardinal", any_chat_id: Any) -> bool:
    try:
        if _queue_mode(any_chat_id) != 3:
            return False

        q = _q(any_chat_id)
        if not q:
            return False

        head = q[0]
        if head.get("finalized") or not _allowed_stages(head):
            return False

        stage = str(head.get("stage"))
        if stage not in {"await_username", "await_confirm"}:
            return False

        ts = head.get("turn_ts")
        if not ts:
            if head.get("prompted") or _was_prompted(head.get("chat_id"), head.get("order_id")):
                head["turn_ts"] = time.time()
            return False

        if (time.time() - float(ts)) < float(_queue_timeout_sec(any_chat_id)):
            return False

        moved = q.pop(0)
        oid = moved.get("order_id")
        cid = moved.get("chat_id")

        moved["turn_ts"] = None
        moved["prompted"] = False
        moved["preconfirmed"] = False
        moved["auto_attempted_for"] = None
        moved["queue_notified"] = False

        if oid:
            _unmark_prompted(cid, oid, everywhere=True)

        q.append(moved)

        try:
            if cid is not None:
                _safe_send(cardinal, cid, "⏳ Вы не ответили — перенёс заказ в конец очереди. Напишите @username/«+», когда будете готовы.")
        except Exception:
            pass

        logger.info(f"[QUEUE] timeout move to end: OID={oid} CID={cid}")
        _notify_next_turn(cardinal, any_chat_id)
        return True

    except Exception as e:
        logger.debug(f"_maybe_rotate_queue_head failed: {e}")
        return False
    
def _is_too_many_attempts(raw_resp: Any) -> tuple[bool, Optional[int]]:
    text = ""
    try:
        if isinstance(raw_resp, dict):
            parts = []
            for k in ("non_field_errors", "errors", "detail", "message", "error"):
                v = raw_resp.get(k)
                if isinstance(v, list):
                    parts.extend([str(x) for x in v])
                elif isinstance(v, (str, int, float)):
                    parts.append(str(v))
            text = " ".join(parts) if parts else json.dumps(raw_resp, ensure_ascii=False)
        else:
            text = str(raw_resp)
    except Exception:
        text = str(raw_resp)

    low = text.lower()
    if "too many login attempts" in low:
        m = _re.search(r"in\s+(\d+)\s+seconds", text, _re.I)
        sec = int(m.group(1)) if m else None
        return True, sec
    return False, None

def _human_auth_error(raw_resp: Any, status: int) -> str:
    try:
        is_tma, wait_sec = _is_too_many_attempts(raw_resp)
        if is_tma:
            return f"Слишком много попыток входа. Подождите {wait_sec or 'несколько'} секунд и повторите."

        msgs = []
        if isinstance(raw_resp, dict):
            keys = ("non_field_errors", "errors", "detail", "message", "error", "phone_number", "mnemonics", "api_key")
            for k in keys:
                v = raw_resp.get(k)
                if isinstance(v, list):
                    msgs.extend([str(x) for x in v if x])
                elif isinstance(v, (str, int, float)) and str(v).strip():
                    msgs.append(str(v))

            for subkey in ("data", "result", "payload"):
                sub = raw_resp.get(subkey)
                if isinstance(sub, dict):
                    for k in keys:
                        v = sub.get(k)
                        if isinstance(v, list):
                            msgs.extend([str(x) for x in v if x])
                        elif isinstance(v, (str, int, float)) and str(v).strip():
                            msgs.append(str(v))

        elif isinstance(raw_resp, list):
            msgs.extend([str(x) for x in raw_resp if x])

        msg = " | ".join(m.strip() for m in msgs if m and str(m).strip())
        if not msg:
            msg = f"Сервер вернул статус {status}. Проверьте API-ключ, телефон и мнемофразу."

        return (msg[:500] + "…") if len(msg) > 500 else msg
    except Exception:
        return f"Не удалось создать токен (HTTP {status}). Проверьте данные и повторите."

def _get_my_lots_by_category(cardinal: "Cardinal", category_id: int) -> Dict[int, Any]:
    lots: Dict[int, Any] = {}
    try:
        cardinal.update_lots_and_categories()

        all_map = (cardinal.tg_profile.get_sorted_lots(2) or {})

        for subcat, items in all_map.items():
            try:
                sid = int(
                    getattr(subcat, "id", None)
                    or getattr(subcat, "subcategory_id", None)
                    or getattr(subcat, "category_id", None)
                    or 0
                )
            except Exception:
                sid = 0

            if sid != int(category_id):
                continue

            if isinstance(items, dict):
                for key, val in items.items():
                    lid = getattr(key, "id", key)
                    try:
                        lid = int(lid)
                    except Exception:
                        continue
                    lots[lid] = val
            else:
                seq = items if isinstance(items, (list, tuple, set)) else []
                for val in seq:
                    lid = getattr(val, "id", None)
                    if lid is not None:
                        try:
                            lots[int(lid)] = val
                        except Exception:
                            pass
    except Exception as e:
        logger.warning(f"_get_my_lots_by_category failed: {e}")
    return lots

def _is_stars_lot(cardinal: "Cardinal", lot_id: int) -> bool:
    try:
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return False

        sub = getattr(fields, "subcategory", None) or getattr(fields, "subcat", None)
        cid = None

        if sub is not None:
            for attr in ("id", "subcategory_id", "category_id"):
                if hasattr(sub, attr):
                    cid = getattr(sub, attr)
                    break

        if cid is None:
            for attr in ("subcategory_id", "category_id"):
                if hasattr(fields, attr):
                    cid = getattr(fields, attr)
                    break

        return cid is not None and int(cid) == int(FNP_STARS_CATEGORY_ID)
    except Exception:
        return False

def _order_is_stars(order: Any) -> bool:
    try:
        cand = (
            getattr(order, "subcategory_id", None)
            or getattr(order, "category_id", None)
            or getattr(getattr(order, "subcategory", None), "id", None)
            or getattr(getattr(order, "category", None), "id", None)
        )
        if cand is None:
            return False
        return int(cand) == int(FNP_STARS_CATEGORY_ID)
    except Exception:
        return False

def _activate_lot(cardinal: "Cardinal", lot_id: int) -> bool:
    try:
        if not _is_stars_lot(cardinal, lot_id):
            logger.warning(f"_activate_lot skipped: lot {lot_id} not in category {FNP_STARS_CATEGORY_ID}")
            return False
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return False
        if not getattr(fields, "active", False):
            fields.active = True
            cardinal.account.save_lot(fields)
        return True
    except Exception as e:
        logger.warning(f"_activate_lot {lot_id} failed: {e}")
        return False

def _deactivate_lot(cardinal: "Cardinal", lot_id: int) -> bool:
    try:
        if not _is_stars_lot(cardinal, lot_id):
            logger.warning(f"_deactivate_lot skipped: lot {lot_id} not in category {FNP_STARS_CATEGORY_ID}")
            return False
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return False
        if getattr(fields, "active", False):
            fields.active = False
            cardinal.account.save_lot(fields)
        return True
    except Exception as e:
        logger.warning(f"_deactivate_lot {lot_id} failed: {e}")
        return False

def _apply_star_lots_state(cardinal: "Cardinal", star_lots: List[dict], enabled: bool) -> Dict[str, List[int]]:
    report = {"ok": [], "skip": [], "err": []}
    for it in star_lots or []:
        lot_id = it.get("lot_id")
        if not lot_id:
            continue
        try:
            if not _is_stars_lot(cardinal, int(lot_id)):
                report["skip"].append(int(lot_id))
                continue
            ok = _activate_lot(cardinal, lot_id) if enabled else _deactivate_lot(cardinal, lot_id)
            (report["ok"] if ok else report["skip"]).append(int(lot_id))
        except Exception as e:
            report["err"].append(int(lot_id))
            logger.warning(f"apply_star_lots_state {lot_id} failed: {e}")
    return report

def _apply_category_state(cardinal: "Cardinal", category_id: int, enabled: bool) -> Dict[str, List[int]]:
    category_id = int(category_id or FNP_STARS_CATEGORY_ID)
    report = {"ok": [], "skip": [], "err": []}

    lots = _get_my_lots_by_category(cardinal, category_id)
    for key, _ in (lots or {}).items():
        try:
            lot_id = int(getattr(key, "id", key))
            ok = _activate_lot(cardinal, lot_id) if enabled else _deactivate_lot(cardinal, lot_id)
            (report["ok"] if ok else report["skip"]).append(lot_id)
        except Exception as e:
            try:
                bad_id = int(getattr(key, "id", 0) or 0)
            except Exception:
                bad_id = 0
            report["err"].append(bad_id)
            logger.warning(f"apply_category_state {key} failed: {e}")
    return report

def _event_chat_id(e) -> Any:
    return (
        getattr(getattr(e, "message", None), "chat_id", None)
        or getattr(e, "chat_id", None)
        or getattr(getattr(e, "order", None), "chat_id", None)
    )

def _parse_fragment_error_text(response_text: str, status_code: int = 0) -> str:
    fallback = "Ошибка обработки заказа."
    try:
        data = json.loads(response_text)
    except Exception:
        data = None
    
    low_text = (response_text or "").lower()
    if "seqno" in low_text and ("exit code -256" in low_text or 'get method "seqno"' in low_text):
        return "Неверная версия кошелька или кошелёк не инициализирован. Пересоздайте токен, выбрав правильную версию (W5/V4R2), и выполните небольшую исходящую транзакцию."

    if status_code == 429:
        return "Слишком много запросов. Попробуйте ещё раз через минуту."
    if status_code in (500, 502, 503, 504):
        return "Сервис Fragment временно недоступен. Повторите позже."
    if status_code in (401, 403):
        return "Нужна повторная авторизация продавца. Попробуем ещё раз чуть позже."
    if ("liteserver" in low_text) or ("lite server" in low_text):
        return "Сервис TON/Fragment временно недоступен. Попробуйте ещё раз позже."

    if isinstance(data, dict):
        if "username" in data:
            return "Неверный Telegram-тег (проверьте @username)."
        if "quantity" in data:
            return "Минимальное количество для покупки — 50 ⭐."
        for k in ("detail", "message", "error"):
            if data.get(k):
                msg = str(data[k])
                if "not enough" in msg.lower() or "balance" in msg.lower():
                    return "Недостаточно средств на кошельке Fragment."
                if "version" in msg.lower():
                    return "Неверная версия кошелька у продавца."
                if "username" in msg.lower():
                    return "Пользователь с таким @username не найден."
                return msg[:200]
        if isinstance(data.get("errors"), list):
            joined = " | ".join(str(x.get("error") or x) for x in data["errors"][:3])
            if "balance" in joined.lower():
                return "Недостаточно средств на кошельке Fragment."
            return (joined or fallback)[:200]
        if isinstance(data.get("data"), dict):
            inner = data["data"]
            for k in ("error", "message", "detail"):
                if inner.get(k):
                    return str(inner[k])[:200]
    elif isinstance(data, list) and data:
        txt = " | ".join(str(x) for x in data[:3])
        return txt[:200]

    return fallback

def _classify_send_failure(resp_text: str, status: int, username: str, jwt: Optional[str]) -> tuple[str, str]:
    if status in (401, 403):
        return "seller", "Нужна повторная авторизация продавца."
    if status == 429:
        return "seller", "Слишком много запросов. Попробуйте ещё раз через минуту."
    if status in (500, 502, 503, 504):
        return "seller", "Сервис Fragment временно недоступен. Повторите позже."

    reason = _parse_fragment_error_text(resp_text, status)
    low = (reason or "").lower()

    if "seqno" in (resp_text or "").lower():
        return "seller", "Неверная версия кошелька у продавца или кошелёк не инициализирован."

    if any(t in low for t in ("username", "user not found", "not found", "invalid", "does not exist")):
        return "username", "Пользователь с таким @username не найден."

    if status == 400 and username and not _check_username_exists_throttled(username, jwt):
        return "username", "Пользователь с таким @username не найден."

    if any(t in low for t in ("balance", "not enough")):
        return "seller", "Недостаточно средств на кошельке Fragment."

    return "seller", reason or "Ошибка обработки заказа."

def _auto_refund_order(cardinal: "Cardinal", order_id: Any, chat_id: Any, reason: str) -> bool:
    try:
        cardinal.account.refund(order_id)
        _safe_send(cardinal, chat_id, "✅ Средства успешно возвращены.")
        logger.warning(f"[REFUND] Заказ {order_id}: возврат выполнен. Причина: {reason}")
        return True
    except Exception as e:
        logger.error(f"[REFUND] Не удалось вернуть средства за заказ {order_id}: {e}")
        _safe_send(cardinal, chat_id, "❌ Не удалось оформить возврат автоматически. Свяжитесь с админом.")
        return False

def _maybe_auto_deactivate(cardinal: "Cardinal", cfg: dict, chat_id: Optional[Any] = None):
    jwt = cfg.get("fragment_jwt")
    ver, bal, _raw = _check_fragment_wallet(jwt) if jwt else (None, None, None)
    if bal is None:
        logger.warning("[BALANCE] Не удалось получить баланс Fragment.")
        return
    thr = float(cfg.get("min_balance_ton") or FNP_MIN_BALANCE_TON)
    if bal < thr and cfg.get("auto_deactivate", False):
        cat_id = FNP_STARS_CATEGORY_ID
        rep = _apply_category_state(cardinal, cat_id, False)
        items = cfg.get("star_lots") or []
        for it in items:
            it["active"] = False
        _set_cfg(
            chat_id if chat_id is not None else "__orders__",
            lots_active=False,
            star_lots=items,
            last_auto_deact_reason=f"Баланс {bal} < порога {thr}"
        )
        logger.warning(f"[AUTODEACT] Баланс {bal} < {thr}. Выключены лоты категории {cat_id}. Report={rep}")

CBT_HOME       = f"{UUID}:home"
CBT_SETTINGS   = f"{UUID}:settings"
CBT_FSM_CANCEL = f"{UUID}:fsm_cancel"
CBT_BACK_PLUGINS = getattr(CBT, "BACK", f"{UUID}:back")
CBT_TOGGLE_PLUGIN = f"{UUID}:toggle_plugin"

CBT_TOGGLE_LOTS   = f"{UUID}:toggle_lots"
CBT_TOGGLE_REFUND = f"{UUID}:toggle_refund"
CBT_TOGGLE_DEACT  = f"{UUID}:toggle_deact"
CBT_REFRESH       = f"{UUID}:refresh"
CBT_SET_MIN_BAL   = f"{UUID}:set_min_balance"
CBT_TOGGLE_MANUAL_REFUND = f"{UUID}:toggle_manual_refund"
CBT_TOGGLE_BACK_PRIORITY = f"{UUID}:toggle_back_priority"

CBT_TOKEN         = f"{UUID}:token"
CBT_CREATE_JWT    = f"{UUID}:create_jwt"
CBT_JWT_CONFIRMED = f"{UUID}:jwt_confirmed"
CBT_JWT_RESEND    = f"{UUID}:jwt_resend"
CBT_MESSAGES      = f"{UUID}:msgs"
CBT_MSG_EDIT_P    = f"{UUID}:msg_edit:"
CBT_MSG_RESET_P   = f"{UUID}:msg_reset:"
CBT_SET_JWT       = f"{UUID}:set_jwt"
CBT_DEL_JWT       = f"{UUID}:del_jwt"

CBT_STARS         = f"{UUID}:stars"
CBT_STAR_ADD      = f"{UUID}:star_add"
CBT_STAR_ACT_ALL  = f"{UUID}:star_act_all"
CBT_STAR_DEACT_ALL= f"{UUID}:star_deact_all"
CBT_STAR_TOGGLE_P = f"{UUID}:star_toggle:"
CBT_STAR_DEL_P    = f"{UUID}:star_del:"

CBT_CONFIRM_SEND    = f"{UUID}:confirm_send"
CBT_CHANGE_USERNAME = f"{UUID}:change_username"
CBT_CANCEL_FLOW     = f"{UUID}:cancel_flow"
CBT_TOGGLE_PREORDER = f"{UUID}:toggle_preorder_username"

CBT_MARKUP         = f"{UUID}:markup"
CBT_MARKUP_APPLY   = f"{UUID}:markup_apply"
CBT_MARKUP_CHANGE  = f"{UUID}:markup_change"
CBT_MINI_SETTINGS = f"{UUID}:mini"
CBT_STAR_PRICE_P  = f"{UUID}:star_price:"
CBT_MARKUP_RESET  = f"{UUID}:markup_reset"
CBT_LOGS = f"{UUID}:logs"
CBT_STATS          = f"{UUID}:stats"
CBT_STATS_RANGE_P  = f"{UUID}:stats_range:"
CBT_DELETE_ASK = f"{UUID}:delete_ask"
CBT_DELETE_YES = f"{UUID}:delete_yes"
CBT_DELETE_NO  = f"{UUID}:delete_no"
CBT_PLUGINS_LIST_OPEN = f"{getattr(CBT, 'PLUGINS_LIST', '44')}:0"
CBT_TOGGLE_LITESERVER_RETRY = f"{UUID}:toggle_liteserver_retry"
CBT_TOGGLE_USERNAME_CHECK   = f"{UUID}:toggle_username_check"
CBT_TOGGLE_AUTOSEND_PLUS = f"{UUID}:toggle_autosend_plus"
CBT_TOGGLE_QUEUE_MODE = f"{UUID}:toggle_queue_mode"

_fsm: dict[int, dict] = {}

def _home_kb() -> InlineKeyboardMarkup:
    kb = K()
    kb.row(
        B("⚙️ Настройки", callback_data=CBT_SETTINGS),
        B("📖 Инструкция", url=INSTRUCTION_URL)
    )

    kb.add(B("🗑 Удалить плагин", callback_data=CBT_DELETE_ASK))
    kb.add(B("🔙 К списку плагинов", callback_data=CBT_PLUGINS_LIST_OPEN))
    return kb

def _settings_kb(chat_id: Any) -> InlineKeyboardMarkup:
    cfg = _get_cfg(chat_id)

    def onoff(v: bool) -> str:
        return "🟢 Включено" if v else "🔴 Выключено"

    def onoff_short(v: bool) -> str:
        return "🟢 Включён" if v else "🔴 Выключен"

    kb = K()

    kb.row(B(f"Плагин: {onoff(cfg.get('plugin_enabled', True))}", callback_data=CBT_TOGGLE_PLUGIN))

    state_txt, _ = _lots_state_summary(cfg)
    kb.row(B(f"Лоты: {state_txt}", callback_data=CBT_TOGGLE_LOTS))

    kb.row(
        B(f"Автовозврат: {onoff_short(cfg.get('auto_refund', False))}", callback_data=CBT_TOGGLE_REFUND),
        B(f"Автодеактивация: {onoff(cfg.get('auto_deactivate', True))}", callback_data=CBT_TOGGLE_DEACT)
    )

    kb.row(
        B(f"Команда !бэк: {onoff(cfg.get('manual_refund_enabled', False))}", callback_data=CBT_TOGGLE_MANUAL_REFUND),
        B(f"Ник из заказа: {onoff_short(cfg.get('preorder_username', False))}", callback_data=CBT_TOGGLE_PREORDER)
    )

    kb.row(B("🔐 Токен", callback_data=CBT_TOKEN))
    kb.row(B("⚙️ Настройка лотов", callback_data=CBT_STARS))
    kb.row(B("🛠️ Мини-настройки", callback_data=CBT_MINI_SETTINGS))
    kb.row(B("📜 Логи", callback_data=CBT_LOGS))
    kb.row(B("📊 Статистика", callback_data=CBT_STATS))
    kb.row(B("🔄 Обновить", callback_data=CBT_REFRESH))
    kb.add(B("🏠 Домой", callback_data=CBT_HOME))
    kb.add(B("◀️ Назад", callback_data=CBT_HOME))
    return kb

def _mini_settings_text(chat_id: Any) -> str:
    cfg = _get_cfg(chat_id)
    prio = "ВЫШЕ автовозврата" if cfg.get("manual_refund_priority", True) else "НИЖЕ автовозврата"
    cur_min = cfg.get("min_balance_ton", FNP_MIN_BALANCE_TON)
    retry_state = _state_on(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT))
    check_state = "🔴 выключена (проверим при отправке)" if cfg.get("skip_username_check", False) else "🟢 включена"
    autosend = cfg.get("auto_send_without_plus", False)
    plus_state = "🟢 не нужно (автоотправка)" if autosend else "🟡 нужно (как раньше)"
    mode = _queue_mode(chat_id)
    timeout = _queue_timeout_sec(chat_id)
    qtxt = _queue_mode_label(mode, timeout)
    return (
        "<b>Мини-настройки</b>\n\n"
        f"• Приоритет !бэк: <b>{prio}</b>\n"
        f"• Очередь: <b>{qtxt}</b>\n"
        f"• Мин. баланс TON: <code>{cur_min}</code>\n"
        f"• Повтор при LiteServer: <b>{retry_state}</b>\n"
        f"• Проверка @username при вводе: <b>{check_state}</b>\n"
        f"• Подтверждение «+»: <b>{plus_state}</b>\n"
        "• Сообщения: редактирование шаблонов ответов покупателю\n\n"
        "Выберите действие ниже."
    )

def _mini_settings_kb(chat_id: Any) -> InlineKeyboardMarkup:
    cfg = _get_cfg(chat_id)
    prio_label = "⬆️ Приоритет !бэк: ВЫШЕ" if cfg.get("manual_refund_priority", True) else "⬇️ Приоритет !бэк: НИЖЕ"
    kb = K()
    mode = _queue_mode(chat_id)
    timeout = _queue_timeout_sec(chat_id)
    queue_label = f"🧾 Очередь: {_queue_mode_label(mode, timeout)}"
    kb.row(B(queue_label, callback_data=CBT_TOGGLE_QUEUE_MODE))
    kb.row(B(prio_label, callback_data=CBT_TOGGLE_BACK_PRIORITY))
    kb.row(B(f"🔋 Мин. баланс: {cfg.get('min_balance_ton', FNP_MIN_BALANCE_TON)} TON", callback_data=CBT_SET_MIN_BAL))
    retry_label = "🔁 LiteServer-ретрай: ВКЛ" if cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT) else "🔁 LiteServer-ретрай: ВЫКЛ"
    kb.row(B(retry_label, callback_data=CBT_TOGGLE_LITESERVER_RETRY))
    ucheck_label = "🔎 Проверка @username: ВКЛ" if not cfg.get("skip_username_check", False) else "🚫 Проверка @username: ВЫКЛ"
    autosend = cfg.get("auto_send_without_plus", False)
    autosend_label = "⚡ Автоотправка без '+': ВКЛ" if autosend else "✋ Автоотправка без '+': ВЫКЛ (нужен '+')"
    kb.row(B(autosend_label, callback_data=CBT_TOGGLE_AUTOSEND_PLUS))
    kb.row(B(ucheck_label, callback_data=CBT_TOGGLE_USERNAME_CHECK))
    kb.row(B("🧩 Сообщения", callback_data=CBT_MESSAGES))
    kb.add(B("◀️ Назад", callback_data=CBT_SETTINGS))
    return kb

def _open_mini_settings(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _mini_settings_text(chat_id), _mini_settings_kb(chat_id))
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _token_kb() -> InlineKeyboardMarkup:
    kb = K()
    kb.add(B("🧩 Создать токен", callback_data=CBT_CREATE_JWT),
            B("📥 Импорт токена", callback_data=CBT_SET_JWT))
    kb.row(B("♻️ Пересоздать токен", callback_data=CBT_CREATE_JWT),
           B("🗑 Удалить токен", callback_data=CBT_DEL_JWT))
    kb.add(B("🏠 Домой", callback_data=CBT_HOME))
    kb.add(B("◀️ Назад", callback_data=CBT_SETTINGS))
    return kb

def _stars_kb(chat_id: Any) -> InlineKeyboardMarkup:
    cfg = _get_cfg(chat_id)
    kb = K()
    for it in (cfg.get("star_lots") or [])[:10]:
        lot_id = it.get("lot_id"); qty = it.get("qty")
        state = "🟢 ON" if it.get("active") else "🔴 OFF"
        kb.row(
            B(f"{qty}⭐  LOT {lot_id}  {state}", callback_data=f"{CBT_STAR_TOGGLE_P}{lot_id}"),
            B("💰 Цена", callback_data=f"{CBT_STAR_PRICE_P}{lot_id}"),
            B("🗑", callback_data=f"{CBT_STAR_DEL_P}{lot_id}")
        )
    kb.row(
        B("➕ Добавить лот", callback_data=CBT_STAR_ADD),
        B("💹 Наценка", callback_data=CBT_MARKUP)
    )
    kb.row(B("♻️ Сбросить наценку", callback_data=CBT_MARKUP_RESET))
    kb.row(B("🔄 Обновить", callback_data=CBT_REFRESH))
    kb.row(
        B("⚡ Включить все", callback_data=CBT_STAR_ACT_ALL),
        B("💤 Выключить все", callback_data=CBT_STAR_DEACT_ALL)
    )
    kb.add(B("◀️ Назад", callback_data=CBT_SETTINGS))
    return kb

_MSG_TITLES = {
    "purchase_created": "После покупки (просим ник)",
    "username_received": "Ник получен (уведомление)",
    "username_invalid": "Ник неверный/не найден",
    "username_valid": "Ник верный (подтверждение)",
    "sending": "Отправка звёзд (процесс)",
    "sent": "Отправлено успешно",
    "failed": "Не удалось отправить",
    "your_turn": "Дошла очередь (просим ник)",
}

def _messages_text(chat_id: Any) -> str:
    tpls = _get_cfg(chat_id).get("templates") or _default_templates()

    pend = _current(chat_id)
    if pend:
        oid = pend.get("order_id") or "ABC123"
        qty = int(pend.get("qty", 50)) or 150
        uname = (pend.get("candidate") or "@username")
        order_url = f"https://funpay.com/orders/{oid}/"
    else:
        oid = "ABC123"
        qty = 150
        uname = "@username"
        order_url = "https://funpay.com/orders/ABC123/"

    lines = [
        "<b>Кастомные сообщения</b>",
        "",
        "Плейсхолдеры (что это и зачем):",
        "• {qty} — количество звёзд в заказе",
        "• {username} — ник покупателя (с @), подставится автоматически",
        "• {order_id} — номер заказа на FunPay",
        "• {order_url} — ссылка на страницу заказа",
        "• {reason} — краткая причина ошибки при неудачной отправке",
        "",
        "Текущие значения (пример):",
        f"qty={qty} username={uname} order_id={oid} order_url={order_url}",
        "",
        "Выберите шаблон ниже, чтобы изменить:"
    ]

    for key, title in _MSG_TITLES.items():
        preview = (tpls.get(key) or "").replace("\n", " ")[:70]
        lines.append(f"• <b>{title}</b>\n{preview}")

    return "\n".join(lines)

def _messages_kb(chat_id: Any) -> InlineKeyboardMarkup:
    kb = K()
    for key, title in list(_MSG_TITLES.items()):
        kb.row(B(f"✏️ {title}", callback_data=f"{CBT_MSG_EDIT_P}{key}"),
               B("♻️", callback_data=f"{CBT_MSG_RESET_P}{key}"))
    kb.add(B("◀️ Назад", callback_data=CBT_SETTINGS))
    return kb

def _open_messages(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _messages_text(chat_id), _messages_kb(chat_id))
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _msg_edit_start(bot, call):
    chat_id = call.message.chat.id
    key = call.data.split(":")[-1]
    _fsm[chat_id] = {"step": "msg_edit_value", "msg_key": key}

    pend = _current(chat_id)
    if pend:
        oid = pend.get("order_id") or "ABC123"
        qty = int(pend.get("qty", 50)) or 150
        uname = (pend.get("candidate") or "@username")
        order_url = f"https://funpay.com/orders/{oid}/"
    else:
        oid = "ABC123"
        qty = 150
        uname = "@username"
        order_url = "https://funpay.com/orders/ABC123/"

    cfg = _get_cfg(chat_id)
    tpls = cfg.get("templates") or _default_templates()
    cur_text = tpls.get(key, _default_templates().get(key, ""))

    try:
        bot.answer_callback_query(call.id,
            "Введите новый текст шаблона. Можно использовать {qty}, {username}, {order_id}, {order_url}, {reason}")
    except Exception:
        pass

    title = _MSG_TITLES.get(key, key)

    text_block = (
        f"Изменение: {title}\n\n"
        "Доступные плейсхолдеры:\n"
        "{qty} {username} {order_id} {order_url} {reason}\n"
        "Текущие значения (пример):\n"
        f"qty={qty} username={uname} order_id={oid} order_url={order_url}\n\n"
        "Текущий текст шаблона:\n"
        f"{cur_text}\n\n"
        "Пришлите новый текст (или /cancel)."
    )
    m = bot.send_message(chat_id, text_block, reply_markup=_kb_cancel_fsm())
    st = _fsm.get(chat_id, {})
    st["prompt_msg_id"] = getattr(m, "message_id", None)
    _fsm[chat_id] = st

def _msg_reset(bot, call):
    chat_id = call.message.chat.id
    key = call.data.split(":")[-1]
    cfg = _get_cfg(chat_id)
    tpls = cfg.get("templates") or {}
    defaults = _default_templates()
    if key in defaults:
        tpls[key] = defaults[key]
        _set_cfg(chat_id, templates=tpls)
    try: bot.answer_callback_query(call.id, "Сброшено по умолчанию")
    except Exception: pass
    _open_messages(bot, call)

def _kb_cancel_fsm() -> InlineKeyboardMarkup:
    kb = K()
    kb.add(B("❌ Отменить ввод", callback_data=CBT_FSM_CANCEL))
    return kb

def _fsm_cancel(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    st = _fsm.pop(chat_id, None)
    pmid = (st or {}).get("prompt_msg_id")
    _safe_delete(cardinal.telegram.bot, chat_id, pmid)
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, "Отменено.")
    except Exception:
        pass
    m = cardinal.telegram.bot.send_message(chat_id, "❌ Отменено.")
    _safe_delete(cardinal.telegram.bot, chat_id, getattr(m, "message_id", None))

def _looks_like_paid(text: str) -> bool:
    t = (text or "").lower()
    return ("оплатил заказ" in t) or ("заказ оплачен" in t) or ("paid the order" in t) or ("order paid" in t)

def _parse_order_info_from_text(text: str) -> tuple[Optional[int], Optional[str]]:
    if not text:
        return None, None
    oid = None
    m = _re.search(r"#([A-Z0-9]{6,})", text, _re.I)
    if m:
        oid = m.group(1)

    qty = None
    m = _re.search(r"(\d{2,7})\s*(?:зв[её]зд|stars|⭐)", text, _re.I)
    if m:
        try:
            v = int(m.group(1))
            if v >= 50:
                qty = v
        except Exception:
            pass
    if qty is None:
        for n in _re.findall(r"\d{2,7}", text):
            try:
                v = int(n)
                if v >= 50:
                    qty = v
                    break
            except Exception:
                pass
    return qty, oid

def _validate_username(u: str) -> bool:
    if not u:
        return False
    u = u.strip().lstrip('@')
    return bool(_re.fullmatch(r"[A-Za-z0-9_]{5,32}", u))

def _funpay_is_system_paid_message(text: str) -> bool:
    if not text:
        return False
    t = text.lower()

    is_paid = (
        "оплатил заказ" in t
        or "заказ оплачен" in t
        or "paid the order" in t
        or "order paid" in t
    )

    in_stars_category = (
        "telegram, звёзды" in t
        or "telegram, звезды" in t
        or "telegram, stars" in t
    )

    is_gifts = _is_gift_like_text(t)
    is_account = _mentions_account_login(t)

    return is_paid and in_stars_category and not is_gifts and not is_account

def _funpay_extract_qty_and_order_id(text: str) -> tuple[Optional[int], Optional[str]]:
    qty = None
    oid = None
    try:
        m = _re.search(r"(?:заказ|order|орд[её]р|№)\s*#?\s*([A-Za-z0-9\-]{6,})", text, _re.IGNORECASE)
        if m:
            oid = m.group(1)
        m2 = _re.search(r"(\d+)\s*(?:зв[её]зд|stars|⭐️|⭐)", text, _re.IGNORECASE)
        if m2:
            qty = int(m2.group(1))
    except Exception:
        pass
    return qty, oid

def _is_gift_like_text(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(x in t for x in ("подарок", "подарком", "подарки", "подароч", "gift", "в подарок"))

def _mentions_account_login(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    patterns = [
        r"с\s*заходом\s*на\s*аккаунт",
        r"заход\s*на\s*аккаунт",
        r"вход\s*(?:в|на)?\s*аккаунт",
        r"логин\s*в\s*аккаунт",
        r"login\s*to\s*account",
        r"sign\s*in\s*to\s*account",
    ]
    return any(_re.search(p, t) for p in patterns)

def _deactivate_all_star_lots(cardinal: "Cardinal", cfg: dict, chat_id: Any, reason: str = "временная ошибка/невалидный заказ") -> None:
    try:
        if _CARDINAL_REF is not None:
            _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, False)
        items = cfg.get("star_lots") or []
        for it in items:
            it["active"] = False
        _set_cfg(chat_id, lots_active=False, star_lots=items, last_auto_deact_reason=reason)
    except Exception as e:
        logger.warning(f"_deactivate_all_star_lots failed: {e}")

def _preview_kb() -> InlineKeyboardMarkup:
    kb = K()
    kb.row(B("✅ Отправить", callback_data=CBT_CONFIRM_SEND),
           B("🔁 Изменить ник", callback_data=CBT_CHANGE_USERNAME))
    kb.add(B("❌ Отмена", callback_data=CBT_CANCEL_FLOW))
    return kb

def _lots_state_summary(cfg: dict) -> tuple[str, Optional[bool]]:
    items = cfg.get("star_lots") or []
    if items:
        total = len(items)
        on = sum(1 for it in items if it.get("active"))
        if on == 0:
            return ("🔴 Выключены", False)
        if on == total:
            return ("🟢 Включены", True)
        return ("🟡 Частично", None)
    return ("🟢 Включены" if cfg.get("lots_active") else "🔴 Выключены", bool(cfg.get("lots_active")))

def _format_currency(value: float, currency: Optional[str]) -> str:
    try:
        v = float(value)
    except Exception:
        return str(value)
    cur = getattr(currency, "name", currency)
    cur = (str(cur) or "RUB").upper()
    if cur in ("RUB", "RUR", "₽"):
        v = round(v)
        return f"{int(v)}₽"
    return f"{v:.2f} {cur}"

def _collect_markup_targets(cardinal: "Cardinal", cfg: dict, percent: float) -> List[dict]:
    targets: List[dict] = []
    star_lots = cfg.get("star_lots") or []
    lot_ids: List[int] = []
    qty_map: Dict[int, Optional[int]] = {}
    if star_lots:
        for it in star_lots:
            try:
                lot_id = int(it.get("lot_id"))
                lot_ids.append(lot_id)
                qty_map[lot_id] = int(it.get("qty")) if it.get("qty") else None
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
            title = getattr(fields, "title", None) or getattr(fields, "name", None) or ""
            old_price = None
            for price_attr in ("price", "cost", "amount", "price_rub"):
                if hasattr(fields, price_attr):
                    try:
                        old_price = float(getattr(fields, price_attr))
                        break
                    except Exception:
                        pass
            if old_price is None:
                continue
            currency = getattr(fields, "currency", None) or getattr(fields, "cur", None) or "RUB"
            qty = qty_map.get(lot_id)
            if qty is None:
                q = _extract_qty_from_title(title)
                qty = int(q) if q else None

            new_price = round(old_price * (1.0 + percent / 100.0), 2)
            if getattr(currency, "name", str(currency)).upper() in ("RUB", "RUR", "₽"):
                new_price = float(int(round(new_price)))
            targets.append({
                "lot_id": lot_id,
                "title": title,
                "qty": qty,
                "currency": currency,
                "old_price": old_price,
                "new_price": new_price,
                "diff": round(new_price - old_price, 2)
            })
        except Exception as e:
            logger.debug(f"_collect_markup_targets: lot {lot_id} skipped: {e}")
            continue
    return targets

def _collect_reset_markup_targets(cardinal: "Cardinal", cfg: dict, percent: float) -> List[dict]:
    if abs(float(percent or 0.0)) < 1e-12:
        return []
    rows: List[dict] = []

    star_lots = cfg.get("star_lots") or []
    if star_lots:
        lot_ids = [int(it.get("lot_id")) for it in star_lots if it.get("lot_id")]
    else:
        lots = _get_my_lots_by_category(cardinal, FNP_STARS_CATEGORY_ID) if cardinal else {}
        lot_ids = [int(lid) for lid in lots.keys()]
    seen = set()
    for lot_id in lot_ids:
        if lot_id in seen:
            continue
        seen.add(lot_id)
        try:
            if _CARDINAL_REF and not _is_stars_lot(_CARDINAL_REF, lot_id):
                continue

            fields = _CARDINAL_REF.account.get_lot_fields(lot_id) if _CARDINAL_REF else None
            if not fields:
                continue

            cur_price = None
            for price_attr in ("price", "cost", "amount", "price_rub"):
                if hasattr(fields, price_attr):
                    try:
                        cur_price = float(getattr(fields, price_attr))
                        break
                    except Exception:
                        pass
            if cur_price is None:
                continue
            currency = getattr(fields, "currency", None) or getattr(fields, "cur", None) or "RUB"
            title = getattr(fields, "title", None) or getattr(fields, "name", None) or ""
            qty = _extract_qty_from_title(title)

            factor = 1.0 + float(percent) / 100.0
            new_price = cur_price / factor

            curr_name = getattr(currency, "name", str(currency)).upper()
            new_price = float(int(round(new_price))) if curr_name in ("RUB", "RUR", "₽") else round(new_price, 2)

            rows.append({
                "lot_id": lot_id,
                "title": title,
                "qty": qty,
                "currency": currency,
                "old_price": cur_price,
                "new_price": new_price,
                "diff": round(new_price - cur_price, 2)
            })
        except Exception:
            continue
    return rows

def _cb_markup_reset(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    try: cardinal.telegram.bot.answer_callback_query(call.id)
    except Exception: pass

    p = float(cfg.get("markup_percent") or 0.0)
    if abs(p) < 1e-12:
        cardinal.telegram.bot.send_message(chat_id, "ℹ️ Наценка уже 0%. Нечего сбрасывать.")
        return

    if _CARDINAL_REF is None:
        cardinal.telegram.bot.send_message(chat_id, "⚠️ Внутренняя ошибка: нет ссылки на Cardinal.")
        return

    rows = _collect_reset_markup_targets(cardinal, cfg, p)
    if not rows:
        cardinal.telegram.bot.send_message(chat_id, "Не нашёл лотов для отката наценки.")
        return

    rep = _apply_markup_prices(cardinal, rows)
    okn = len(rep["ok"]); ern = len(rep["err"]); total = len(rows)

    _set_cfg(chat_id, markup_percent=0.0)
    msg = f"✅ Сброс наценки выполнен: обновлено {okn} из {total} лот(ов)."
    if ern:
        msg += f"\n⚠️ Ошибок: {ern}. См. логи."
    cardinal.telegram.bot.send_message(chat_id, msg)

def _markup_preview_text(percent: float, rows: List[dict]) -> str:
    lines = [f"<b>Наценка: {percent}%</b>"]
    if not rows:
        lines.append("Лотов не найдено.")
        return "\n".join(lines)

    total_old = 0.0
    total_new = 0.0
    lines.append("")

    for r in rows[:20]:
        lot_id = r["lot_id"]
        qty = r.get("qty")
        cur = r.get("currency")
        oldp = r["old_price"]; newp = r["new_price"]; diff = r["diff"]
        total_old += float(oldp)
        total_new += float(newp)
        qty_part = f"{qty}⭐ — " if qty else ""
        lines.append(
            f"• LOT <code>{lot_id}</code> — {qty_part}"
            f"{_format_currency(oldp, cur)} → <b>{_format_currency(newp, cur)}</b> "
            f"(+{_format_currency(diff, cur)})"
        )

    more = len(rows) - 20
    if more > 0:
        lines.append(f"… и ещё {more} лот(ов)")

    lines.append("")
    lines.append(
        f"Итого: { _format_currency(total_old, rows[0]['currency'] if rows else 'RUB') } → "
        f"<b>{ _format_currency(total_new, rows[0]['currency'] if rows else 'RUB') }</b> "
        f"(+{ _format_currency(total_new - total_old, rows[0]['currency'] if rows else 'RUB') })"
    )
    lines.append("")
    lines.append("Подтвердите наценку или измените процент.")
    return "\n".join(lines)

def _kb_markup_preview() -> InlineKeyboardMarkup:
    kb = K()
    kb.row(B("✅ Подтвердить", callback_data=CBT_MARKUP_APPLY),
           B("✏️ Изменить %", callback_data=CBT_MARKUP_CHANGE))
    kb.add(B("❌ Отмена", callback_data=CBT_FSM_CANCEL))
    return kb

def _apply_markup_prices(cardinal: "Cardinal", rows: List[dict]) -> Dict[str, List[int]]:
    rep = {"ok": [], "err": []}
    for r in rows:
        lot_id = int(r["lot_id"])
        new_price = float(r["new_price"])
        try:
            if not _is_stars_lot(cardinal, lot_id):
                rep["err"].append(lot_id)
                continue

            fields = cardinal.account.get_lot_fields(lot_id)
            if not fields:
                rep["err"].append(lot_id); continue
            set_ok = False
            for price_attr in ("price", "cost", "amount", "price_rub"):
                if hasattr(fields, price_attr):
                    try:
                        setattr(fields, price_attr, new_price)
                        set_ok = True
                        break
                    except Exception:
                        pass
            if not set_ok:
                rep["err"].append(lot_id); continue

            cardinal.account.save_lot(fields)
            rep["ok"].append(lot_id)
        except Exception as e:
            logger.warning(f"_apply_markup_prices {lot_id} failed: {e}")
            rep["err"].append(lot_id)
    return rep

def _get_lot_price_currency(cardinal: "Cardinal", lot_id: int) -> tuple[Optional[float], str]:
    try:
        fields = cardinal.account.get_lot_fields(int(lot_id))
        if not fields:
            return None, "RUB"
        price = None
        for price_attr in ("price", "cost", "amount", "price_rub"):
            if hasattr(fields, price_attr):
                try:
                    price = float(getattr(fields, price_attr))
                    break
                except Exception:
                    pass
        currency = getattr(fields, "currency", None) or getattr(fields, "cur", None) or "RUB"
        return price, getattr(currency, "name", str(currency)) or "RUB"
    except Exception:
        return None, "RUB"

def _start_markup(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {"step": "markup_percent"}
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    m = bot.send_message(
        chat_id,
        "Введите наценку в процентах (например, <b>15</b> или <b>12.5</b>). Можно отрицательное значение для скидки.\n(или /cancel)",
        parse_mode="HTML",
        reply_markup=_kb_cancel_fsm()
    )
    st = _fsm.get(chat_id) or {}
    st["prompt_msg_id"] = getattr(m, "message_id", None)
    _fsm[chat_id] = st

def _cb_markup_change(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    try: cardinal.telegram.bot.answer_callback_query(call.id, "Измените процент сообщением ниже.")
    except Exception: pass
    st = _fsm.get(chat_id) or {}
    st["step"] = "markup_percent"
    _fsm[chat_id] = st
    m = cardinal.telegram.bot.send_message(chat_id, "Введите новый процент наценки (или /cancel):", reply_markup=_kb_cancel_fsm())
    st["prompt_msg_id"] = getattr(m, "message_id", None)
    _fsm[chat_id] = st

def _cb_markup_apply(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    try: cardinal.telegram.bot.answer_callback_query(call.id, "Применяю…")
    except Exception: pass
    st = _fsm.get(chat_id) or {}
    rows = st.get("markup_rows")
    percent = st.get("markup_percent")
    if not rows or percent is None:
        cardinal.telegram.bot.send_message(chat_id, "⚠️ Нет данных для применения. Запустите заново через «💹 Наценка лотов».")
        return
    rep = _apply_markup_prices(cardinal, rows)
    okn = len(rep["ok"]); ern = len(rep["err"]); total = len(rows)

    _set_cfg(chat_id, markup_percent=float(percent))
    msg = f"✅ Готово: обновлено {okn} из {total} лот(ов)."
    if ern:
        msg += f"\n⚠️ Ошибок: {ern}. См. логи."
    cardinal.telegram.bot.send_message(chat_id, msg)

    _fsm.pop(chat_id, None)
    try:
        _open_settings(cardinal.telegram.bot, call)
    except Exception:
        pass

_CARDINAL_REF: Optional["Cardinal"] = None

def init_cardinal(cardinal: Cardinal):
    global _CARDINAL_REF
    _CARDINAL_REF = cardinal

    tg = cardinal.telegram
    bot = tg.bot

    try:
        cardinal.add_telegram_commands(UUID, [
            ("fnp", "Открыть панель FNP Stars", True),
            ("fnphelp", "Инструкция FNP Stars", True),
            ("stars_thc", "Открыть панель FNP Stars (прямая команда)", True),
        ])
    except Exception:
        pass

    logger.info("🚀 Плагин по продаже звёзд запущен.")

    threading.Thread(
        target=_queue_watchdog,
        args=(cardinal,),
        daemon=True,
        name="FTS-QUEUE-WATCHDOG"
    ).start()

    def _send_home(m):
        return bot.send_message(
            m.chat.id,
            _about_text(),
            parse_mode="HTML",
            reply_markup=_home_kb(),
            disable_web_page_preview=True
        )

    tg.msg_handler(_send_home, commands=["fnp", "stars_thc"])

    fsm_steps = {
        "jwt_api_key", "jwt_phone", "jwt_wallet_ver", "jwt_seed",
        "set_min_balance",
        "star_add_qty", "star_add_lotid",
        "msg_edit_value",
        "markup_percent",
        "set_jwt",
        "star_price_value",
    }

    tg.msg_handler(
        lambda m: _handle_fsm(m, cardinal),
        func=lambda m: (
            m.chat.id in _fsm and _fsm[m.chat.id].get("step") in fsm_steps
        ),
        content_types=["text", "document"]
    )

    tg.cbq_handler(
        lambda c: _open_home(bot, c),
        func=lambda c: (
            c.data.startswith(f"{CBT.EDIT_PLUGIN}:{UUID}")
            or c.data.startswith(f"{CBT.PLUGIN_SETTINGS}:{UUID}")
            or c.data == f"{UUID}:0"
            or c.data == CBT_HOME
        )
    )

    tg.cbq_handler(lambda c: _open_settings(bot, c), func=lambda c: c.data == CBT_SETTINGS)
    tg.cbq_handler(lambda c: _open_token(bot, c), func=lambda c: c.data == CBT_TOKEN)
    tg.cbq_handler(lambda c: _open_stars(bot, c), func=lambda c: c.data == CBT_STARS)
    tg.cbq_handler(lambda c: _fsm_cancel(cardinal, c), func=lambda c: c.data == CBT_FSM_CANCEL)
    tg.cbq_handler(lambda c: _toggle_plugin(bot, c), func=lambda c: c.data == CBT_TOGGLE_PLUGIN)

    tg.cbq_handler(lambda c: _select_wallet_version(bot, c), func=lambda c: c.data.startswith(f"{UUID}:ver:"))

    tg.cbq_handler(lambda c: _toggle_lots(bot, c),   func=lambda c: c.data == CBT_TOGGLE_LOTS)
    tg.cbq_handler(lambda c: _toggle_refund(bot, c), func=lambda c: c.data == CBT_TOGGLE_REFUND)
    tg.cbq_handler(lambda c: _toggle_deact(bot, c),  func=lambda c: c.data == CBT_TOGGLE_DEACT)
    tg.cbq_handler(lambda c: _refresh(bot, c),       func=lambda c: c.data == CBT_REFRESH)
    tg.cbq_handler(lambda c: _ask_set_min_balance(bot, c), func=lambda c: c.data == CBT_SET_MIN_BAL)

    tg.msg_handler(lambda m: _cancel_cmd(cardinal, m.chat.id), commands=["cancel"])

    tg.cbq_handler(lambda c: _start_create_jwt(bot, c), func=lambda c: c.data == CBT_CREATE_JWT)
    tg.cbq_handler(lambda c: _jwt_confirmed(bot, c),   func=lambda c: c.data == CBT_JWT_CONFIRMED)
    tg.cbq_handler(lambda c: _jwt_resend(bot, c),      func=lambda c: c.data == CBT_JWT_RESEND)
    tg.cbq_handler(lambda c: _del_jwt(bot, c),         func=lambda c: c.data == CBT_DEL_JWT)
    tg.cbq_handler(lambda c: _ask_set_jwt(bot, c),     func=lambda c: c.data == CBT_SET_JWT)

    tg.cbq_handler(lambda c: _star_add(bot, c),         func=lambda c: c.data == CBT_STAR_ADD)
    tg.cbq_handler(lambda c: _star_act_all(bot, c),     func=lambda c: c.data == CBT_STAR_ACT_ALL)
    tg.cbq_handler(lambda c: _star_deact_all(bot, c),   func=lambda c: c.data == CBT_STAR_DEACT_ALL)
    tg.cbq_handler(lambda c: _star_toggle(bot, c),      func=lambda c: c.data.startswith(CBT_STAR_TOGGLE_P))
    tg.cbq_handler(lambda c: _star_delete(bot, c),      func=lambda c: c.data.startswith(CBT_STAR_DEL_P))

    tg.cbq_handler(lambda c: _cb_confirm_send(cardinal, c),    func=lambda c: c.data == CBT_CONFIRM_SEND)
    tg.cbq_handler(lambda c: _cb_change_username(cardinal, c), func=lambda c: c.data == CBT_CHANGE_USERNAME)
    tg.cbq_handler(lambda c: _cb_cancel_flow(cardinal, c),     func=lambda c: c.data == CBT_CANCEL_FLOW)
    tg.cbq_handler(lambda c: _go_main_menu(cardinal, c),       func=lambda c: c.data == CBT_BACK_PLUGINS)

    tg.cbq_handler(lambda c: _open_messages(bot, c),           func=lambda c: c.data == CBT_MESSAGES)
    tg.cbq_handler(lambda c: _msg_edit_start(bot, c),          func=lambda c: c.data.startswith(CBT_MSG_EDIT_P))
    tg.cbq_handler(lambda c: _msg_reset(bot, c),               func=lambda c: c.data.startswith(CBT_MSG_RESET_P))
    tg.cbq_handler(lambda c: _toggle_manual_refund(bot, c),    func=lambda c: c.data == CBT_TOGGLE_MANUAL_REFUND)
    tg.cbq_handler(lambda c: _toggle_back_priority(bot, c),    func=lambda c: c.data == CBT_TOGGLE_BACK_PRIORITY)
    tg.cbq_handler(lambda c: _toggle_preorder_username(bot, c),func=lambda c: c.data == CBT_TOGGLE_PREORDER)

    tg.cbq_handler(lambda c: _start_markup(bot, c),            func=lambda c: c.data == CBT_MARKUP)
    tg.cbq_handler(lambda c: _cb_markup_apply(cardinal, c),    func=lambda c: c.data == CBT_MARKUP_APPLY)
    tg.cbq_handler(lambda c: _cb_markup_change(cardinal, c),   func=lambda c: c.data == CBT_MARKUP_CHANGE)
    tg.cbq_handler(lambda c: _open_mini_settings(bot, c),      func=lambda c: c.data == CBT_MINI_SETTINGS)
    tg.cbq_handler(lambda c: _star_price_start(bot, c),        func=lambda c: c.data.startswith(CBT_STAR_PRICE_P))
    tg.cbq_handler(lambda c: _cb_markup_reset(cardinal, c),    func=lambda c: c.data == CBT_MARKUP_RESET)

    tg.cbq_handler(lambda c: _send_logs(bot, c),               func=lambda c: c.data == CBT_LOGS)
    tg.cbq_handler(lambda c: _open_stats(bot, c),              func=lambda c: c.data == CBT_STATS)
    tg.cbq_handler(
        lambda c: _open_stats(bot, c, c.data.split(":")[-1]),
        func=lambda c: c.data.startswith(CBT_STATS_RANGE_P)
    )

    tg.cbq_handler(lambda c: _open_delete_confirm(bot, c),     func=lambda c: c.data == CBT_DELETE_ASK)
    tg.cbq_handler(lambda c: _cb_delete_yes(cardinal, c),      func=lambda c: c.data == CBT_DELETE_YES)
    tg.cbq_handler(lambda c: _cb_delete_no(bot, c),            func=lambda c: c.data == CBT_DELETE_NO)

    tg.cbq_handler(lambda c: _toggle_liteserver_retry(bot, c), func=lambda c: c.data == CBT_TOGGLE_LITESERVER_RETRY)
    tg.cbq_handler(lambda c: _toggle_username_check(bot, c),   func=lambda c: c.data == CBT_TOGGLE_USERNAME_CHECK)
    tg.cbq_handler(lambda c: _toggle_autosend_plus(bot, c),    func=lambda c: c.data == CBT_TOGGLE_AUTOSEND_PLUS)
    tg.cbq_handler(lambda c: _toggle_queue_mode(bot, c),       func=lambda c: c.data == CBT_TOGGLE_QUEUE_MODE)


def _open_home(bot, call):
    _safe_edit(bot, call.message.chat.id, call.message.id, _about_text(), _home_kb())
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _go_main_menu(cardinal: "Cardinal", call):
    bot = cardinal.telegram.bot
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    for attr in ("open_main_menu", "show_main_menu", "menu", "open_menu", "start_menu", "home"):
        fn = getattr(cardinal.telegram, attr, None) or getattr(cardinal, attr, None)
        if callable(fn):
            try:
                fn(call.message.chat.id)
                return
            except Exception as e:
                logger.warning(f"open main menu via {attr} failed: {e}")
    _open_home(bot, call)

def _open_settings(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _settings_text(chat_id), _settings_kb(chat_id))
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _delete_confirm_text() -> str:
    return (
        "⚠️ <b>Удаление плагина</b>\n\n"
        "Вы точно хотите удалить <b>FTS-Plugin</b>?\n\n"
        "Будут удалены:\n"
        "• файлы плагина\n"
        "• настройки и логи\n\n"
        "<b>Действие необратимо.</b>\n"
        "После удаления выполните перезапуск: напишите команду <code>/restart</code>."
    )

def _delete_confirm_kb() -> InlineKeyboardMarkup:
    kb = K()
    kb.row(
        B("✅ Да, удалить", callback_data=CBT_DELETE_YES),
        B("❌ Нет", callback_data=CBT_DELETE_NO),
    )
    return kb

def _open_delete_confirm(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _delete_confirm_text(), _delete_confirm_kb())
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

def _self_delete_from_disk() -> tuple[bool, list[str]]:
    errors: list[str] = []

    try:
        if _CARDINAL_REF is not None:
            _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, False)
    except Exception as e:
        errors.append(f"Не удалось выключить лоты: {e}")

    try:
        shutil.rmtree(PLUGIN_FOLDER, ignore_errors=True)
    except Exception as e:
        errors.append(f"Не удалось удалить папку настроек {PLUGIN_FOLDER}: {e}")

    plugin_file = os.path.abspath(__file__)
    try:
        try:
            pycache_dir = os.path.join(os.path.dirname(plugin_file), "__pycache__")
            if os.path.isdir(pycache_dir):
                base = os.path.splitext(os.path.basename(plugin_file))[0]
                for fn in os.listdir(pycache_dir):
                    if fn.startswith(base) and fn.endswith(".pyc"):
                        try:
                            os.remove(os.path.join(pycache_dir, fn))
                        except Exception:
                            pass
        except Exception:
            pass

        os.remove(plugin_file)
    except Exception as e:
        errors.append(f"Не удалось удалить файл плагина {plugin_file}: {e}")

    return (len(errors) == 0), errors

def _cb_delete_yes(cardinal: "Cardinal", call):
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id

    try:
        bot.answer_callback_query(call.id, "Удаляю…")
    except Exception:
        pass

    ok, errors = _self_delete_from_disk()

    kb = K()
    kb.add(B("🔙 К списку плагинов", callback_data=CBT_PLUGINS_LIST_OPEN))

    if ok:
        text = (
            "✅ <b>Плагин удалён.</b>\n\n"
            "🔁 Чтобы применилось, напишите команду: <code>/restart</code>"
        )
    else:
        text = (
            "⚠️ <b>Удаление выполнено частично.</b>\n\n"
            "Что пошло не так:\n"
            + "\n".join([f"• {e}" for e in errors[:10]])
            + "\n\n"
            "🔁 После завершения удаления всё равно выполните перезапуск командой: <code>/restart</code>"
        )

    _safe_edit(bot, chat_id, call.message.id, text, kb)

def _cb_delete_no(bot, call):
    _open_home(bot, call)

def _open_token(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _token_text(chat_id), _token_kb())
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _open_stars(bot, call):
    chat_id = call.message.chat.id
    _safe_edit(bot, chat_id, call.message.id, _stars_text(chat_id), _stars_kb(chat_id))
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _send_logs(bot, call):
    chat_id = call.message.chat.id
    path = os.getenv("FTS_RAW_LOG_FILE") or os.path.join(PLUGIN_FOLDER, "lot.txt")
    try:

        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as _:
                pass

        with open(path, "rb") as f:
            bot.send_document(chat_id, ("lot.txt", f.read()), caption="Логи FTS-Plugin")

        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
    except Exception as e:
        try:
            bot.answer_callback_query(call.id, f"Не удалось отправить лог: {e}", show_alert=True)
        except Exception:
            pass

CBT_VER_PREFIX    = f"{UUID}:ver:"

def _stats_kb(selected: str) -> InlineKeyboardMarkup:
    ranges = [("24ч","24h"), ("7д","7d"), ("30д","30d"), ("Всё","all")]
    kb = K()
    row = []
    for label, key in ranges:
        mark = "• " if key == selected else ""
        row.append(B(f"{mark}{label}", callback_data=f"{CBT_STATS_RANGE_P}{key}"))
    kb.row(*row)
    kb.add(B("◀️ Назад", callback_data=CBT_SETTINGS))
    return kb

def _log_path() -> str:
    return os.getenv("FTS_RAW_LOG_FILE") or LOG_FILE_LOCAL

def _read_log_tail(max_bytes: int = 1_500_000) -> list[str]:
    path = _log_path()
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            sz = f.tell()
            f.seek(max(0, sz - max_bytes), os.SEEK_SET)
            return f.read().decode("utf-8", errors="ignore").splitlines()
    except Exception as e:
        logger.debug(f"stats read log failed: {e}")
        return []

def _parse_line_ts(line: str) -> Optional[float]:
    try:
        s = line[:19]
        tt = time.strptime(s, "%Y-%m-%d %H:%M:%S")
        return time.mktime(tt)
    except Exception:
        return None

def _stats_collect(lines: list[str], since_ts: Optional[float]) -> dict:
    stats = {
        "ok": 0, "fail": 0, "qty_ok": 0,
        "per_user": defaultdict(lambda: {"qty": 0, "cnt": 0}),
        "per_day": defaultdict(int),
        "refunds_ok": 0, "refunds_fail": 0,
        "auto_deact": 0, "preorder": 0, "queue_merge": 0, "ignore": 0,
        "last_ok": None, "last_fail": None, "min_balance_set": []
    }

    for ln in lines:
        ts = _parse_line_ts(ln)
        if since_ts and ts and ts < since_ts:
            continue
        low = ln.lower()

        if "[ignore]" in low:            stats["ignore"] += 1
        if "[queue] merged" in low:       stats["queue_merge"] += 1
        if "[autodeact]" in low:          stats["auto_deact"] += 1
        if "[preorder]" in low:           stats["preorder"] += 1
        if "min balance set to" in low:
            m = _re.search(r"min balance set to\s*([0-9.]+)\s*ton", ln, _re.I)
            if m:
                try: stats["min_balance_set"].append(float(m.group(1)))
                except Exception: pass

        m = _re.search(r"SEND OK\s+(\d+)\s*⭐.*?@([A-Za-z0-9_]{4,32})", ln)
        if m:
            q = int(m.group(1)); u = m.group(2).lower()
            stats["ok"] += 1
            stats["qty_ok"] += q
            stats["per_user"][u]["qty"] += q
            stats["per_user"][u]["cnt"] += 1
            if ts: stats["last_ok"] = ts
            day = time.strftime("%Y-%m-%d", time.localtime(ts or time.time()))
            stats["per_day"][day] += q
            continue

        m = _re.search(r"SEND FAIL\s+(\d+)\s*⭐.*?@([A-Za-z0-9_]{4,32})", ln)
        if m:
            stats["fail"] += 1
            if ts: stats["last_fail"] = ts
            continue

        m = _re.search(r"SEND result:\s*ok=(True|False)", ln)
        if m:
            if m.group(1) == "True":
                stats["ok"] += 1
                if ts: stats["last_ok"] = ts
            else:
                stats["fail"] += 1
                if ts: stats["last_fail"] = ts

        m = _re.search(r"REFUND\s+#?([A-Za-z0-9\-]+)\s*->\s*(OK|FAIL)", ln)
        if m:
            if m.group(2) == "OK": stats["refunds_ok"] += 1
            else:                   stats["refunds_fail"] += 1

    stats["per_user"] = dict(stats["per_user"])
    stats["per_day"]  = dict(stats["per_day"])
    return stats

def _fmt_human_ts(ts: Optional[float]) -> str:
    if not ts: return "—"
    return time.strftime("%d.%m.%Y %H:%M:%S", time.localtime(ts))

def _stats_text(chat_id: Any, range_key: str) -> str:
    now = time.time()
    ranges = {
        "24h": (now - 24*3600, "за 24 часа"),
        "7d":  (now - 7*24*3600, "за 7 дней"),
        "30d": (now - 30*24*3600, "за 30 дней"),
        "all": (None, "за всё время"),
    }
    since_ts, label = ranges.get(range_key, ranges["7d"])
    lines = _read_log_tail()
    s = _stats_collect(lines, since_ts)

    total = s["ok"] + s["fail"]
    conv  = (s["ok"] / total * 100.0) if total else 0.0
    avg   = (s["qty_ok"] / s["ok"]) if s["ok"] else 0.0

    top = sorted(s["per_user"].items(), key=lambda kv: kv[1]["qty"], reverse=True)[:5]
    top_lines = []
    for i, (u, v) in enumerate(top, 1):
        top_lines.append(f"{i}) @{u} — {int(v['qty'])}⭐ ({v['cnt']} заказ.)")

    days = sorted(s["per_day"].items())[-10:]
    day_lines = [f"{d}: {int(q)}⭐" for d, q in days] if days else ["—"]

    cfg = _get_cfg(chat_id)
    cur_thr = cfg.get("min_balance_ton", FNP_MIN_BALANCE_TON)
    bal_txt = cfg.get("balance_ton")
    bal_txt = (f"{bal_txt} TON" if bal_txt is not None else "—")

    return (
        f"<b>📊 Статистика ({label})</b>\n\n"
        f"<b>Заказы</b>\n"
        f"• Успешно: <b>{s['ok']}</b> (всего {int(s['qty_ok'])}⭐)\n"
        f"• Средний успешный заказ: <b>{avg:.0f}⭐</b>\n"
        f"• Последний успех: <code>{_fmt_human_ts(s['last_ok'])}</code>\n"
        f"• Последняя ошибка: <code>{_fmt_human_ts(s['last_fail'])}</code>\n\n"

        f"<b>Возвраты</b>\n"
        f"• Оформлено: <b>{s['refunds_ok']}</b>\n"

        f"<b>Топ покупателей</b>\n" +
        (("\n".join(top_lines)) if top_lines else "—") + "\n\n" +
        f"<b>Активность по дням</b>\n" +
        ("\n".join(day_lines))
    )

def _open_stats(bot, call, range_key: Optional[str] = None):
    chat_id = call.message.chat.id
    rk = range_key or "7d"
    text = _stats_text(chat_id, rk)
    _safe_edit(bot, chat_id, call.message.id, text, _stats_kb(rk))
    try: bot.answer_callback_query(call.id)
    except Exception: pass

def _star_add(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {"step": "star_add_qty"}
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    bot.send_message(chat_id, "Введите <b>количество звёзд</b> (целое число от 50 до 1_000_000):", parse_mode="HTML", reply_markup=_kb_cancel_fsm())

def _star_act_all(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    items = cfg.get("star_lots") or []
    if not items:
        bot.answer_callback_query(call.id, "Список пуст.", show_alert=True)
        return
    if _CARDINAL_REF is not None:
        _apply_star_lots_state(_CARDINAL_REF, items, True)
    for it in items: it["active"] = True
    _set_cfg(chat_id, star_lots=items)
    _open_stars(bot, call)

def _star_deact_all(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    items = cfg.get("star_lots") or []
    if not items:
        bot.answer_callback_query(call.id, "Список пуст.", show_alert=True)
        return
    if _CARDINAL_REF is not None:
        _apply_star_lots_state(_CARDINAL_REF, items, False)
    for it in items: it["active"] = False
    _set_cfg(chat_id, star_lots=items)
    _open_stars(bot, call)

def _star_toggle(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    lot_id = int(call.data.split(":")[-1])
    items = cfg.get("star_lots") or []
    found = None
    for it in items:
        if int(it.get("lot_id", 0)) == lot_id:
            found = it; break
    if not found:
        bot.answer_callback_query(call.id, "Лот не найден.", show_alert=True)
        return
    enabled = not bool(found.get("active"))
    if _CARDINAL_REF is not None:
        if enabled: _activate_lot(_CARDINAL_REF, lot_id)
        else:       _deactivate_lot(_CARDINAL_REF, lot_id)
    found["active"] = enabled
    _set_cfg(chat_id, star_lots=items)
    _open_stars(bot, call)

def _star_delete(bot, call):
    chat_id = call.message.chat.id
    lot_id = int(call.data.split(":")[-1])
    cfg = _get_cfg(chat_id)
    items = [x for x in (cfg.get("star_lots") or []) if int(x.get("lot_id", 0)) != lot_id]
    _set_cfg(chat_id, star_lots=items)
    _open_stars(bot, call)

def _toggle_liteserver_retry(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT))
    _set_cfg(chat_id, retry_liteserver=new_state)
    try:
        bot.answer_callback_query(call.id, "LiteServer-ретрай включён." if new_state else "LiteServer-ретрай выключен.")
    except Exception:
        pass
    _open_mini_settings(bot, call)

def _toggle_autosend_plus(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("auto_send_without_plus", False))
    _set_cfg(chat_id, auto_send_without_plus=new_state)
    try:
        bot.answer_callback_query(
            call.id,
            "Автоотправка без '+' включена." if new_state else "Теперь требуется подтверждение '+'."
        )
    except Exception:
        pass
    _open_mini_settings(bot, call)

def _toggle_queue_mode(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cur = int(cfg.get("queue_mode") or 1)
    nxt = 1 if cur >= 3 else cur + 1

    _set_cfg(chat_id, queue_mode=nxt)
    if not cfg.get("queue_timeout_sec"):
        _set_cfg(chat_id, queue_timeout_sec=int(os.getenv("FTS_QUEUE_TIMEOUT_SEC", "300")))

    try:
        bot.answer_callback_query(call.id, f"Очередь: {_queue_mode_label(nxt, _queue_timeout_sec(chat_id))}")
    except Exception:
        pass

    _open_mini_settings(bot, call)

def _toggle_username_check(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("skip_username_check", False))
    _set_cfg(chat_id, skip_username_check=new_state)
    try:
        bot.answer_callback_query(
            call.id,
            "Проверка @username при вводе отключена (проверим при отправке)." if new_state
            else "Проверка @username при вводе включена."
        )
    except Exception:
        pass
    _open_mini_settings(bot, call)

def _toggle_lots(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)

    if not cfg.get("plugin_enabled", True):
        bot.answer_callback_query(call.id, "Плагин выключен. Сначала включите его в настройках.", show_alert=True)
        return

    current = bool(cfg.get("lots_active", False))
    desired = not current

    if _CARDINAL_REF is not None:
        star_lots = cfg.get("star_lots") or []
        if star_lots:
            _apply_star_lots_state(_CARDINAL_REF, star_lots, desired)
            for it in star_lots:
                it["active"] = bool(desired)
            _set_cfg(chat_id, star_lots=star_lots)
        else:
            _apply_category_state(_CARDINAL_REF, FNP_STARS_CATEGORY_ID, desired)

    _set_cfg(chat_id, lots_active=desired)
    bot.answer_callback_query(call.id, ("Лоты включены." if desired else "Лоты выключены."), show_alert=False)
    _open_settings(bot, call)

def _toggle_refund(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cfg = _set_cfg(chat_id, auto_refund = not bool(cfg.get("auto_refund", False)))
    bot.answer_callback_query(call.id, ("Автовозврат включён." if cfg["auto_refund"] else "Автовозврат выключен."), show_alert=False)
    _open_settings(bot, call)

def _toggle_deact(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    cfg = _set_cfg(chat_id, auto_deactivate = not bool(cfg.get("auto_deactivate", True)))
    bot.answer_callback_query(call.id, ("Автодеактивация включена." if cfg["auto_deactivate"] else "Автодеактивация выключена."), show_alert=False)
    _open_settings(bot, call)

def _toggle_manual_refund(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("manual_refund_enabled", False))
    _set_cfg(chat_id, manual_refund_enabled=new_state)
    try:
        bot.answer_callback_query(call.id, "Команда !бэк включена." if new_state else "Команда !бэк выключена.")
    except Exception:
        pass
    _open_settings(bot, call)

def _toggle_back_priority(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    new_state = not bool(cfg.get("manual_refund_priority", True))
    _set_cfg(chat_id, manual_refund_priority=new_state)
    try:
        txt = "Приоритет !бэк: ВЫШЕ автовозврата" if new_state else "Приоритет !бэк: НИЖЕ автовозврата"
        bot.answer_callback_query(call.id, txt)
    except Exception:
        pass
    _open_settings(bot, call)

def _refresh(bot, call):
    chat_id = call.message.chat.id
    cfg = _get_cfg(chat_id)
    if cfg.get("fragment_jwt"):
        ver, bal, raw = _check_fragment_wallet(cfg["fragment_jwt"])
        if ver is not None or bal is not None or raw is not None:
            _set_cfg(chat_id, wallet_version=ver, balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None), last_wallet_raw=raw)
    _open_settings(bot, call)
    try: bot.answer_callback_query(call.id, "Обновлено.", show_alert=False)
    except Exception: pass

def _ask_set_min_balance(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {"step": "set_min_balance"}
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    cur = _get_cfg(chat_id).get("min_balance_ton", FNP_MIN_BALANCE_TON)
    bot.send_message(chat_id, f"Введите новый порог баланса в TON (сейчас {cur}). Можно с точкой или запятой. Пример: 5.5\n(или /cancel)", reply_markup=_kb_cancel_fsm())

def _cancel_cmd(cardinal: "Cardinal", chat_id: Any):
    _fsm.pop(chat_id, None)
    if _has_queue(chat_id):
        _pop_current(chat_id, keep_prompted=False)
    _safe_send(cardinal, chat_id, "❌ Отменено. Текущий шаг сброшен.")
    if _has_queue(chat_id):
        nxt = _current(chat_id)
        qty = int(nxt.get("qty", 50))
        _safe_send(cardinal, chat_id,
                   f"Следующий заказ в очереди: {qty}⭐.\n"
                   "Пришлите тег в формате @username одной строкой.")

def _select_wallet_version(bot, call):
    chat_id = call.message.chat.id
    data = call.data.split(":")[-1] if call and call.data else "W5"
    ver = _normalize_wallet_version(data)
    st = _fsm.get(chat_id) or {}
    if st.get("step") not in ("jwt_phone", "jwt_wallet_ver"):
        st["step"] = "jwt_wallet_ver"
    st["wallet_version"] = ver
    st["step"] = "jwt_seed"
    _fsm[chat_id] = st
    try: bot.answer_callback_query(call.id, f"Версия выбрана: {ver}")
    except Exception: pass
    bot.send_message(chat_id, "Введите 24 слова мнемофразы одной строкой (или /cancel):")

def _star_price_start(bot, call):
    chat_id = call.message.chat.id
    try:
        lot_id = int(call.data.split(":")[-1])
    except Exception:
        try: bot.answer_callback_query(call.id, "Некорректный LOT_ID", show_alert=True)
        except Exception: pass
        return

    if _CARDINAL_REF is not None and not _is_stars_lot(_CARDINAL_REF, lot_id):
        try: bot.answer_callback_query(call.id, f"LOT {lot_id} не из категории {FNP_STARS_CATEGORY_ID}", show_alert=True)
        except Exception: pass
        return

    price, cur = _get_lot_price_currency(_CARDINAL_REF, lot_id) if _CARDINAL_REF else (None, "RUB")
    price_txt = f"{price:.2f}" if isinstance(price, (int, float)) else "—"

    _fsm[chat_id] = {"step": "star_price_value", "lot_id": lot_id, "currency": cur}
    try: bot.answer_callback_query(call.id)
    except Exception: pass

    m = bot.send_message(
        chat_id,
        f"LOT {lot_id}\nТекущая цена: <b>{price_txt} {cur}</b>\n\n"
        "Введите <b>новую цену</b> (число). Пример: 149 или 149.99\n(или /cancel)",
        parse_mode="HTML",
        reply_markup=_kb_cancel_fsm()
    )
    st = _fsm.get(chat_id) or {}
    st["prompt_msg_id"] = getattr(m, "message_id", None)
    _fsm[chat_id] = st

CLEAN_USER_MSGS = bool(int(os.getenv("FTS_Plugin_CLEAN_USER_MSGS", "0")))

def _ask_set_jwt(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {"step": "set_jwt"}
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    bot.send_message(
        chat_id,
        "Пришлите файлом (.txt / .json). "
        "В JSON токен может лежать в ключах token/jwt/access/authorization. (или /cancel)",
        reply_markup=_kb_cancel_fsm()
    )

def _del_jwt(bot, call):
    chat_id = call.message.chat.id
    _set_cfg(chat_id, fragment_jwt=None, wallet_version=None, balance_ton=None, last_wallet_raw=None)
    try: bot.answer_callback_query(call.id, "Токен удалён.")
    except Exception: pass
    _open_token(bot, call)

def _start_create_jwt(bot, call):
    chat_id = call.message.chat.id
    _fsm[chat_id] = {"step": "jwt_api_key"}
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    bot.send_message(chat_id, "Введите API-ключ Fragment (или /cancel). Его можно взять в dashboard: https://fragment-api.com/dashboard", reply_markup=_kb_cancel_fsm())

def _clean_jwt_text(s: str) -> str:
    try:
        s = (s or "").strip().strip('"').strip("'")
        s = _re.sub(r'^(?:JWT|Bearer)\s+', '', s, flags=_re.I)
        s = _re.sub(r'\s+', '', s)
        return s
    except Exception:
        return s or ""

def _is_jwt_like(s: str) -> bool:
    if not s or s.count(".") < 2:
        return False
    return bool(_re.fullmatch(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+", s))

def _find_jwt_in_json(obj: Any) -> Optional[str]:
    CAND_KEYS = {"token","jwt","access","authorization","Authorization","auth","detail"}
    try:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in CAND_KEYS and isinstance(v, str):
                    cand = _clean_jwt_text(v)
                    if _is_jwt_like(cand) or len(cand) > 16:
                        return cand
            for subk in ("data","result","payload"):
                if subk in obj:
                    v = _find_jwt_in_json(obj[subk])
                    if v: return v
            for v in obj.values():
                vv = _find_jwt_in_json(v)
                if vv: return vv
        elif isinstance(obj, list):
            for v in obj:
                vv = _find_jwt_in_json(v)
                if vv: return vv
    except Exception:
        pass
    return None

def _handle_fsm(message: Message, cardinal: Cardinal):
    chat_id = message.chat.id
    text = (message.text or "").strip()
    state = _fsm.get(chat_id) or {}

    if state.get("step") == "jwt_api_key":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        state["api_key"] = text
        state["step"] = "jwt_phone"
        _fsm[chat_id] = state
        cardinal.telegram.bot.send_message(
            chat_id,
            "Укажите телефон (без «+», только цифры), или /cancel:",
            reply_markup=_kb_cancel_fsm()
        )
        return
    
    if state.get("step") == "msg_edit_value":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            try:
                pmid = (_fsm.get(chat_id) or {}).get("prompt_msg_id")
                if pmid:
                    cardinal.telegram.bot.delete_message(chat_id, pmid)
            except Exception:
                pass
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, "❌ Отменено.")
            return

        key = state.get("msg_key")
        cfg = _get_cfg(chat_id)
        tpls = cfg.get("templates") or _default_templates()
        tpls[key] = text
        _set_cfg(chat_id, templates=tpls)

        try:
            pmid = state.get("prompt_msg_id")
            if pmid:
                cardinal.telegram.bot.delete_message(chat_id, pmid)
            cardinal.telegram.bot.delete_message(chat_id, message.message_id)
        except Exception:
            pass

        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, "✅ Шаблон обновлён.")

        try:
            _open_messages(
                cardinal.telegram.bot,
                type("obj", (), {"message": type("m", (), {"chat": type("c", (), {"id": chat_id})(), "id": message.message_id})(), "id": ""})
            )
        except Exception:
            pass
        return

    if state.get("step") == "jwt_phone":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        state["phone"] = text; state["step"] = "jwt_wallet_ver"; _fsm[chat_id] = state
        kb = K(); kb.row(B("W5 (V5R1)", callback_data=f"{UUID}:ver:W5"), B("V4R2", callback_data=f"{UUID}:ver:V4R2"))
        cardinal.telegram.bot.send_message(chat_id, "Укажите версию кошелька (W5 или V4R2). Можно нажать кнопку или ввести текстом. По умолчанию W5.", reply_markup=kb); return

    if state.get("step") == "jwt_wallet_ver":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        ver = _normalize_wallet_version(text); state["wallet_version"] = ver; state["step"] = "jwt_seed"; _fsm[chat_id] = state
        cardinal.telegram.bot.send_message(chat_id, "Вставьте 24 слова мнемофразы одной строкой (или /cancel):", reply_markup=_kb_cancel_fsm())
        return

    if state.get("step") == "jwt_seed":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return

        words = [w.strip() for w in text.replace("\n", " ").split(" ") if w.strip()]
        if len(words) != 24:
            cardinal.telegram.bot.send_message(chat_id, f"⚠️ Должно быть ровно 24 слова (сейчас {len(words)}). Повторите ввод или /cancel."); return

        state["mnemonic"] = words
        _fsm[chat_id] = state

        api_key = state.get("api_key")
        phone = state.get("phone")
        wallet_ver = state.get("wallet_version") or "W5"

        jwt, raw, sc = _authenticate_fragment(api_key=api_key, phone_number=phone, version=wallet_ver, mnemonics=words)
        if jwt:
                _set_cfg(chat_id, fragment_jwt=jwt)
                cardinal.telegram.bot.send_message(chat_id, "✅ Успешно: токен создан и привязан.")
                ver, bal, resp = _check_fragment_wallet(jwt)
                if ver is not None or bal is not None or resp is not None:
                    _set_cfg(chat_id, wallet_version=ver, balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None), last_wallet_raw=resp)
                _fsm.pop(chat_id, None)
                return
        
        human = _human_auth_error(raw, sc)
        cardinal.telegram.bot.send_message(chat_id, f"❌ Не удалось создать токен. {human}")

        is_tma, wait_sec = _is_too_many_attempts(raw)
        if sc == 400 and is_tma:
                jwt, raw, sc = _authenticate_fragment(api_key=api_key, phone_number=phone, version=wallet_ver, mnemonics=words)
                if jwt:
                    _set_cfg(chat_id, fragment_jwt=jwt)
                    cardinal.telegram.bot.send_message(chat_id, "✅ Успешно: токен создан и привязан.")
                    ver, bal, resp = _check_fragment_wallet(jwt)
                    if ver is not None or bal is not None or resp is not None:
                        _set_cfg(chat_id, wallet_version=ver, balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None), last_wallet_raw=resp)
                    _fsm.pop(chat_id, None)
                    return
                human2 = _human_auth_error(raw, sc)
                cardinal.telegram.bot.send_message(chat_id, f"❌ Повтор не удался. {human2}")
                return
        else:
            human = _human_auth_error(raw, sc)
            if sc == 400:
                cardinal.telegram.bot.send_message(chat_id, f"❌ Не удалось выдать токен. {human}")
            else:
                cardinal.telegram.bot.send_message(chat_id, f"❌ Не удалось выдать токен (HTTP {sc}). {human}")

            try:
                text = json.dumps(raw, ensure_ascii=False, indent=2)[:1900]
            except Exception:
                text = str(raw)[:1900]
            cardinal.telegram.bot.send_message(chat_id, f"Ответ сервера:\n<code>{text}</code>", parse_mode="HTML")

    if state.get("step") == "markup_percent":
        pmid = (_fsm.get(chat_id) or {}).get("prompt_msg_id")
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _safe_delete(cardinal.telegram.bot, chat_id, pmid)
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
            _fsm.pop(chat_id, None)
            m_cancel = cardinal.telegram.bot.send_message(chat_id, "❌ Отменено.")
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(m_cancel, "message_id", None))
            return

        t = text.replace(",", ".").strip()
        try:
            percent = float(t)
            if not (-90.0 <= percent <= 500.0):
                raise ValueError
        except Exception:
            _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите число (проценты), например 10 или 12.5. Диапазон: от -90 до 500.")
            return

        _safe_delete(cardinal.telegram.bot, chat_id, pmid)
        _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))

        cfg = _get_cfg(chat_id)
        if _CARDINAL_REF is None:
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Внутренняя ошибка: нет ссылки на Cardinal.")
            return

        rows = _collect_markup_targets(_CARDINAL_REF, cfg, percent)
        preview = _markup_preview_text(percent, rows)

        st = _fsm.get(chat_id) or {}
        st["markup_percent"] = percent
        st["markup_rows"] = rows
        st["step"] = "markup_preview"
        _fsm[chat_id] = st

        cardinal.telegram.bot.send_message(chat_id, preview, parse_mode="HTML", reply_markup=_kb_markup_preview())
        return
    
    if state.get("step") == "star_price_value":
            pmid = (_fsm.get(chat_id) or {}).get("prompt_msg_id")
            if text.lower() in ("/cancel", "cancel", "отмена"):
                _safe_delete(cardinal.telegram.bot, chat_id, pmid)
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
                _fsm.pop(chat_id, None)
                m_cancel = cardinal.telegram.bot.send_message(chat_id, "❌ Отменено.")
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(m_cancel, "message_id", None))
                return

            lot_id = int(state.get("lot_id"))
            cur = state.get("currency") or "RUB"

            t = text.replace(",", ".").strip()
            try:
                new_price = float(t)
                if new_price <= 0:
                    raise ValueError
            except Exception:
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
                cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите положительное число (цена), например 149 или 149.99.")
                return

            old_price, cur_detected = _get_lot_price_currency(cardinal, lot_id)
            cur = cur_detected or cur

            try:
                fields = cardinal.account.get_lot_fields(lot_id)
                if not fields:
                    raise RuntimeError("Лот недоступен.")
                if not _is_stars_lot(cardinal, lot_id):
                    raise RuntimeError(f"Лот не из категории {FNP_STARS_CATEGORY_ID}.")

                set_ok = False
                for price_attr in ("price", "cost", "amount", "price_rub"):
                    if hasattr(fields, price_attr):
                        setattr(fields, price_attr, float(new_price))
                        set_ok = True
                        break
                if not set_ok:
                    raise RuntimeError("Не удалось изменить цену в полях лота.")
                cardinal.account.save_lot(fields)

                _safe_delete(cardinal.telegram.bot, chat_id, pmid)
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
                cardinal.telegram.bot.send_message(
                    chat_id,
                    f"✅ Цена обновлена для LOT {lot_id}: "
                    f"{_format_currency(old_price, cur) if old_price is not None else ''} → "
                    f"<b>{_format_currency(new_price, cur)}</b>",
                    parse_mode="HTML"
                )
            except Exception as e:
                _safe_delete(cardinal.telegram.bot, chat_id, pmid)
                _safe_delete(cardinal.telegram.bot, chat_id, getattr(message, "message_id", None))
                cardinal.telegram.bot.send_message(chat_id, f"❌ Не удалось сохранить цену: {e}")
            finally:
                _fsm.pop(chat_id, None)

                try:
                    _open_stars(cardinal.telegram.bot, type("obj", (), {"message": type("m", (), {"chat": type("c", (), {"id": chat_id})(), "id": message.message_id})(), "id": ""}))
                except Exception:
                    pass
            return

    if state.get("step") == "star_add_qty":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        try:
            qty = int(text.strip())
            if qty < 50 or qty > 1_000_000: raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите целое число от 50 до 1 000 000, либо /cancel."); return
        state["new_qty"] = qty; state["step"] = "star_add_lotid"; _fsm[chat_id] = state
        cardinal.telegram.bot.send_message(chat_id, "Теперь введите <b>LOT_ID</b> (целое положительное), или /cancel:", parse_mode="HTML", reply_markup=_kb_cancel_fsm())
        return

    if state.get("step") == "star_add_lotid":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        try:
            lot_id = int(text.strip())
            if lot_id <= 0: raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите положительное целое (LOT_ID), либо /cancel."); return

        if _CARDINAL_REF is not None and not _is_stars_lot(_CARDINAL_REF, lot_id):
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(
                chat_id,
                f"❌ LOT {lot_id} не относится к категории {FNP_STARS_CATEGORY_ID}. Добавление отклонено."
            )
            return

        qty = int(state.get("new_qty"))
        cfg = _get_cfg(chat_id); items = cfg.get("star_lots") or []
        updated = False
        for it in items:
            if int(it.get("lot_id", 0)) == lot_id:
                it["qty"] = qty; it["active"] = True; updated = True; break
        if not updated:
            items.append({"qty": qty, "lot_id": lot_id, "active": True})
        _set_cfg(chat_id, star_lots=items); _fsm.pop(chat_id, None)
        if _CARDINAL_REF is not None: _activate_lot(_CARDINAL_REF, lot_id)
        cardinal.telegram.bot.send_message(chat_id, f"✅ Добавлено: {qty} ⭐ (LOT {lot_id}). Управляйте в «⭐ Звёзды»."); 
        return

    if state.get("step") == "set_min_balance":
        if text.lower() in ("/cancel", "cancel", "отмена"):
            _fsm.pop(chat_id, None); cardinal.telegram.bot.send_message(chat_id, "❌ Отменено."); return
        t = text.replace(",", ".").strip()
        try:
            val = float(t)
            if val < 0:
                raise ValueError
        except Exception:
            cardinal.telegram.bot.send_message(chat_id, "⚠️ Введите неотрицательное число. Пример: 4.2"); return
        _set_cfg(chat_id, min_balance_ton=val)
        _fsm.pop(chat_id, None)
        cardinal.telegram.bot.send_message(chat_id, f"✅ Порог сохранён: {val} TON")
        _log("info", f"MIN BALANCE set to {val} TON")
        return

    if state.get("step") == "set_jwt":
            if (message.text or "").strip().lower() in ("/cancel", "cancel", "отмена"):
                _fsm.pop(chat_id, None)
                cardinal.telegram.bot.send_message(chat_id, "❌ Отменено.")
                return

            jwt_val = None
            file_bytes = None
            filename = None
            mime = None

            if getattr(message, "document", None):
                try:
                    filename = (message.document.file_name or "").lower()
                    mime = (message.document.mime_type or "").lower()
                    if message.document.file_size and message.document.file_size > 2_000_000:
                        cardinal.telegram.bot.send_message(chat_id, "⚠️ Файл слишком большой (>2MB). Пришлите меньший или вставьте токен текстом.")
                        return
                    f_info = cardinal.telegram.bot.get_file(message.document.file_id)
                    file_bytes = cardinal.telegram.bot.download_file(f_info.file_path)
                except Exception as e:
                    cardinal.telegram.bot.send_message(chat_id, f"⚠️ Не удалось прочитать файл: {e}")
                    return

            if file_bytes is not None:
                try:
                    text_data = file_bytes.decode("utf-8-sig", errors="ignore")
                except Exception:
                    text_data = file_bytes.decode("utf-8", errors="ignore")
                content = (text_data or "").strip()

                is_json = (filename or "").endswith(".json") or (mime or "").endswith("json") or content[:1] in "{["
                if is_json:
                    try:
                        obj = json.loads(content)
                        jwt_val = _find_jwt_in_json(obj)
                    except Exception:
                        jwt_val = None

                if not jwt_val:
                    cleaned = _clean_jwt_text(content)
                    parts = _re.findall(r"[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+", cleaned)
                    jwt_val = parts[0] if parts else cleaned

            else:
                part = _clean_jwt_text((message.text or ""))
                acc = _clean_jwt_text((state.get("jwt_acc") or "") + part)

                if not _is_jwt_like(acc) and len(acc) < 16:
                    state["jwt_acc"] = acc
                    _fsm[chat_id] = state
                    cardinal.telegram.bot.send_message(chat_id, "Принял часть токена. Пришлите оставшиеся части (или /cancel).")
                    return

                jwt_val = acc

            jwt_val = _clean_jwt_text(jwt_val or "")
            if not jwt_val or len(jwt_val) < 16:
                cardinal.telegram.bot.send_message(chat_id, "⚠️ Похоже на некорректный токен. Пришлите валидный JWT текстом или файлом .txt/.json, либо /cancel.")
                return

            _set_cfg(chat_id, fragment_jwt=jwt_val)
            ver, bal, resp = _check_fragment_wallet(jwt_val)
            _set_cfg(
                chat_id,
                wallet_version=ver,
                balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None),
                last_wallet_raw=resp
            )
            _fsm.pop(chat_id, None)
            cardinal.telegram.bot.send_message(chat_id, "✅ Токен сохранён.")
            _open_token(
                cardinal.telegram.bot,
                type("obj", (), {"message": type("m", (), {"chat": type("c", (), {"id": chat_id})(), "id": message.message_id})(), "id": ""})
            )
            return

def _jwt_confirmed(bot, call):
    chat_id = call.message.chat.id
    state = _fsm.get(chat_id) or {}
    api_key = state.get("api_key"); phone = state.get("phone"); wallet_ver = state.get("wallet_version") or "W5"; mnemonic = state.get("mnemonic")

    try: bot.answer_callback_query(call.id)
    except Exception: pass

    if not (api_key and phone and wallet_ver and mnemonic):
        bot.send_message(chat_id, "⚠️ Не хватает данных для создания токена. Попробуйте ещё раз."); _fsm.pop(chat_id, None); _open_token(bot, call); return

    jwt, raw, sc = _authenticate_fragment(api_key=api_key, phone_number=phone, version=wallet_ver, mnemonics=mnemonic)
    if jwt:
        _set_cfg(chat_id, fragment_jwt=jwt); bot.send_message(chat_id, "✅ Успешно: токен создан и привязан.")
        ver, bal, resp = _check_fragment_wallet(jwt)
        if ver is not None or bal is not None or resp is not None:
            _set_cfg(chat_id, wallet_version=ver, balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None), last_wallet_raw=resp)
        _fsm.pop(chat_id, None)
    else:
        is_tma, wait_sec = _is_too_many_attempts(raw)
        if sc == 400 and is_tma:
            bot.send_message(chat_id,
                f"Слишком много попыток входа. Подождите {wait_sec or 'несколько'} секунд и попробуйте ещё раз "
                "через «🔁 Отправить ещё раз» или «✅ Я подтвердил вход».")
        else:
            human = _human_auth_error(raw, sc)
            if sc == 400:
                bot.send_message(chat_id, f"❌ Не удалось выдать токен. {human}")
            else:
                bot.send_message(chat_id, f"❌ Не удалось выдать токен (HTTP {sc}). {human}")
            try:
                text = json.dumps(raw, ensure_ascii=False, indent=2)[:1900]
            except Exception:
                text = str(raw)[:1900]
            bot.send_message(chat_id, f"Ответ сервера:\n<code>{text}</code>", parse_mode="HTML")

def _jwt_resend(bot, call):
    chat_id = call.message.chat.id
    st = _fsm.get(chat_id) or {}
    api_key = st.get("api_key"); phone = st.get("phone"); wallet_ver = st.get("wallet_version") or "W5"; words = st.get("mnemonic")
    try: bot.answer_callback_query(call.id, "Отправляю запрос…")
    except Exception: pass
    if not (api_key and phone and words):
        bot.send_message(chat_id, "Не хватает данных для повтора."); return

    jwt, raw, sc = _authenticate_fragment(api_key=api_key, phone_number=phone, version=wallet_ver, mnemonics=words)
    if jwt:
        _set_cfg(chat_id, fragment_jwt=jwt); bot.send_message(chat_id, "✅ Успешно: токен создан и привязан.")
        ver, bal, resp = _check_fragment_wallet(jwt)
        if ver is not None or bal is not None or resp is not None:
            _set_cfg(chat_id, wallet_version=ver, balance_ton=(round(bal, 6) if isinstance(bal, (int, float)) else None), last_wallet_raw=resp)
        _fsm.pop(chat_id, None); _open_token(bot, call); return

    is_tma, wait_sec = _is_too_many_attempts(raw)
    if sc == 400 and is_tma:
        bot.send_message(chat_id,
            f"Слишком много попыток входа. Подождите {wait_sec or 'несколько'} секунд и попробуйте снова.")
    else:
        human = _human_auth_error(raw, sc)
        bot.send_message(chat_id, f"❌ Не удалось выдать токен (HTTP {sc}). {human}")
        try:
            pretty = json.dumps(raw, ensure_ascii=False, indent=2)
        except Exception:
            pretty = str(raw)
        bot.send_message(chat_id, "Подробности:\n<code>{}</code>".format(pretty[:1900]), parse_mode="HTML")

_pending_orders: Dict[str, List[Dict[str, Any]]] = {}
FTS_GLOBAL_QUEUE = bool(int(os.getenv("FTS_GLOBAL_QUEUE", "1")))
_GLOBAL_QKEY = "__global_orders__"
_prompted_orders: Dict[str, set] = {}
_prompted_oids: set[str] = set()
_preorders: Dict[str, Dict[str, Any]] = {}
_done_oids: set[str] = set()

def _remove_order_everywhere(oid: Optional[str]) -> None:
    if not oid:
        return
    s = str(oid)
    for key, q in list(_pending_orders.items()):
        for it in list(q):
            if str(it.get("order_id")) == s:
                it["finalized"] = True
                try:
                    q.remove(it)
                except ValueError:
                    pass
        if not q:
            try:
                del _pending_orders[key]
            except Exception:
                pass

def _mark_done(chat_id: Any, oid: Optional[str]):
    if not oid:
        return
    s = str(oid)
    _done_oids.add(s)
    _preorders.pop(s, None)
    _remove_order_everywhere(s)

_blocked_oids: set[str] = set()
_failed_orders: dict[str, dict] = {}

def _finalize_order(oid: str, chat_id: Any, *, ok: bool, reason: str = ""):
    oid = str(oid)
    if ok:
        _done_oids.add(oid)
    else:
        _blocked_oids.add(oid)
        _failed_orders[oid] = {"chat_id": chat_id, "reason": reason, "ts": time.time()}
    _remove_order_everywhere(oid)

def _set_order_qty(chat_id: Any, order_id: Optional[str], qty: Optional[int]) -> None:
    if not order_id or not qty or qty < 50:
        return
    try:
        for it in _q(chat_id):
            if str(it.get("order_id")) == str(order_id):
                it["qty"] = int(qty)
                break
        if str(order_id) in _preorders:
            _preorders[str(order_id)]["qty"] = int(qty)
    except Exception:
        pass

def _adopt_foreign_queue_for(chat_id: Any) -> bool:
    key = str(chat_id)
    if key in _pending_orders and _pending_orders[key]:
        return False

    for other_key, items in list(_pending_orders.items()):
        if other_key == key:
            continue

        for it in list(items):
            oid = it.get("order_id")
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

        if any(_allowed_stages(x) for x in items):
            _pending_orders[key] = items
            del _pending_orders[other_key]
            logger.warning(f"[QUEUE] merged {other_key} -> {key}")
            return True

    return False

def _mark_prompted(chat_id: Any, order_id: Optional[Any]) -> None:
    if order_id is None:
        return
    oid = str(order_id)
    _prompted_oids.add(oid)
    _prompted_orders.setdefault(str(chat_id), set()).add(oid)

def _was_prompted(chat_id: Any, order_id: Optional[Any]) -> bool:
    if order_id is None:
        return any(x.get("prompted") for x in _q(chat_id))
    oid = str(order_id)
    if oid in _prompted_oids:
        return True
    return oid in _prompted_orders.get(str(chat_id), set())

def _unmark_prompted(chat_id: Any, order_id: Optional[Any], *, everywhere: bool = False) -> None:
    if order_id is None:
        return
    oid = str(order_id)
    s = _prompted_orders.get(str(chat_id))
    if s:
        s.discard(oid)
    if everywhere:
        _prompted_oids.discard(oid)

def _q(chat_id: Any) -> List[Dict[str, Any]]:
    key = _GLOBAL_QKEY if FTS_GLOBAL_QUEUE else str(chat_id)
    return _pending_orders.setdefault(key, [])

def _current(chat_id: Any) -> Optional[Dict[str, Any]]:
    q = _q(chat_id)
    return q[0] if q else None

def _push(chat_id: Any, item: Dict[str, Any]) -> None:
    oid = item.get("order_id")
    if oid and (str(oid) in _done_oids or str(oid) in _blocked_oids):
        logger.debug(f"[QUEUE] skip push for done/blocked order #{oid}")
        return

    q = _q(chat_id)
    if oid and any(str(x.get("order_id")) == str(oid) for x in q):
        for x in q:
            if str(x.get("order_id")) == str(oid):
                for k, v in item.items():
                    if v is not None:
                        x[k] = v
                x.setdefault("prompted", False)
                break
        return

    item.setdefault("prompted", False)
    q.append(item)

def _ensure_pending(chat_id: Any, order_id: Optional[str], qty: Optional[int]) -> Dict[str, Any]:
    if order_id and (str(order_id) in _done_oids or str(order_id) in _blocked_oids):
        return {
            "qty": int(qty or 50),
            "order_id": order_id,
            "chat_id": chat_id,
            "stage": "finalized",
            "candidate": None,
            "finalized": True,
            "confirmed": True,
            "prompted": False,
            "preconfirmed": False,
            "auto_attempted_for": None,
            "queue_notified": False,
            "turn_ts": None,
        }

    q = _q(chat_id)

    if order_id:
        for x in q:
            if str(x.get("order_id")) == str(order_id):
                if qty and int(qty) >= 50:
                    x["qty"] = int(qty)
                x.setdefault("chat_id", chat_id)
                x.setdefault("stage", "await_username")
                x.setdefault("candidate", None)
                x.setdefault("finalized", False)
                x.setdefault("confirmed", False)
                x.setdefault("prompted", False)
                x.setdefault("preconfirmed", False)
                x.setdefault("auto_attempted_for", None)
                x.setdefault("queue_notified", False)
                x.setdefault("turn_ts", None)
                return x

    item: Dict[str, Any] = {
        "qty": int(qty or 50),
        "order_id": order_id,
        "chat_id": chat_id,
        "stage": "await_username",
        "candidate": None,
        "finalized": False,
        "confirmed": False,
        "prompted": False,
        "preconfirmed": False,
        "auto_attempted_for": None,
        "queue_notified": False,
        "turn_ts": None,
    }

    _push(chat_id, item)

    if order_id:
        for x in q:
            if str(x.get("order_id")) == str(order_id):
                return x

    return item


def _find_item_by_chat(chat_id: Any) -> Optional[Dict[str, Any]]:
    cid = str(chat_id)
    for it in _q(chat_id):
        if str(it.get("chat_id")) == cid and not it.get("finalized"):
            return it
    return None

def _active_item_for_chat(chat_id: Any) -> Optional[Dict[str, Any]]:
    if FTS_GLOBAL_QUEUE:
        return _find_item_by_chat(chat_id)
    return _current(chat_id)

def _queue_pos_of(item: Dict[str, Any]) -> int:
    q = _q(item.get("chat_id"))
    for i, it in enumerate(q, 1):
        if it is item:
            return i
    oid = str(item.get("order_id") or "")
    for i, it in enumerate(q, 1):
        if str(it.get("order_id") or "") == oid:
            return i
    return 9999

def _notify_queued_once(cardinal: "Cardinal", item: Dict[str, Any]) -> None:
    if item.get("queue_notified"):
        return
    pos = _queue_pos_of(item)
    if pos <= 1:
        return
    item["queue_notified"] = True
    _safe_send(cardinal, item["chat_id"], _tpl(item["chat_id"], "queued", pos=pos, qty=int(item.get("qty") or 50), order_id=item.get("order_id")))

def _notify_next_turn(cardinal: "Cardinal", chat_id: Any = None) -> None:
    nxt = _current(chat_id if (chat_id is not None) else "__any__")
    if not nxt:
        return
    nxt["turn_ts"] = time.time()
    cid = nxt.get("chat_id")
    if not cid:
        return
    oid = nxt.get("order_id")
    qty = int(nxt.get("qty") or 50)
    if oid:
        _unmark_prompted(cid, oid, everywhere=True)
    nxt["auto_attempted_for"] = None
    cand = (nxt.get("candidate") or "").lstrip("@").strip()
    if cand and _validate_username(cand):
        nxt.update(
            stage="await_confirm",
            candidate=cand,
            finalized=False,
            confirmed=False,
            prompted=True,
        )

        _safe_send(
            cardinal,
            cid,
            f"⭐️ Ваша очередь дошла на {qty}⭐.\n"
            f"Ник принят: @{cand}.\n"
            f"Начинаю отправку…"
        )

        _schedule_confirm_send(cardinal, cid)
        return

    nxt.update(
        stage="await_username",
        candidate=None,
        finalized=False,
        confirmed=False,
        prompted=True,
    )
    nxt["auto_attempted_for"] = None

    msg = _tpl(cid, "your_turn", qty=qty, order_id=oid)
    if msg.strip():
        _safe_send(cardinal, cid, msg)
    _mark_prompted(cid, oid)

def _pop_current(chat_id: Any, *, keep_prompted: bool = True) -> Optional[Dict[str, Any]]:
    q = _q(chat_id)
    item = q.pop(0) if q else None
    if item and item.get("order_id") and not keep_prompted:
        _unmark_prompted(chat_id, item.get("order_id"), everywhere=True)
    return item

def _prompt_current_order_if_needed(cardinal: "Cardinal", chat_id: Any) -> None:
    cfg = _get_cfg_for_orders(chat_id)
    cur = _current(chat_id)
    if not cur:
        return

    if cur.get("finalized") or not _allowed_stages(cur):
        return

    oid = cur.get("order_id")
    qty = int(cur.get("qty") or 50)

    if cur.get("prompted") or _was_prompted(chat_id, oid):
        return
    if not _should_prompt_once(chat_id, oid, qty):
        return

    use_pre = _cfg_bool(cfg, "preorder_username", False)
    cand = (cur.get("candidate") or "").lstrip("@").strip()

    if (not use_pre) and cand:
        cur.update(
            stage="await_confirm",
            candidate=cand,
            finalized=False,
            confirmed=False,
        )

        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=qty, username=cand, order_id=oid))

        if _auto_send_without_plus(chat_id):
            if cur.get("auto_attempted_for") != cand:
                cur["auto_attempted_for"] = cand
                _schedule_confirm_send(cardinal, chat_id)
            return

        cur["prompted"] = True
        if oid:
            _safe_send(cardinal, chat_id, f'Если всё верно — ответьте "+ #{oid}". Чтобы изменить — пришлите другой @username.')
        else:
            _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
        _mark_prompted(chat_id, oid)
        return


    cur.update(
        stage="await_username",
        candidate=None,
        prompted=True,
        finalized=False,
        confirmed=False,
    )
    cur["turn_ts"] = time.time()
    _safe_send(cardinal, chat_id, _tpl(chat_id, "purchase_created", qty=qty, order_id=oid))
    _mark_prompted(chat_id, oid)

def _update_current(chat_id: Any, **updates) -> None:
    cur = _current(chat_id)
    if cur is not None:
        cur.update(updates)

def _has_queue(chat_id: Any) -> bool:
    return bool(_q(chat_id))

def _send_html_chunks(bot, chat_id, text, kb=None):
    from telebot.apihelper import ApiTelegramException
    MAX = 3800
    s = text or ""
    chunks = []
    while s:
        part = s[:MAX]
        if len(s) > MAX:
            cut = max(part.rfind("\n"), part.rfind(". "))
            if cut >= 0 and cut > MAX // 4:
                part = part[:cut+1]
        chunks.append(part)
        s = s[len(part):]

    for i, part in enumerate(chunks):
        rm = kb if i == len(chunks) - 1 else None
        try:
            bot.send_message(
                chat_id, part, parse_mode="HTML",
                reply_markup=rm, disable_web_page_preview=True
            )
        except ApiTelegramException:
            bot.send_message(
                chat_id, part,
                reply_markup=rm, disable_web_page_preview=True
            )

def _safe_send(c: "Cardinal", chat_id, text: str):
    try:
        c.send_message(chat_id, text)
    except Exception as e:
        logger.warning(f"send_message failed: {e}")

def _is_auto_reply(msg) -> bool:
    try:
        if getattr(msg, "is_autoreply", False):
            return True

        badge = (getattr(msg, "badge", None) or getattr(msg, "badge_text", None) or "")
        if isinstance(badge, str) and "автоответ" in badge.lower():
            return True

        if getattr(msg, "by_bot", False) or getattr(msg, "by_vertex", False):
            return True
    except Exception:
        pass
    return False

_QUEUE_TICK_SEC = float(os.getenv("FTS_QUEUE_TICK_SEC", "5"))

def _queue_watchdog(cardinal: "Cardinal"):
    while True:
        time.sleep(_QUEUE_TICK_SEC)
        try:
            for _ in range(5):
                if not _maybe_rotate_queue_head(cardinal, "__orders__"):
                    break
        except Exception as e:
            logger.debug(f"queue_watchdog failed: {e}")

def new_order_handler(cardinal: Cardinal, event):
    chat_id = _event_chat_id(event)
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id if chat_id is not None else "__orders__"):
            break
    try:
        chat_id = _event_chat_id(event)
        cfg = _get_cfg_for_orders(chat_id if chat_id is not None else "__orders__")

        if not cfg.get("plugin_enabled", True):
            return

        order = getattr(event, "order", None)
        if order is not None and not _order_is_stars(order):
            return

        title = getattr(order, "title", None) or getattr(order, "name", None) or ""
        qty = _extract_qty_from_title(title)

        order_id = (
            getattr(order, "id", None)
            or getattr(order, "order_id", None)
            or getattr(event, "order_id", None)
        )
        if order_id and (str(order_id) in _done_oids or str(order_id) in _blocked_oids):
            return

        text_blob = " ".join(str(x) for x in [
            title,
            getattr(order, "description", None),
            getattr(order, "buyer_message", None),
        ] if x)

        if _is_gift_like_text(text_blob) or _mentions_account_login(text_blob):
            _log("info", f"[IGNORE] gift/account-login order ignored (#{order_id})")
            return

        if qty is not None and qty < 50:
            try:
                _safe_send(
                    cardinal,
                    chat_id,
                    "Извините, временно появилась ошибка при обработке заказа. "
                    "Заказы с количеством звёзд меньше 50⭐ обрабатываются вручную. "
                    "Мы временно отключили лоты и свяжемся с вами."
                )
            except Exception:
                pass
            cfg_local = _get_cfg_for_orders(chat_id if chat_id is not None else "__orders__")
            _deactivate_all_star_lots(cardinal, cfg_local, chat_id, reason="невалидный заказ (<50⭐)")
            return

        item = _ensure_pending(chat_id, order_id, qty if qty is not None else 50)
        if FTS_GLOBAL_QUEUE and item is not _current(chat_id):
            _notify_queued_once(cardinal, item)
            return

        username = None
        for candidate in [
            getattr(order, "title", None),
            getattr(order, "description", None),
            getattr(order, "buyer_message", None),
            getattr(event, "message", None),
        ]:
            u = _extract_username_from_order_text(candidate)
            if u:
                username = u
                break

        if not username:
            username = _extract_username_from_any(order) or _extract_username_from_any(event)

        try:
            my_user = (getattr(cardinal.account, "username", None) or "").lstrip("@").lower()
            if username and username.lower() == my_user:
                username = None
        except Exception:
            pass

        jwt = cfg.get("fragment_jwt")
        use_pre = bool(cfg.get("preorder_username", False))

        if use_pre and username and order_id:
            item.update(
                stage="await_paid",
                candidate=username.lstrip("@"),
                prompted=False,
                finalized=False,
                confirmed=False
            )
            _preorders[str(order_id)] = {"username": username.lstrip("@"), "qty": qty}
            return

        if jwt and (qty is not None and qty >= 50):

            if use_pre and username and not order_id:
                _safe_send(cardinal, chat_id, f"Ник из заказа: @{username}. Отправляю {qty}⭐…")

                _set_sending(chat_id, True)
                try:
                    resp = _order_stars_with_retry(
                        jwt,
                        username=username,
                        quantity=qty,
                        show_sender=False,
                        retry_enabled=bool(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT)),
                    )
                finally:
                    _set_sending(chat_id, False)
                _mark_done(chat_id, order_id)
                _preorders.pop(str(order_id), None)

                if resp.get("ok"):
                    _safe_send(cardinal, chat_id, f"✅ Готово: отправлено {qty}⭐ на @{username.lstrip('@')}.")
                    order_url = f"https://funpay.com/orders/{order_id}/" if order_id else ""
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "sent", qty=qty, username=username.lstrip("@"), order_url=order_url))
                    _pop_current(chat_id)
                    if _has_queue(chat_id):
                        _notify_next_turn(cardinal, chat_id)
                else:
                    kind, human = _classify_send_failure(resp.get("text",""), resp.get("status",0), username.lstrip("@"), jwt)
                    if kind == "username":
                        _update_current(chat_id, stage="await_username", finalized=False, candidate=None)
                        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
                        return
                    else:
                        _update_current(chat_id, finalized=True)
                        _mark_prompted(chat_id, order_id)
                        _safe_send(cardinal, chat_id, _tpl(chat_id, "failed", reason=human))
                        _log("error", f"ORDER FAIL #{order_id} {qty}⭐ @{username}: {human} | status={resp.get('status')}")
                        if cfg.get("auto_refund", False) and order_id:
                            _safe_send(cardinal, chat_id, "🔁 Пытаюсь оформить возврат…")
                            ok_ref = _auto_refund_order(cardinal, order_id, chat_id, reason=human)
                            _log("info" if ok_ref else "error", f"REFUND #{order_id} -> {'OK' if ok_ref else 'FAIL'}")
                        else:
                            _safe_send(cardinal, chat_id, "⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.")
                        _maybe_auto_deactivate(cardinal, cfg, chat_id)
                return

            cand = (username or "").lstrip("@")
            item.update(
                stage=("await_confirm" if cand else "await_username"),
                candidate=cand or None,
                prompted=True,
                finalized=False,
                confirmed=False
            )

            if item is _current(chat_id):
                if cand:
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=(qty or 50), username=cand))
                    _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                else:
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "purchase_created", qty=(qty or 50)))

            _mark_prompted(chat_id, order_id)
            return

        _log("info", f"ORDER #{order_id}: queued, waiting for username/system message.")

    except Exception as e:
        logger.exception(f"new_order_handler error: {e}")

_IMMUTABLE_META = {
    "CREDITS": "@tinechelovec",
    "UUID": "fa0c2f3a-7a85-4c09-a3b2-9f3a9b8f8a75",
    "CREATOR_URL": "https://t.me/tinechelovec",
    "GROUP_URL": "https://t.me/dev_thc_chat",
    "CHANNEL_URL": "https://t.me/by_thc",
    "GITHUB_URL": "https://github.com/tinechelovec/FPC-Plugin-Telegram-Stars",
    "INSTRUCTION_URL": "https://teletype.in/@tinechelovec/FTS-Plugin",
}

_IMMUTABLE_OK = True
_IMMUTABLE_REASON = ""

def _meta_guard() -> bool:
    global _IMMUTABLE_OK, _IMMUTABLE_REASON
    if not _IMMUTABLE_OK:
        return False

    for k, expected in _IMMUTABLE_META.items():
        cur = globals().get(k, None)
        if cur != expected:
            _IMMUTABLE_OK = False
            _IMMUTABLE_REASON = f"{k} изменён"
            try:
                logger.critical(f"[ANTI-TAMPER] immutable field changed: {k} expected={expected!r} got={cur!r}")
            except Exception:
                pass
            return False

    return True

def _tamper_text() -> str:
    reason = _IMMUTABLE_REASON or "обнаружены изменения в данных плагина"
    return (
        "⛔️ <b>Плагин не работает.</b>\n\n"
        f"Причина: <code>{reason}</code>\n\n"
        "Похоже, файл плагина был изменён или подменён.\n"
        "Установите оригинальную версию или обратитесь к создателю:\n\n"
        f"👤 <b>Создатель:</b> <a href=\"{_IMMUTABLE_META['CREATOR_URL']}\">{_IMMUTABLE_META['CREDITS']}</a>\n"
        f"👥 <b>Группа:</b> <a href=\"{_IMMUTABLE_META['GROUP_URL']}\">dev chat</a>\n"
        f"📣 <b>Канал:</b> <a href=\"{_IMMUTABLE_META['CHANNEL_URL']}\">channel</a>\n"
        f"🌐 <b>GitHub:</b> <a href=\"{_IMMUTABLE_META['GITHUB_URL']}\">repo</a>\n"
        f"📖 <b>Инструкция:</b> <a href=\"{_IMMUTABLE_META['INSTRUCTION_URL']}\">open</a>\n"
    )

def _tamper_kb() -> InlineKeyboardMarkup:
    kb = K()
    kb.row(B("👤 Создатель", url=_IMMUTABLE_META["CREATOR_URL"]),
           B("🌐 GitHub", url=_IMMUTABLE_META["GITHUB_URL"]))
    kb.row(B("👥 Группа", url=_IMMUTABLE_META["GROUP_URL"]),
           B("📣 Канал", url=_IMMUTABLE_META["CHANNEL_URL"]))
    kb.add(B("📖 Инструкция", url=_IMMUTABLE_META["INSTRUCTION_URL"]))
    return kb

CREDITS = _IMMUTABLE_META["CREDITS"]
UUID = _IMMUTABLE_META["UUID"]
CREATOR_URL = _IMMUTABLE_META["CREATOR_URL"]
GROUP_URL = _IMMUTABLE_META["GROUP_URL"]
CHANNEL_URL = _IMMUTABLE_META["CHANNEL_URL"]
GITHUB_URL = _IMMUTABLE_META["GITHUB_URL"]
INSTRUCTION_URL = _IMMUTABLE_META["INSTRUCTION_URL"]


def _do_confirm_send(cardinal: "Cardinal", chat_id):
    pend = _active_item_for_chat(chat_id)
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id):
            break

    if not pend:
        _safe_send(cardinal, chat_id, "Нет активного заказа. Если нужно — дождитесь нового сообщения о заказе.")
        return

    if FTS_GLOBAL_QUEUE:
        mode = _queue_mode(chat_id)
        head = _current(chat_id)
        if mode in (1, 3) and (not head or pend is not head):
            pend["preconfirmed"] = True
            pos = _queue_pos_of(pend)
            _safe_send(
                cardinal,
                chat_id,
                f"✅ Подтверждение принято. Сейчас ещё не ваша очередь (позиция {pos}). "
                "Когда очередь дойдёт — отправлю автоматически."
            )
            return

    oid = pend.get("order_id")
    if oid and str(oid) in _done_oids:
        if (not FTS_GLOBAL_QUEUE) or (pend is _current(chat_id)):
            _pop_current(chat_id, keep_prompted=False)
        return

    qty = int(pend.get("qty", 50))
    username = (pend.get("candidate") or "").strip()
    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get("fragment_jwt")

    if not jwt:
        _safe_send(cardinal, chat_id, "⚠️ Токен Fragment не привязан. Покупка невозможна.")
        _log("warn", "SEND aborted: no JWT")
        return

    if not username or not _validate_username(username):
        _safe_send(cardinal, chat_id, "❌ Некорректный тег. Отправьте в формате @username (5–32, латиница/цифры/подчёркивание).")
        pend.update(stage="await_username", candidate=None)
        _log("warn", f"SEND aborted: invalid username '{username}'")
        return

    if qty < 50:
        _safe_send(cardinal, chat_id, "Минимум 50⭐. Уточните количество или лот.")
        _log("warn", f"SEND aborted: qty {qty} < 50")
        return

    if not _check_username_exists_throttled(username, jwt, chat_id):
        _safe_send(cardinal, chat_id, f'❌ Ник "{username}" не найден. Пришлите верный тег в формате @username.')
        _log("warn", f"USERNAME not found (confirm): @{username}")
        return

    _safe_send(cardinal, chat_id, _tpl(chat_id, "sending", qty=qty, username=username.lstrip("@")))

    oid = pend.get("order_id")
    _set_sending(chat_id, True)
    try:
        resp = _order_stars_with_retry(
            jwt,
            username=username,
            quantity=qty,
            show_sender=False,
            retry_enabled=bool(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT)),
        )
    finally:
        _set_sending(chat_id, False)

    if resp and resp.get("ok"):
        order_url = f"https://funpay.com/orders/{oid}/" if oid else ""
        _safe_send(cardinal, chat_id, _tpl(chat_id, "sent", qty=qty, username=username.lstrip("@"), order_url=order_url))
        _log("info", f"SEND OK {qty}⭐ -> @{username}")

        if oid:
            _finalize_order(oid, chat_id, ok=True)

        if _has_queue(chat_id):
            _notify_next_turn(cardinal, chat_id)
        return

    kind, human = _classify_send_failure(
        (resp or {}).get("text", ""),
        (resp or {}).get("status", 0),
        username.lstrip("@"),
        jwt
    )

    if kind == "username":
        pend.update(stage="await_username", finalized=False, candidate=None)
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
        return

    _safe_send(cardinal, chat_id, _tpl(chat_id, "failed", reason=human))
    _log("error", f"SEND FAIL {qty}⭐ -> @{username}: {human} | status={(resp or {}).get('status')}")
    pend.update(finalized=True)

    if oid:
        _finalize_order(oid, chat_id, ok=False, reason=human)

    if cfg.get("auto_refund", False) and oid:
        _safe_send(cardinal, chat_id, "🔁 Пытаюсь оформить возврат…")
        ok_ref = _auto_refund_order(cardinal, oid, chat_id, reason=human)
        _log("info" if ok_ref else "error", f"REFUND #{oid} -> {'OK' if ok_ref else 'FAIL'}")
    else:
        _safe_send(cardinal, chat_id, "⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.")

    _maybe_auto_deactivate(cardinal, cfg, chat_id)

    if _has_queue(chat_id):
        _notify_next_turn(cardinal, chat_id)

def _cb_confirm_send(cardinal: "Cardinal", call):
    try:
        cardinal.telegram.bot.answer_callback_query(call.id)
    except Exception:
        pass
    _schedule_confirm_send(cardinal, call.message.chat.id)

def _cb_change_username(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, "Измените ник сообщением.")
    except Exception:
        pass
    pend = _current(chat_id)
    if not pend:
        cardinal.telegram.bot.send_message(chat_id, "Нет активного заказа.")
        return
    _update_current(chat_id, stage="await_username")
    cardinal.telegram.bot.send_message(chat_id, "Введите новый тег в формате @username:")

def _cb_cancel_flow(cardinal: "Cardinal", call):
    chat_id = call.message.chat.id
    try:
        cardinal.telegram.bot.answer_callback_query(call.id, "Отменено.")
    except Exception:
        pass
    removed = _pop_current(call.message.chat.id, keep_prompted=False)
    if _has_queue(chat_id):
        nxt = _current(chat_id); qn = int(nxt.get("qty", 50))
        cardinal.telegram.bot.send_message(chat_id, f"Текущий заказ отменён. Следующий: {qn}⭐.\nПришлите тег в формате @username одной строкой.")
    else:
        cardinal.telegram.bot.send_message(chat_id, "Текущий заказ отменён.")

def _allowed_stages(item: dict) -> bool:
    stage = str(item.get("stage"))
    if stage == "await_paid":
        return not item.get("finalized")
    return (
        stage in {"await_username", "await_confirm"}
        and not item.get("confirmed")
        and not item.get("finalized")
    )

def _pending_by_oid(chat_id: Any, oid: Optional[str]) -> Optional[dict]:
    if not oid:
        return None
    for x in _q(chat_id):
        if str(x.get("order_id")) == str(oid) and _allowed_stages(x):
            return x
    return None

def _apply_username_for_item(cardinal: "Cardinal", chat_id: Any, item: dict, uname: str):
    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get("fragment_jwt")

    if (not _skip_username_check(chat_id)) and jwt and (not _check_username_exists_throttled(uname, jwt, chat_id)):
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid", order_id=item.get("order_id")))
        item.update(stage="await_username", candidate=None)
        return

    if not _validate_username(uname):
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid", order_id=item.get("order_id")))
        item.update(stage="await_username", candidate=None)
        return

    if jwt and not _check_username_exists_throttled(uname, jwt, chat_id):
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid", order_id=item.get("order_id")))
        item.update(stage="await_username", candidate=None)
        return

    qty = int(item.get("qty") or 50)
    item.update(candidate=uname, stage="await_confirm", confirmed=False)

    _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=qty, username=uname, order_id=item.get("order_id"),),)
    if item.get("preconfirmed"):
        if (not FTS_GLOBAL_QUEUE) or (_current(chat_id) is item):
            _schedule_confirm_send(cardinal, chat_id)
        return
    if _auto_send_without_plus(chat_id):
        if FTS_GLOBAL_QUEUE and (_current(chat_id) is not item) and (_queue_mode(chat_id) != 2):
            return

        if FTS_GLOBAL_QUEUE and (_current(chat_id) is not item) and item.get("order_id"):
            _schedule_confirm_send(cardinal, chat_id, str(item.get("order_id")))
            return

        _schedule_confirm_send(cardinal, chat_id)
        return


    _safe_send(
        cardinal,
        chat_id,
        "Проверьте данные по заказу #{oid}:\n"
        "- Количество: {qty}⭐\n"
        "- Ник: @{uname}\n\n"
        "Если всё верно — ответьте \"+ #{}\".\n"
        "Чтобы изменить — пришлите другой тег формата @username с #OID."
        .format(item.get("order_id"))
        .replace("{oid}", str(item.get("order_id") or "—"))
        .replace("{qty}", str(qty))
        .replace("{uname}", uname)
    )

def _do_confirm_send_for_oid(cardinal: "Cardinal", chat_id: Any, oid: str):
    if str(oid) in _done_oids:
        _safe_send(cardinal, chat_id, f"Заказ #{oid} уже выполнен.")
        _remove_order_everywhere(oid)
        return
    head = _current(chat_id)
    if head and str(head.get("order_id")) == str(oid):
        _schedule_confirm_send(cardinal, chat_id)
        return

    item = _pending_by_oid(chat_id, oid)
    if FTS_GLOBAL_QUEUE and _queue_mode(chat_id) in (1, 3):
        head = _current(chat_id)
        if not head or str(head.get("order_id")) != str(oid):
            item["preconfirmed"] = True
            pos = _queue_pos_of(item)
            _safe_send(
                cardinal,
                chat_id,
                f"✅ Подтверждение принято. Сейчас ещё не ваша очередь (позиция {pos}). "
                "Когда очередь дойдёт — отправлю автоматически."
            )
            return

    if not item:
        if FTS_GLOBAL_QUEUE and _queue_mode(chat_id) in (1, 3):
            head = _current(chat_id)
            if not head or str(head.get("order_id")) != str(oid):
                item["preconfirmed"] = True
                pos = _queue_pos_of(item)
                _safe_send(
                    cardinal,
                    chat_id,
                    f"✅ Подтверждение принято. Сейчас ещё не ваша очередь (позиция {pos}). "
                    "Когда очередь дойдёт — отправлю автоматически."
                )
                return

        _safe_send(cardinal, chat_id, f"Не нашёл активный заказ #{oid} для подтверждения.")
        return

    cfg = _get_cfg_for_orders(chat_id)
    jwt = cfg.get("fragment_jwt")
    qty = int(item.get("qty") or 50)
    username = (item.get("candidate") or "").strip()

    if not jwt:
        _safe_send(cardinal, chat_id, "⚠️ Токен Fragment не привязан. Покупка невозможна.")
        return
    if not username or not _validate_username(username):
        _safe_send(cardinal, chat_id, f"❌ Для #{oid} не указан корректный @username.")
        item.update(stage="await_username")
        return
    if qty < 50:
        _safe_send(cardinal, chat_id, f"Минимум 50⭐. Заказ #{oid}.")
        return

    _safe_send(cardinal, chat_id, _tpl(chat_id, "sending", qty=qty, username=username))

    _set_sending(chat_id, True)
    if not _check_username_exists_throttled(username, jwt, chat_id):
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid", order_id=oid))
        item.update(stage="await_username", finalized=False, candidate=None)
        return
    try:
        resp = _order_stars_with_retry(
            jwt,
            username=username,
            quantity=qty,
            show_sender=False,
            retry_enabled=bool(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT)),
        )
    finally:
        _set_sending(chat_id, False)

        if resp and resp.get("ok"):
            order_url = f"https://funpay.com/orders/{oid}/"
            _safe_send(cardinal, chat_id, _tpl(chat_id, "sent", qty=qty, username=username, order_url=order_url))

            _finalize_order(oid, chat_id, ok=True)
            return

        kind, human = _classify_send_failure(
            (resp or {}).get("text", ""),
            (resp or {}).get("status", 0),
            username.lstrip("@"),
            jwt
        )

        if kind == "username":
            item.update(stage="await_username", finalized=False, candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid", order_id=oid))
            return

        _safe_send(cardinal, chat_id, _tpl(chat_id, "failed", reason=human))
        item.update(finalized=True)

        _finalize_order(oid, chat_id, ok=False, reason=human)

        if cfg.get("auto_refund", False) and oid:
            _safe_send(cardinal, chat_id, "🔁 Пытаюсь оформить возврат…")
            ok_ref = _auto_refund_order(cardinal, oid, chat_id, reason=human)
            _log("info" if ok_ref else "error", f"REFUND #{oid} -> {'OK' if ok_ref else 'FAIL'}")
        else:
            _safe_send(cardinal, chat_id, "⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.")

        _maybe_auto_deactivate(cardinal, cfg, chat_id)
        return

def _find_order_for_back(chat_id: Any, order_id: Optional[str]) -> Optional[dict]:
    q = _q(chat_id)
    candidates = [x for x in q if _allowed_stages(x)]
    if order_id:
        for x in candidates:
            if str(x.get("order_id")) == str(order_id):
                return x
        return None
    if len(candidates) == 1:
        return candidates[0]
    return None

def _list_pending_oids(chat_id: Any) -> List[str]:
    return [str(x.get("order_id")) for x in _q(chat_id) if _allowed_stages(x) and x.get("order_id")]

def new_message_handler(cardinal: Cardinal, event: NewMessageEvent):
    chat_id = _event_chat_id(event)
    for _ in range(3):
        if not _maybe_rotate_queue_head(cardinal, chat_id if chat_id is not None else "__orders__"):
            break
    try:
        my_user = (getattr(cardinal.account, "username", None) or "").lower()
        author  = (getattr(event.message, "author", "") or "").lower()

        chat_id = _event_chat_id(event)
        text = (event.message.text or "").strip()

        if _is_auto_reply(event.message):
            try:
                chat_id = _event_chat_id(event)
            except Exception:
                chat_id = None
            cur = _current(chat_id) or {}
            oid = cur.get("order_id")
            suffix = []
            if chat_id is not None:
                suffix.append(f"CID:{chat_id}")
            if oid:
                suffix.append(f"OID:{oid}")
            extra = (" (" + " ".join(suffix) + ")") if suffix else ""
            _log("info", f"[IGNORE] auto-reply skipped{extra}")
            return

        if author == "funpay" and (_is_gift_like_text(text) or _mentions_account_login(text)):
            _log("info", "[IGNORE] gift/account-login system note")
            return
        
        if _is_sending(chat_id) and (author != "funpay"):
            return

        cfg = _get_cfg_for_orders(chat_id)

        try:
            user_mid = getattr(event.message, "message_id", None) or getattr(event.message, "id", None)
        except Exception:
            user_mid = None

        if CLEAN_USER_MSGS:
            stage = (_current(chat_id) or {}).get("stage")
            waiting_input = stage in {"await_username", "await_confirm"}
            if text and author not in {"funpay"} and not waiting_input:
                _safe_delete(cardinal.telegram.bot, chat_id, user_mid)

        if not cfg.get("plugin_enabled", True):
            return

        while _has_queue(chat_id):
            head = _current(chat_id) or {}
            if head.get("finalized"):
                _pop_current(chat_id)
                continue
            if not _allowed_stages(head):
                _pop_current(chat_id)
                continue
            break

        if author == "funpay" and _funpay_is_system_paid_message(text):
            qty, oid = _funpay_extract_qty_and_order_id(text)
            hint_uname = _extract_explicit_handle(text)
            if oid and (str(oid) in _done_oids or str(oid) in _blocked_oids):
                return
            _set_order_qty(chat_id, oid, qty)
            if qty is not None and qty < 50:
                return

            known_here = any(str(x.get("order_id")) == str(oid) for x in _q(chat_id))
            known_any  = known_here or (
                oid and any(str(x.get("order_id")) == str(oid) for q in _pending_orders.values() for x in q)
            ) or (oid and str(oid) in _preorders)

            if not known_any:
                _ensure_pending(chat_id, oid, qty)
                if _should_prompt_once(chat_id, oid, qty or 0):
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "purchase_created", qty=(qty or 50)))
                    _mark_prompted(chat_id, oid)
                return

            pending = None
            for x in _q(chat_id):
                if str(x.get("order_id")) == str(oid):
                    pending = x
                    break

            if pending is None:
                pending = _ensure_pending(chat_id, oid, qty)
            else:
                old_chat = pending.get("chat_id")
                if str(old_chat) != str(chat_id):
                    pending["chat_id"] = chat_id
                    logger.warning(f"[QUEUE] bind order #{oid}: chat_id {old_chat} -> {chat_id}")

            use_pre = _cfg_bool(cfg, "preorder_username", False)
            jwt = cfg.get("fragment_jwt")
            uname = None
            real_qty = int(qty or 50)

            if use_pre and pending and str(pending.get("stage")) == "await_paid" and pending.get("candidate") and jwt:
                uname = str(pending["candidate"]).lstrip("@")
                real_qty = int(pending.get("qty") or qty or 50)

            elif use_pre and oid and _preorders.get(str(oid)) and jwt:
                pr = _preorders[str(oid)]
                uname = str(pr.get("username", "")).lstrip("@")
                real_qty = int(pr.get("qty") or real_qty)

            if not uname and hint_uname:
                uname = hint_uname.lstrip("@")

            if uname and jwt and not use_pre:
                item = pending or _ensure_pending(chat_id, oid, real_qty)
                item.update(
                    qty=int(real_qty),
                    candidate=str(uname).lstrip("@"),
                    stage="await_confirm",
                    finalized=False,
                    confirmed=False,
                    prompted=True,
                )
                _mark_prompted(chat_id, oid)

                _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=real_qty, username=uname))
                if oid:
                    _safe_send(cardinal, chat_id, f'Если всё верно — ответьте "+ #{oid}". Чтобы изменить — пришлите другой @username с #{oid}.')
                else:
                    _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                return

            if uname and jwt and use_pre:
                _update_current(chat_id, prompted=True)
                _mark_prompted(chat_id, oid)

                _safe_send(cardinal, chat_id, f"Ник из заказа: @{uname}. Отправляю {real_qty}⭐…")

                _set_sending(chat_id, True)
                try:
                    resp = _order_stars_with_retry(
                        jwt,
                        username=uname,
                        quantity=real_qty,
                        show_sender=False,
                        retry_enabled=bool(cfg.get("retry_liteserver", LITESERVER_RETRY_DEFAULT)),
                    )
                finally:
                    _set_sending(chat_id, False)


                if resp.get("ok"):
                    order_url = f"https://funpay.com/orders/{oid}/" if oid else ""
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "sent", qty=real_qty, username=uname, order_url=order_url))
                    _mark_done(chat_id, oid)

                    if oid:
                        _preorders.pop(str(oid), None)
                    head = _current(chat_id)
                    if head and str(head.get("order_id")) == str(oid):
                        _update_current(chat_id, finalized=True)
                        _pop_current(chat_id)
                    return
                
                if uname and jwt and not use_pre:
                    item = pending or _ensure_pending(chat_id, oid, real_qty)
                    item.update(
                        qty=int(real_qty),
                        candidate=str(uname).lstrip("@"),
                        stage="await_confirm",
                        finalized=False,
                        confirmed=False,
                        prompted=True,
                    )
                    _mark_prompted(chat_id, oid)

                    _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=real_qty, username=uname))
                    if oid:
                        _safe_send(cardinal, chat_id, f'Если всё верно — ответьте "+ #{oid}". Чтобы изменить — пришлите другой @username с #{oid}.')
                    else:
                        _safe_send(cardinal, chat_id, 'Если всё верно — ответьте "+". Чтобы изменить — пришлите другой @username.')
                    return

                kind, human = _classify_send_failure(resp.get("text",""), resp.get("status",0), uname, jwt)
                if kind == "username":
                    if oid:
                        _preorders.pop(str(oid), None)
                    _update_current(chat_id, stage="await_username", finalized=False, candidate=None, prompted=False)
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
                    return
                else:
                    if oid:
                        _preorders.pop(str(oid), None)
                    _update_current(chat_id, finalized=True)
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "failed", reason=human))
                    _log("error", f"ORDER FAIL #{oid} {real_qty}⭐ @{uname}: {human} | status={(resp or {}).get('status')}")
                    if cfg.get("auto_refund", False) and oid:
                        _safe_send(cardinal, chat_id, "🔁 Пытаюсь оформить возврат…")
                        ok_ref = _auto_refund_order(cardinal, oid, chat_id, reason=human)
                        _log("info" if ok_ref else "error", f"REFUND #{oid} -> {'OK' if ok_ref else 'FAIL'}")
                    else:
                        _safe_send(cardinal, chat_id, "⏳ У продавца автовозврат отключён. Пожалуйста, дождитесь продавца.")
                    _mark_prompted(chat_id, oid)
                    _maybe_auto_deactivate(cardinal, cfg, chat_id)
                    return

            if not _was_prompted(chat_id, oid):
                _ensure_pending(chat_id, oid, qty)
                if _should_prompt_once(chat_id, oid, qty or 0):
                    _safe_send(cardinal, chat_id, _tpl(chat_id, "purchase_created", qty=(qty or 50)))
                    _mark_prompted(chat_id, oid)
                    _update_current(chat_id, prompted=True, stage="await_username")
                return
            return

        if not text:
            return
        
        m_back = _re.match(r'^\s*!(?:бэк|бек|back)\b(?:\s*#?([A-Za-z0-9]{6,}))?\s*$', text, _re.I)
        if m_back:
            if not cfg.get("manual_refund_enabled", False):
                _safe_send(cardinal, chat_id, "Команда возврата выключена у продавца.")
                return

            if (not cfg.get("manual_refund_priority", True)) and (not cfg.get("auto_refund", False)):
                _safe_send(cardinal, chat_id, "Команда !бэк недоступна: приоритет ниже автовозврата, а автовозврат отключён.")
                return

            oid_arg = m_back.group(1)

            target = _find_order_for_back(chat_id, oid_arg)
            if not target:
                target = _current(chat_id)

            if not target or not _allowed_stages(target):
                _safe_send(cardinal, chat_id, "Нет активного заказа для возврата.")
                return

            oid = target.get("order_id")
            if not oid:
                _safe_send(cardinal, chat_id, "Не вижу order_id у текущего заказа.")
                return

            ok = _auto_refund_order(cardinal, oid, chat_id, reason="Возврат по запросу покупателя (!бэк)")
            if ok:
                try:
                    _q(chat_id).remove(target)
                except ValueError:
                    pass
            return

        if author == my_user and (_current(chat_id) or {}).get("stage") not in {"await_username", "await_confirm"}:
            return

        if author == "funpay":
            u = _extract_username_from_text(text)
            if not u or u.lower() == my_user.lstrip("@"):
                return

        pend = _find_item_by_chat(chat_id) if FTS_GLOBAL_QUEUE else _current(chat_id)
        if not pend:
            return
        if not _has_queue(chat_id):
            logger.warning(f"[QUEUE] no head for chat_id={chat_id}; queues={list(_pending_orders.keys())[:5]}")
        if not pend:
            return

        nick_items = [
            x for x in _q(chat_id)
            if str(x.get("stage")) in {"await_username", "await_confirm"} and not x.get("finalized")
        ]
        nick_oids = [str(x.get("order_id")) for x in nick_items if x.get("order_id")]
        many_nick_orders = len(nick_oids) > 1

        m_plus = _re.match(r'^\s*(?:\+{1,2}|ok|да)\s*(?:#?([A-Za-z0-9\-]{6,}))?\s*$', text, _re.I)
        if m_plus and author != "funpay":
            oid_arg = m_plus.group(1)
            if oid_arg:
                _schedule_confirm_send(cardinal, chat_id, oid_arg)
            else:
                _schedule_confirm_send(cardinal, chat_id)
            return

        username = _extract_username_from_order_text(text)
        if not username:
            if pend:
                pend.update(stage="await_username", candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
            return

        uname = username.lstrip("@")

        m_username_oid = _re.search(r"#([A-Za-z0-9]{6,})", text)
        if m_username_oid:
            target_oid = m_username_oid.group(1)
            item = _pending_by_oid(chat_id, target_oid)
            if not item:
                _safe_send(cardinal, chat_id, f"Не нашёл активный заказ #{target_oid} для ника @{uname}.")
                return

            _apply_username_for_item(cardinal, chat_id, item, uname)
            return

        if not _validate_username(uname):
            if pend:
                pend.update(stage="await_username", candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
            return
        
        jwt = cfg.get("fragment_jwt")
        if jwt and (not _skip_username_check(chat_id)):
            if not _check_username_exists_throttled(uname, jwt, chat_id):
                if pend:
                    pend.update(stage="await_username", candidate=None)
                _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
                return

        qty = int(pend.get("qty", 0)) or 50
        if "qty" not in pend:
            cfg_tmp = _get_cfg_for_orders(chat_id)
            enabled_qty = [int(o["qty"]) for o in (cfg_tmp.get("star_lots") or []) if o.get("active")]
            if len(enabled_qty) == 1:
                qty = enabled_qty[0]

        if pend:
            pend.update(qty=int(qty), candidate=uname, stage="await_confirm", finalized=False, confirmed=False)
        _safe_send(cardinal, chat_id, _tpl(chat_id, "username_valid", qty=qty, username=uname))
        if _auto_send_without_plus(chat_id):
            if FTS_GLOBAL_QUEUE and (pend is not _current(chat_id)):
                return
            _schedule_confirm_send(cardinal, chat_id)
            return

        _safe_send(
            cardinal, chat_id,
            "Проверьте данные:\n"
            f"- Количество: {qty}⭐\n"
            f"- Ник: @{uname}\n\n"
            'Если всё верно — ответьте "+".\n'
            "Чтобы изменить — пришлите другой тег в формате @username."
        )

        jwt = cfg.get("fragment_jwt")
        if not _check_username_exists_throttled(uname, jwt, chat_id):
            if pend:
                pend.update(stage="await_username", candidate=None)
            _safe_send(cardinal, chat_id, _tpl(chat_id, "username_invalid"))
            return

        if str(pend.get("stage")) == "await_confirm" and text.lower() in {"+", "++", "да", "ок", "ok"}:
            _schedule_confirm_send(cardinal, chat_id)
            return

    except Exception as e:
        logger.exception(f"new_message_handler error: {e}")

BIND_TO_PRE_INIT    = [init_cardinal]
BIND_TO_NEW_MESSAGE = [new_message_handler]
try:
    BIND_TO_NEW_ORDER = [new_order_handler]
except Exception:
    pass
BIND_TO_DELETE = None