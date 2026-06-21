from __future__ import annotations
import os, re, html, shutil, logging
import requests

NAME = "FTS-Temp-Updater"
VERSION = "1.1.0"
DESCRIPTION = "Временный мини-плагин для обновления FTS-Plugin командой /update_fts."
CREDITS = "@tinechelovec"
UUID = "6d3d1fc0-2e6d-46b8-9cf8-14a81911d0a7"
SETTINGS_PAGE = False

TARGET_NAME = "FTS-Plugin"
TARGET_UUID = "fa0c2f3a-7a85-4c09-a3b2-9f3a9b8f8a75"
TARGET_UUID_B64 = "ZmEwYzJmM2EtN2E4NS00YzA5LWEzYjItOWYzYTliOGY4YTc1"
UPDATE_URL = os.getenv(
    "FTS_PLUGIN_UPDATE_URL",
    "https://raw.githubusercontent.com/tinechelovec/FPC-Plugin-Telegram-Stars/main/FTS-Plugin/FTS-Plugin.py",
).strip()

log = logging.getLogger(NAME)
_http = requests.Session()

def _h(x):
    return html.escape(str(x), quote=False)

def _send(bot, chat_id, text):
    try:
        return bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
    except TypeError:
        return bot.send_message(chat_id, text, parse_mode="HTML")

def _ver(src):
    m = re.search(r'(?m)^\s*VERSION\s*=\s*["\']([^"\']+)["\']', src or "")
    return m.group(1).strip() if m else None

def _vkey(v):
    nums = [int(x) for x in re.findall(r"\d+", str(v or "0"))[:4]]
    return tuple(nums + [0] * (4 - len(nums)))

def _read(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return f.read()

def _is_fts_source(src):
    if not src or TARGET_NAME not in src or "def init_cardinal" not in src:
        return False
    return TARGET_UUID in src or TARGET_UUID_B64 in src

def _candidate_dirs():
    here = os.path.dirname(os.path.abspath(__file__))
    dirs = [here, os.getcwd(), os.path.join(os.getcwd(), "storage", "plugins")]
    parent = os.path.dirname(here)
    if parent:
        dirs.append(parent)
    out = []
    for d in dirs:
        d = os.path.abspath(d)
        if os.path.isdir(d) and d not in out:
            out.append(d)
    return out

def _find_fts_plugin():
    env = os.getenv("FTS_PLUGIN_FILE", "").strip()
    self_file = os.path.abspath(__file__)
    checked, priority = set(), []
    if env:
        priority.append(os.path.abspath(env))
    for d in _candidate_dirs():
        priority += [os.path.join(d, "FTS-Plugin.py"), os.path.join(d, "FTS_Plugin.py")]
    for path in priority:
        if path in checked or path == self_file or not os.path.isfile(path):
            continue
        checked.add(path)
        try:
            if _is_fts_source(_read(path)):
                return path
        except Exception:
            pass
    for d in _candidate_dirs():
        try:
            for root, subdirs, files in os.walk(d):
                if root.count(os.sep) - d.count(os.sep) > 2:
                    subdirs[:] = []
                    continue
                for fn in files:
                    if not fn.endswith(".py"):
                        continue
                    path = os.path.abspath(os.path.join(root, fn))
                    if path in checked or path == self_file:
                        continue
                    checked.add(path)
                    try:
                        if _is_fts_source(_read(path)):
                            return path
                    except Exception:
                        pass
        except Exception:
            pass
    raise RuntimeError("не нашёл установленный FTS-Plugin.py. Можно указать путь через FTS_PLUGIN_FILE")

def _download():
    if not UPDATE_URL.lower().startswith("https://"):
        raise RuntimeError("ссылка обновления должна быть HTTPS")
    r = _http.get(UPDATE_URL, headers={"Accept": "text/plain, */*;q=0.1", "User-Agent": f"{NAME}/{VERSION}"}, timeout=60)
    r.raise_for_status()
    data = r.content or b""
    if len(data) < 10000:
        raise RuntimeError(f"GitHub вернул слишком маленький файл ({len(data)} байт)")
    if len(data) > 5 * 1024 * 1024:
        raise RuntimeError("файл обновления слишком большой")
    try:
        src = data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise RuntimeError(f"файл обновления не UTF-8: {e}") from e
    if "<html" in src[:700].lower() or "<!doctype" in src[:700].lower():
        raise RuntimeError("вместо Python-файла скачалась HTML-страница")
    missing = [x for x in (TARGET_NAME, "def init_cardinal", "BIND_TO_PRE_INIT", "BIND_TO_NEW_MESSAGE") if x not in src]
    if missing:
        raise RuntimeError("скачан не тот файл: нет " + ", ".join(missing))
    if not _is_fts_source(src):
        raise RuntimeError("UUID скачанного FTS-Plugin не совпадает")
    new_ver = _ver(src)
    if not new_ver:
        raise RuntimeError("в скачанном файле не найдена VERSION")
    return src, new_ver, data

def _cleanup_pyc(path):
    try:
        base = os.path.splitext(os.path.basename(path))[0]
        cache = os.path.join(os.path.dirname(path), "__pycache__")
        if os.path.isdir(cache):
            for fn in os.listdir(cache):
                if fn.startswith(base + ".") and fn.endswith(".pyc"):
                    os.remove(os.path.join(cache, fn))
    except Exception:
        pass

def _atomic_replace(path, payload):
    tmp = path + ".fts_update_tmp"
    with open(tmp, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.chmod(tmp, os.stat(path).st_mode)
    except Exception:
        pass
    os.replace(tmp, path)
    _cleanup_pyc(path)

def _delete_self():
    if os.getenv("FTS_UPDATER_KEEP_SELF", "0").strip() in ("1", "true", "yes", "on"):
        return "оставлен по FTS_UPDATER_KEEP_SELF"
    self_file = os.path.abspath(__file__)
    try:
        _cleanup_pyc(self_file)
        os.remove(self_file)
        return "удалён"
    except Exception as e:
        return f"не удалён автоматически: {_h(e)}"

def _cmd_update(cardinal, message):
    bot, chat_id = cardinal.telegram.bot, message.chat.id
    try:
        target = _find_fts_plugin()
        old_src = _read(target)
        old_ver = _ver(old_src) or "не найдена"
        _send(bot, chat_id, f"⏬ <b>Проверяю обновление FTS-Plugin…</b>\n\nТекущая версия: <code>{_h(old_ver)}</code>")
        new_src, new_ver, new_payload = _download()
        _send(bot, chat_id, f"🧩 <b>Версии FTS-Plugin</b>\n\nТекущая: <code>{_h(old_ver)}</code>\nНовая: <code>{_h(new_ver)}</code>")
        if old_ver != "не найдена" and _vkey(new_ver) <= _vkey(old_ver):
            _send(bot, chat_id, "✅ <b>Обновление не требуется.</b>\n\nФайл плагина, конфиг и updater не изменены.")
            return
        compile(new_src, target, "exec")
        _atomic_replace(target, new_payload)
        self_status = _delete_self()
        _send(
            bot,
            chat_id,
            "✅ <b>FTS-Plugin обновлён.</b>\n\n"
            f"Версия: <code>{_h(old_ver)}</code> → <code>{_h(new_ver)}</code>\n"
            "💾 Конфиг сохранён: <code>storage/plugins/FTS-Plugin/settings.json</code>\n"
            "🗑 Старая версия основного файла заменена новой.\n"
            f"🧹 Временный updater: <code>{self_status}</code>\n\n"
            "🔁 Теперь выполните: <code>/restart</code>"
        )
    except Exception as e:
        log.exception("FTS update failed")
        try:
            _send(bot, chat_id, f"❌ <b>Не удалось обновить FTS-Plugin.</b>\n\nОшибка: <code>{_h(e)}</code>\n\nКонфиг и текущий FTS-файл не изменены.")
        except Exception:
            pass

def init_cardinal(cardinal):
    try:
        cardinal.add_telegram_commands(UUID, [("update_fts", "Обновить FTS-Plugin", True)])
    except Exception:
        pass
    cardinal.telegram.msg_handler(lambda m: _cmd_update(cardinal, m), commands=["update_fts"])
    log.info("FTS temporary updater loaded. Command: /update_fts")

BIND_TO_PRE_INIT = [init_cardinal]
BIND_TO_NEW_MESSAGE = []
BIND_TO_NEW_ORDER = []
BIND_TO_DELETE = None
