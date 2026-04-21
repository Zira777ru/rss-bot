#!/usr/bin/env python3
"""RSS News Bot v4: FreshRSS → Gemini AI → Telegram"""

import os
import re
import time
import hashlib
import sqlite3
import logging
import threading
import requests
from datetime import datetime, timezone
from typing import Optional

from google import genai as google_genai
from google.genai import types as genai_types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

FRESHRSS_URL      = os.environ["FRESHRSS_URL"].rstrip("/")
FRESHRSS_USER     = os.environ["FRESHRSS_USER"]
FRESHRSS_PASS     = os.environ["FRESHRSS_PASS"]
TG_TOKEN          = os.environ["TG_TOKEN"]
TG_CHANNEL        = os.environ["TG_CHANNEL"]
GEMINI_KEY        = os.environ["GEMINI_KEY"]
ADMIN_TG_ID       = os.environ.get("ADMIN_TG_ID", "")
DB_PATH           = os.environ.get("DB_PATH", "/data/news.db")
INTERVAL_MIN      = int(os.environ.get("INTERVAL_MIN", "30"))
BATCH_SIZE        = int(os.environ.get("BATCH_SIZE", "50"))
MAX_POSTS_PER_RUN = int(os.environ.get("MAX_POSTS_PER_RUN", "5"))
TZ_OFFSET         = int(os.environ.get("TZ_OFFSET", "3"))
FRESHRSS_CATEGORY = os.environ.get("FRESHRSS_CATEGORY", "")
BLACKLIST_ENV     = {s.strip().lower() for s in os.environ.get("BLACKLIST_SOURCES", "").split(",") if s.strip()}
STATS_HOUR        = int(os.environ.get("STATS_HOUR", "9"))
DIGEST_HOUR       = int(os.environ.get("DIGEST_HOUR", "20"))
DIGEST_COUNT      = int(os.environ.get("DIGEST_COUNT", "5"))
KEEP_DAYS         = int(os.environ.get("KEEP_DAYS", "30"))
MIN_SCORE         = int(os.environ.get("MIN_SCORE", "5"))
KEYWORD_BLACKLIST = {s.strip().lower() for s in os.environ.get("BLACKLIST_KEYWORDS", "").split(",") if s.strip()}

TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"

HELP_TEXT = """\
🤖 RSS-бот — справка для администратора

━━━ КАК РАБОТАЕТ ━━━
Каждые {interval} мин берёт статьи из FreshRSS (категория: {category}), фильтрует через AI и публикует лучшие находки в канал.

Фильтр пропускает: GitHub-репозитории, новые инструменты и сервисы, open source библиотеки, бизнес-возможности, SaaS, лайфхаки с конкретным инструментом.
Фильтр блокирует: новости, политику, пресс-релизы, мнения без продукта, рекламу, крипто.

Конвейер:
1. Blacklist источников и ключевых слов
2. Gemini flash-lite: SKIP / PASS + оценка 1–10 + теги
3. Тег-дедупликация за 72ч (≥2 совпадающих тега = дубль)
4. Отсев ниже MIN_SCORE={min_score}
5. Топ-{max_posts} по оценке → Gemini Pro пишет пост → публикация

━━━ УПРАВЛЕНИЕ ━━━
/status   — состояние, время прогона, стоимость за день
/stats    — полная статистика за сегодня
/run      — запустить прогон немедленно
/pause    — остановить публикации (прогоны продолжаются)
/resume   — возобновить публикации

━━━ БЛОКИРОВКИ ━━━
/blacklist list/add/remove <источник>
  Блокирует по вхождению строки в название источника.
  Пример: /blacklist add r/python

/keyword list/add/remove <слово>
  Блокирует статью если слово есть в заголовке.
  Пример: /keyword add розыгрыш

━━━ СТАТУСЫ В БД ━━━
posted      — опубликовано в канал
skipped     — реклама/мусор или оценка ниже {min_score}
duplicate   — то же событие уже выходило (теги)
blacklisted — заблокированный источник или слово
ranked_out  — PASS, но не вошёл в топ-{max_posts}
error       — ошибка публикации в Telegram

━━━ КОНФИГ (docker-compose.yml) ━━━
INTERVAL_MIN      интервал прогона (сейчас {interval} мин)
BATCH_SIZE        статей за прогон (сейчас {batch})
MAX_POSTS_PER_RUN макс. публикаций за прогон (сейчас {max_posts})
MIN_SCORE         мин. оценка для публикации (сейчас {min_score})
FRESHRSS_CATEGORY категория RSS (сейчас {category})
DIGEST_HOUR       дайджест топ-5 в HH:00 (сейчас {digest_h}:00)
STATS_HOUR        авто-статистика в HH:00 (сейчас {stats_h}:00)
KEEP_DAYS         хранить записи N дней (сейчас {keep_days})

━━━ СТОИМОСТЬ ━━━
Фильтр:    gemini-2.5-flash-lite ($0.10/M input)
Написание: gemini-2.5-pro ($1.25/M input, $10/M output)
Текущая стоимость за день — в /status"""


# ── Время ─────────────────────────────────────────────────────────────────────

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def local_hour() -> int:
    return (utcnow().hour + TZ_OFFSET) % 24


# ── База данных ───────────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = open_db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS seen (
            hash       TEXT PRIMARY KEY,
            title      TEXT,
            source     TEXT,
            summary    TEXT,
            status     TEXT,
            score      INTEGER DEFAULT 0,
            ts         INTEGER,
            created_at INTEGER DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS blacklist (
            source   TEXT PRIMARY KEY,
            added_at INTEGER DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS keyword_blacklist (
            keyword  TEXT PRIMARY KEY,
            added_at INTEGER DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    cols = {r[1] for r in con.execute("PRAGMA table_info(seen)").fetchall()}
    for col, typ in [("summary","TEXT"), ("status","TEXT"), ("score","INTEGER DEFAULT 0"), ("tags","TEXT DEFAULT ''")]:
        if col not in cols:
            con.execute(f"ALTER TABLE seen ADD COLUMN {col} {typ}")
    con.execute("CREATE INDEX IF NOT EXISTS idx_seen_recent ON seen(created_at, status)")
    for src in BLACKLIST_ENV:
        con.execute("INSERT OR IGNORE INTO blacklist (source) VALUES (?)", (src,))
    for kw in KEYWORD_BLACKLIST:
        con.execute("INSERT OR IGNORE INTO keyword_blacklist (keyword) VALUES (?)", (kw,))
    # сбрасываем зависшие pending после перезапуска
    result = con.execute("UPDATE seen SET status='ranked_out' WHERE status='pending'")
    if result.rowcount:
        log.info(f"Очищено {result.rowcount} зависших pending статей")
    con.commit()
    return con


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def is_seen(con: sqlite3.Connection, url: str) -> bool:
    return con.execute("SELECT 1 FROM seen WHERE hash=?", (url_hash(url),)).fetchone() is not None


def mark_seen(con: sqlite3.Connection, url: str, title: str, source: str,
              summary: str, status: str, ts: int, score: int = 0, tags: str = ""):
    con.execute(
        "INSERT OR IGNORE INTO seen (hash,title,source,summary,status,score,ts,tags) VALUES (?,?,?,?,?,?,?,?)",
        (url_hash(url), title, source, summary, status, score, ts, tags),
    )
    con.commit()


def tag_duplicate(con: sqlite3.Connection, tags: list[str], hours: int = 72) -> bool:
    if len(tags) < 2:
        return False
    cutoff = int(time.time()) - hours * 3600
    rows = con.execute(
        """SELECT tags FROM seen
           WHERE created_at > ? AND tags != '' AND status IN ('posted', 'pending')""",
        (cutoff,),
    ).fetchall()
    tag_set = set(tags)
    for (row_tags,) in rows:
        existing = {t.strip() for t in row_tags.split(",") if t.strip()}
        if len(tag_set & existing) >= 2:
            return True
    return False


def update_status(con: sqlite3.Connection, url: str, status: str):
    con.execute("UPDATE seen SET status=? WHERE hash=?", (status, url_hash(url)))
    con.commit()


def recent_summaries(con: sqlite3.Connection, hours: int = 48, limit: int = 10) -> list[str]:
    cutoff = int(time.time()) - hours * 3600
    rows = con.execute(
        """SELECT summary FROM seen
           WHERE created_at > ? AND summary IS NOT NULL AND summary != ''
             AND status IN ('posted','pending')
           ORDER BY created_at DESC LIMIT ?""",
        (cutoff, limit),
    ).fetchall()
    return [r[0][:80] for r in rows]


def get_blacklist(con: sqlite3.Connection) -> set[str]:
    return {r[0] for r in con.execute("SELECT source FROM blacklist").fetchall()}


def blacklist_add(con: sqlite3.Connection, source: str):
    con.execute("INSERT OR IGNORE INTO blacklist (source) VALUES (?)", (source.lower(),))
    con.commit()


def blacklist_remove(con: sqlite3.Connection, source: str):
    con.execute("DELETE FROM blacklist WHERE source=?", (source.lower(),))
    con.commit()


def is_blacklisted(con: sqlite3.Connection, source: str) -> bool:
    sl = source.lower()
    return any(b in sl for b in get_blacklist(con))


def get_keywords(con: sqlite3.Connection) -> set[str]:
    return {r[0] for r in con.execute("SELECT keyword FROM keyword_blacklist").fetchall()}


def keyword_add(con: sqlite3.Connection, keyword: str):
    con.execute("INSERT OR IGNORE INTO keyword_blacklist (keyword) VALUES (?)", (keyword.lower(),))
    con.commit()


def keyword_remove(con: sqlite3.Connection, keyword: str):
    con.execute("DELETE FROM keyword_blacklist WHERE keyword=?", (keyword.lower(),))
    con.commit()


def is_keyword_blocked(title: str, keywords: set[str]) -> bool:
    tl = title.lower()
    return any(kw in tl for kw in keywords)


def today_start_ts() -> int:
    d = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return int(d.timestamp())


def get_today_stats(con: sqlite3.Connection) -> dict[str, int]:
    rows = con.execute(
        "SELECT status, COUNT(*) FROM seen WHERE created_at >= ? GROUP BY status",
        (today_start_ts(),),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_top_sources(con: sqlite3.Connection, limit: int = 5) -> list[tuple]:
    return con.execute(
        """SELECT source, COUNT(*) cnt FROM seen
           WHERE created_at >= ? AND status='posted'
           GROUP BY source ORDER BY cnt DESC LIMIT ?""",
        (today_start_ts(), limit),
    ).fetchall()


def get_meta(con: sqlite3.Connection, key: str) -> Optional[str]:
    r = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r[0] if r else None


def set_meta(con: sqlite3.Connection, key: str, value: str):
    con.execute("INSERT OR REPLACE INTO meta (key,value) VALUES (?,?)", (key, value))
    con.commit()


# ── Telegram ──────────────────────────────────────────────────────────────────

def fetch_image(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"})
        if r.status_code == 200 and "image" in r.headers.get("content-type", ""):
            return r.content
    except Exception as e:
        log.debug(f"fetch_image: {e}")
    return None


def _tg_post(chat_id, text: str, photo_bytes: Optional[bytes] = None,
             photo_url: Optional[str] = None) -> bool:
    if len(text) > 4090:
        text = text[:4086] + "..."
    if photo_bytes:
        cap = text[:1024] if len(text) > 1024 else text
        r = requests.post(f"{TG_API}/sendPhoto",
            data={"chat_id": chat_id, "caption": cap},
            files={"photo": ("photo.jpg", photo_bytes, "image/jpeg")},
            timeout=30)
        if r.status_code == 200:
            return True
        log.warning(f"sendPhoto(bytes) {r.status_code}: {r.text[:80]}")
    elif photo_url:
        cap = text[:1024] if len(text) > 1024 else text
        r = requests.post(f"{TG_API}/sendPhoto",
            json={"chat_id": chat_id, "photo": photo_url, "caption": cap},
            timeout=20)
        if r.status_code == 200:
            return True
        log.warning(f"sendPhoto(url) {r.status_code} — пробую прокси")
        img = fetch_image(photo_url)
        if img:
            return _tg_post(chat_id, text, photo_bytes=img)
        log.warning("Прокси не помог, отправляю текстом")
    r = requests.post(f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
        timeout=20)
    return r.status_code == 200


def send_to_channel(text: str, image_url: Optional[str] = None) -> bool:
    ok = _tg_post(TG_CHANNEL, text, photo_url=image_url)
    if not ok:
        send_admin(f"❌ Не удалось опубликовать:\n{text[:150]}")
    return ok


def send_admin(text: str):
    if not ADMIN_TG_ID:
        return
    try:
        _tg_post(ADMIN_TG_ID, text)
    except Exception as e:
        log.error(f"send_admin: {e}")


# ── Статистика и служебные задачи ─────────────────────────────────────────────

def build_stats_text(con: sqlite3.Connection) -> str:
    s = get_today_stats(con)
    sources = get_top_sources(con)
    today = utcnow().strftime("%Y-%m-%d")
    lines = [
        f"📊 Статистика за {today}", "",
        f"✅ Опубликовано: {s.get('posted', 0)}",
        f"🏆 Не прошли рейтинг: {s.get('ranked_out', 0)}",
        f"🚫 Реклама/спам: {s.get('skipped', 0)}",
        f"🔁 Дубликаты: {s.get('duplicate', 0) + s.get('url_dup', 0)}",
        f"⛔ Blacklist: {s.get('blacklisted', 0)}",
        f"❌ Ошибки: {s.get('error', 0)}",
        "",
        f"💸 Стоимость Gemini: ${_daily_cost:.4f}",
    ]
    if sources:
        lines += ["", "📰 Топ источников:"]
        for i, (src, cnt) in enumerate(sources, 1):
            lines.append(f"  {i}. {src} — {cnt}")
    return "\n".join(lines)


def maybe_send_daily_stats(db: sqlite3.Connection):
    if local_hour() < STATS_HOUR:
        return
    today = utcnow().strftime("%Y-%m-%d")
    if get_meta(db, "last_stats_date") == today:
        return
    send_admin(build_stats_text(db))
    set_meta(db, "last_stats_date", today)
    log.info("Дневная статистика отправлена")


def maybe_send_digest(db: sqlite3.Connection):
    if local_hour() != DIGEST_HOUR:
        return
    today = utcnow().strftime("%Y-%m-%d")
    if get_meta(db, "last_digest_date") == today:
        return
    cutoff = int(time.time()) - 86400
    rows = db.execute(
        """SELECT summary, score FROM seen
           WHERE created_at > ? AND status='posted' AND summary != ''
           ORDER BY score DESC LIMIT ?""",
        (cutoff, DIGEST_COUNT),
    ).fetchall()
    if not rows:
        return
    medals = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣"]
    date_str = utcnow().strftime("%d.%m.%Y")
    lines = [f"📰 Дайджест за {date_str}", "", "🏆 Лучшие новости дня:", ""]
    for i, (summary, score) in enumerate(rows):
        lines.append(f"{medals[i]} [{score}/10]  {summary}")
        lines.append("")
    ok = send_to_channel("\n".join(lines))
    if ok:
        set_meta(db, "last_digest_date", today)
        log.info("Дайджест отправлен в канал")


def maybe_cleanup_db(db: sqlite3.Connection):
    if local_hour() != 0:
        return
    today = utcnow().strftime("%Y-%m-%d")
    if get_meta(db, "last_cleanup_date") == today:
        return
    cutoff = int(time.time()) - KEEP_DAYS * 86400
    result = db.execute(
        "DELETE FROM seen WHERE created_at < ? AND status NOT IN ('posted')",
        (cutoff,),
    )
    db.commit()
    log.info(f"Очистка БД: удалено {result.rowcount} записей старше {KEEP_DAYS} дней")
    set_meta(db, "last_cleanup_date", today)


# ── FreshRSS ──────────────────────────────────────────────────────────────────

class FreshRSS:
    def __init__(self):
        self._token: Optional[str] = None

    def _login(self):
        r = requests.post(
            f"{FRESHRSS_URL}/api/greader.php/accounts/ClientLogin",
            data={"Email": FRESHRSS_USER, "Passwd": FRESHRSS_PASS}, timeout=15,
        )
        r.raise_for_status()
        for line in r.text.splitlines():
            if line.startswith("Auth="):
                self._token = line[5:]
                log.info("FreshRSS: авторизован")
                return
        raise RuntimeError(f"Auth не найден: {r.text[:200]}")

    def _h(self) -> dict:
        return {"Authorization": f"GoogleLogin auth={self._token}"}

    def get_unread(self, n: int) -> list[dict]:
        if not self._token:
            self._login()
        if FRESHRSS_CATEGORY:
            stream = f"user/-/label/{FRESHRSS_CATEGORY}"
        else:
            stream = "user/-/state/com.google/reading-list"
        r = requests.get(
            f"{FRESHRSS_URL}/api/greader.php/reader/api/0/stream/contents/{stream}",
            params={"xt": "user/-/state/com.google/read", "n": n, "output": "json"},
            headers=self._h(), timeout=20,
        )
        if r.status_code == 401:
            self._token = None
            self._login()
            return self.get_unread(n)
        r.raise_for_status()
        return r.json().get("items", [])

    def mark_read(self, item_id: str):
        try:
            token = requests.get(f"{FRESHRSS_URL}/api/greader.php/reader/api/0/token",
                                 headers=self._h(), timeout=10).text.strip()
            requests.post(f"{FRESHRSS_URL}/api/greader.php/reader/api/0/edit-tag",
                          data={"i": item_id, "a": "user/-/state/com.google/read", "T": token},
                          headers=self._h(), timeout=10)
        except Exception as e:
            log.warning(f"mark_read: {e}")


# ── Утилиты ───────────────────────────────────────────────────────────────────

def strip_html(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


# Технические эмодзи которые источники ставят в начало заголовков как маркеры
_LEADING_EMOJI = re.compile(
    r'^[\U0001F300-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF'
    r'\u23E9-\u23FF\u25AA-\u25FE\u2702-\u27B0⚡↩️🔴🟡⬆⬇]+[\s]*'
)

def clean_title(title: str) -> str:
    """Убирает технические эмодзи-маркеры из начала заголовков."""
    return _LEADING_EMOJI.sub("", title).strip()


def find_image(item: dict) -> Optional[str]:
    if t := item.get("media$thumbnail"):
        if u := t.get("url"):
            return u
    for enc in item.get("enclosure", []):
        if enc.get("type", "").startswith("image/") and (u := enc.get("href")):
            return u
    bags = [item.get("summary", {})] + item.get("content", [])
    for bag in bags:
        raw = bag.get("content", "") if isinstance(bag, dict) else ""
        if m := re.search(r'<img[^>]+src=["\']([^"\']+)["\']', raw, re.I):
            if m.group(1).startswith("http"):
                return m.group(1)
    return None


def titles_similar(t1: str, t2: str, threshold: float = 0.55) -> bool:
    """Грубая проверка схожести заголовков по пересечению слов."""
    w1 = set(re.findall(r'\w{4,}', t1.lower()))
    w2 = set(re.findall(r'\w{4,}', t2.lower()))
    if not w1 or not w2:
        return False
    return len(w1 & w2) / min(len(w1), len(w2)) >= threshold


# ── Gemini: два раздельных вызова ────────────────────────────────────────────

_gemini: Optional[google_genai.Client] = None


def get_gemini() -> google_genai.Client:
    global _gemini
    if _gemini is None:
        _gemini = google_genai.Client(api_key=GEMINI_KEY)
    return _gemini


# Цены на токены ($/M): [input, output]
_MODEL_PRICES: dict[str, tuple[float, float]] = {
    "gemini-2.5-flash-lite": (0.10, 0.40),
    "gemini-2.5-flash":      (0.15, 0.60),
    "gemini-2.5-pro":        (1.25, 10.00),
}
_daily_cost: float = 0.0
_daily_cost_date: str = ""


def _track_cost(model: str, input_tokens: int, output_tokens: int):
    global _daily_cost, _daily_cost_date
    today = utcnow().strftime("%Y-%m-%d")
    if _daily_cost_date != today:
        _daily_cost = 0.0
        _daily_cost_date = today
    key = next((k for k in _MODEL_PRICES if k in model), None)
    if key:
        p_in, p_out = _MODEL_PRICES[key]
        _daily_cost += (input_tokens * p_in + output_tokens * p_out) / 1_000_000


def _gemini_call(prompt: str, max_tokens: int = 200, temp: float = 0.2,
                 thinking: bool = True, thinking_budget: int = 0,
                 model: str = "gemini-2.5-flash") -> str:
    cfg = genai_types.GenerateContentConfig(
        max_output_tokens=max_tokens,
        temperature=temp,
    )
    if not thinking:
        cfg.thinking_config = genai_types.ThinkingConfig(thinking_budget=0)
    elif thinking_budget > 0:
        cfg.thinking_config = genai_types.ThinkingConfig(thinking_budget=thinking_budget)

    last_exc: Exception = RuntimeError("no attempts")
    for attempt in range(2):
        try:
            resp = get_gemini().models.generate_content(
                model=model, contents=prompt, config=cfg,
            )
            if resp.usage_metadata:
                _track_cost(model,
                            resp.usage_metadata.prompt_token_count or 0,
                            resp.usage_metadata.candidates_token_count or 0)
            return (resp.text or "").strip()
        except Exception as e:
            last_exc = e
            if attempt == 0:
                log.warning(f"Gemini retry после ошибки: {e}")
                time.sleep(10)
    raise last_exc


def ai_filter(title: str, content: str, source: str) -> tuple[str, int, list[str]]:
    """
    Фильтрация, оценка и извлечение тегов.
    Возвращает ('skip'|'duplicate'|'pass', score, tags).
    """
    prompt = f"""Ты фильтр Telegram-канала об интересных инструментах, GitHub-репозиториях и бизнес-возможностях.

Источник: {source}
Заголовок: {title}
Текст: {content[:800]}

Ответь СТРОГО в формате (ровно три строки):
DECISION: SKIP|PASS
SCORE: <число 1-10>
TAGS: <3-5 тегов через запятую строчными буквами: технология/язык, тип контента, область применения>

PASS — GitHub-репозиторий, новый инструмент/сервис/продукт, open source библиотека, бизнес-возможность, SaaS, интересная находка для разработчика или предпринимателя, лайфхак с конкретным инструментом.
SKIP — обычная новость (политика, конфликты, корпоративные события), пресс-релиз без продукта, мнение/колонка без конкретного инструмента, реклама, SEO-статья, финансовые новости, криптовалюта.
SCORE — полезность и интерес: 10=меняет правила игры, 9=очень полезно широкой аудитории, 7-8=хороший инструмент или возможность, 5-6=нишевая полезность, 3-4=минимальный интерес, 1-2=незначительное.
TAGS — пример: «github, python, автоматизация» или «saas, продуктивность, no-code»."""

    try:
        raw = _gemini_call(prompt, max_tokens=40, temp=0.1, thinking=False,
                           model="gemini-2.5-flash-lite")
        decision = "pass"
        score = 5
        tags: list[str] = []
        for line in raw.splitlines():
            upper = line.upper()
            if upper.startswith("DECISION:"):
                val = upper.split(":", 1)[1].strip()
                if "SKIP" in val:
                    decision = "skip"
            elif upper.startswith("SCORE:"):
                try:
                    score = max(1, min(10, int(re.search(r"\d+", line).group())))
                except Exception:
                    pass
            elif upper.startswith("TAGS:"):
                raw_tags = line.split(":", 1)[1].strip().lower()
                tags = [t.strip() for t in raw_tags.split(",") if t.strip()][:5]
        return decision, score, tags
    except Exception as e:
        log.error(f"ai_filter ошибка: {e}")
        return "skip", 0, []


def ai_write(title: str, content: str, source: str) -> Optional[str]:
    """
    Шаг 2 — пишет финальный пост. Вызывается только для отобранных статей.
    """
    prompt = f"""Ты редактор Telegram-канала об интересных инструментах, GitHub-репозиториях и бизнес-возможностях. Пишешь на русском языке.

Источник: {source}
Заголовок: {title}
Текст: {content[:3000]}

Напиши пост для Telegram. Строгие правила:

1. Первый символ — эмодзи категории:
   🛠 инструмент/утилита/CLI
   🤖 AI/ML/автоматизация
   💼 бизнес/SaaS/монетизация
   📦 open source/библиотека
   🚀 продуктивность/workflow
   🔐 безопасность/приватность

2. Структура: 2–3 предложения.
   • Первое — что это: название + одна фраза о сути (не повторяй заголовок дословно).
   • Второе — зачем: какую проблему решает или чем конкретно полезно.
   • Третье (если есть) — ключевая деталь: язык, звёзды на GitHub, цена/бесплатность, уникальная фича.

3. Стиль:
   • Конкретно и ёмко, без воды и маркетинга
   • Технические термины допустимы, не надо всё упрощать
   • Для GitHub-репо — упомяни язык и звёзды если есть в тексте
   • Адаптируй английские названия, не переводи дословно

4. Запрещено:
   • Добавлять факты которых нет в тексте
   • «Стало известно», «разработчики представили», «команда выпустила»
   • Упоминать название источника
   • Общие фразы без конкретики («очень полезный инструмент», «революционное решение»)

Пост (только текст, никаких пояснений):"""

    try:
        text = _gemini_call(prompt, max_tokens=3000, temp=0.4,
                            thinking_budget=2048, model="gemini-2.5-pro")
        if not text or len(text.split()) < 10:
            log.warning(f"ai_write вернул слишком короткий текст: {repr(text)}")
            return None
        return text
    except Exception as e:
        log.error(f"ai_write ошибка: {e}")
        return None


# ── Telegram Bot — команды ────────────────────────────────────────────────────

def handle_command(con: sqlite3.Connection, text: str,
                   paused: threading.Event, run_event: threading.Event):
    text = text.strip()
    if text == "/status":
        last_run = get_meta(con, "last_run_time") or "ещё не запускался"
        restarts = get_meta(con, "restart_count") or "1"
        s = get_today_stats(con)
        state = "⏸ на паузе" if paused.is_set() else "▶️ активен"
        send_admin(
            f"🤖 Статус: {state}\n"
            f"Последний прогон: {last_run}\n"
            f"Опубликовано сегодня: {s.get('posted', 0)}\n"
            f"Перезапусков: {restarts}\n"
            f"💸 Стоимость сегодня: ${_daily_cost:.4f}"
        )
    elif text == "/stats":
        send_admin(build_stats_text(con))
    elif text == "/run":
        if paused.is_set():
            send_admin("⏸ Бот на паузе — сначала /resume")
        else:
            send_admin("🔄 Запускаю прогон...")
            run_event.set()
    elif text == "/pause":
        paused.set()
        set_meta(con, "paused", "1")
        send_admin("⏸ Бот на паузе. /resume для возобновления.")
    elif text == "/resume":
        paused.clear()
        set_meta(con, "paused", "0")
        send_admin("▶️ Бот возобновлён.")
    elif text == "/blacklist list":
        bl = sorted(get_blacklist(con))
        send_admin("⛔ Источники:\n" + ("\n".join(f"• {s}" for s in bl) if bl else "пусто"))
    elif text.startswith("/blacklist add "):
        src = text[15:].strip()
        if src:
            blacklist_add(con, src)
            send_admin(f"⛔ Источник добавлен: {src}")
    elif text.startswith("/blacklist remove "):
        src = text[18:].strip()
        if src:
            blacklist_remove(con, src)
            send_admin(f"✅ Источник удалён: {src}")
    elif text == "/keyword list":
        kws = sorted(get_keywords(con))
        send_admin("🔤 Ключевые слова:\n" + ("\n".join(f"• {k}" for k in kws) if kws else "пусто"))
    elif text.startswith("/keyword add "):
        kw = text[13:].strip()
        if kw:
            keyword_add(con, kw)
            send_admin(f"🔤 Слово добавлено: {kw}")
    elif text.startswith("/keyword remove "):
        kw = text[16:].strip()
        if kw:
            keyword_remove(con, kw)
            send_admin(f"✅ Слово удалено: {kw}")
    elif text in ("/help", "/start"):
        send_admin(HELP_TEXT.format(
            interval=INTERVAL_MIN, batch=BATCH_SIZE,
            max_posts=MAX_POSTS_PER_RUN, min_score=MIN_SCORE,
            category=FRESHRSS_CATEGORY or "все",
            digest_h=DIGEST_HOUR, stats_h=STATS_HOUR, keep_days=KEEP_DAYS,
        ))
    else:
        send_admin(f"Неизвестная команда.\n{HELP_TEXT}")


def poll_commands(paused: threading.Event, run_event: threading.Event):
    con = open_db()
    offset = 0
    log.info("Команды: polling запущен")
    while True:
        try:
            r = requests.get(f"{TG_API}/getUpdates",
                params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=35)
            if r.status_code != 200:
                time.sleep(5)
                continue
            for upd in r.json().get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message", {})
                if str(msg.get("from", {}).get("id", "")) == str(ADMIN_TG_ID):
                    txt = msg.get("text", "")
                    if txt.startswith("/"):
                        log.info(f"Команда: {txt}")
                        handle_command(con, txt, paused, run_event)
        except Exception as e:
            log.error(f"poll_commands: {e}")
            time.sleep(5)


# ── Основной цикл ─────────────────────────────────────────────────────────────

def run_once(rss: FreshRSS, db: sqlite3.Connection, paused: threading.Event):
    maybe_send_daily_stats(db)
    maybe_send_digest(db)
    maybe_cleanup_db(db)

    if paused.is_set() or get_meta(db, "paused") == "1":
        log.info("На паузе, пропускаю")
        return

    log.info("Получаю непрочитанные статьи…")
    try:
        items = rss.get_unread(BATCH_SIZE)
    except Exception as e:
        msg = f"⚠️ FreshRSS недоступен:\n{e}"
        log.error(msg)
        send_admin(msg)
        return

    items = sorted(items, key=lambda x: x.get("published", 0))
    log.info(f"Получено {len(items)} статей")

    # ── Шаг 1: фильтрация и оценка всего батча ───────────────────────────────
    candidates = []   # (score, title, content, item, image, url, source, ts, tags)
    url_dups = skipped = dups = blacklisted = errors = 0
    blacklist_set = get_blacklist(db)
    keywords_set  = get_keywords(db)

    for item in items:
        links = item.get("canonical") or item.get("alternate") or []
        url = links[0].get("href", "") if links else ""
        if not url:
            continue
        if is_seen(db, url):
            url_dups += 1
            rss.mark_read(item.get("id", ""))
            continue

        raw_title = item.get("title", "Без заголовка")
        title     = clean_title(raw_title)
        source    = item.get("origin", {}).get("title", "Unknown")
        ts        = item.get("published", 0)

        if any(b in source.lower() for b in blacklist_set):
            log.info(f"  ⛔ [{source}]: {title[:50]}")
            blacklisted += 1
            mark_seen(db, url, title, source, "", "blacklisted", ts)
            rss.mark_read(item.get("id", ""))
            continue

        if is_keyword_blocked(title, keywords_set):
            log.info(f"  🔤 KEYWORD: {title[:70]}")
            blacklisted += 1
            mark_seen(db, url, title, source, "", "blacklisted", ts)
            rss.mark_read(item.get("id", ""))
            continue

        raw_html = item.get("summary", {}).get("content", "") or \
                   "".join(c.get("content", "") for c in item.get("content", []))
        content = strip_html(raw_html)
        image   = find_image(item)

        decision, score, tags = ai_filter(title, content, source)
        time.sleep(4)  # Gemini rate limit

        if decision == "skip":
            log.info(f"  🚫 SKIP: {title[:70]}")
            skipped += 1
            mark_seen(db, url, title, source, "", "skipped", ts)
            rss.mark_read(item.get("id", ""))
        elif score < MIN_SCORE:
            log.info(f"  📉 LOW [{score}/10]: {title[:70]}")
            skipped += 1
            mark_seen(db, url, title, source, "", "skipped", ts)
            rss.mark_read(item.get("id", ""))
        else:
            # tag-дедупликация: проверяем совпадение тегов с уже опубликованным/pending
            tags_str = ",".join(tags)
            if tag_duplicate(db, tags):
                log.info(f"  🏷️ TAG-DUP [{tags_str}]: {title[:60]}")
                dups += 1
                mark_seen(db, url, title, source, "", "duplicate", ts, tags=tags_str)
                rss.mark_read(item.get("id", ""))
            else:
                mark_seen(db, url, title, source, f"[ожидает]: {title}",
                          "pending", ts, score, tags=tags_str)
                candidates.append((score, title, content, item, image, url, source, ts, tags))

    # ── Шаг 2: выбираем топ-N, дедупликация внутри кандидатов ───────────────
    candidates.sort(key=lambda x: x[0], reverse=True)
    log.info(f"Кандидаты: {len(candidates)}, выбираю топ {MAX_POSTS_PER_RUN}")

    selected = []
    for cand in candidates:
        score, title, tags = cand[0], cand[1], cand[8]
        tag_set = set(tags)
        # дубль по тегам среди уже выбранных в этом батче
        tag_clash = tag_set and any(len(tag_set & set(s[8])) >= 2 for s in selected)
        if tag_clash or any(titles_similar(title, s[1]) for s in selected):
            log.info(f"  ♻️  дубль среди кандидатов: {title[:60]}")
            update_status(db, cand[5], "ranked_out")
            rss.mark_read(cand[3].get("id", ""))
            continue
        selected.append(cand)
        if len(selected) >= MAX_POSTS_PER_RUN:
            break

    # остальные кандидаты помечаем ranked_out
    selected_urls = {c[5] for c in selected}
    for cand in candidates:
        if cand[5] not in selected_urls:
            update_status(db, cand[5], "ranked_out")
            rss.mark_read(cand[3].get("id", ""))

    # ── Шаг 3: пишем и публикуем ─────────────────────────────────────────────
    posted = 0
    for score, title, content, item, image, url, source, ts, tags in selected:
        log.info(f"  ✍️  [{score}/10] пишу пост: {title[:60]}")
        post_text = ai_write(title, content, source)
        time.sleep(4)  # rate limit для write-вызова

        if not post_text:
            update_status(db, url, "error")
            errors += 1
            continue

        # обновляем summary с реальным текстом поста
        db.execute("UPDATE seen SET summary=? WHERE hash=?", (post_text, url_hash(url)))
        db.commit()

        ok = send_to_channel(post_text, image)
        if ok:
            update_status(db, url, "posted")
            posted += 1
            log.info(f"  ✅ [{score}/10] опубликовано: {title[:65]}")
            rss.mark_read(item.get("id", ""))
            time.sleep(2)
        else:
            update_status(db, url, "error")
            errors += 1

    now_str = utcnow().strftime("%Y-%m-%d %H:%M UTC")
    set_meta(db, "last_run_time", now_str)
    set_meta(db, "daily_cost", f"{_daily_cost:.6f}")
    set_meta(db, "daily_cost_date", utcnow().strftime("%Y-%m-%d"))
    log.info(
        f"Итог: ✅{posted} | 🔁{url_dups+dups} дублей "
        f"| 🚫{skipped} реклама | ⛔{blacklisted} blacklist | ❌{errors} ошибок"
    )


def main():
    global _daily_cost, _daily_cost_date

    db        = init_db()
    rss       = FreshRSS()
    paused    = threading.Event()
    run_event = threading.Event()

    if get_meta(db, "paused") == "1":
        paused.set()

    # восстанавливаем накопленную стоимость если перезапуск в тот же день
    saved_date = get_meta(db, "daily_cost_date") or ""
    if saved_date == utcnow().strftime("%Y-%m-%d"):
        _daily_cost = float(get_meta(db, "daily_cost") or "0")
        _daily_cost_date = saved_date

    # счётчик перезапусков
    restarts = int(get_meta(db, "restart_count") or "0") + 1
    set_meta(db, "restart_count", str(restarts))

    log.info(f"Бот v4 | {INTERVAL_MIN}мин | батч={BATCH_SIZE} | лимит={MAX_POSTS_PER_RUN}/прогон | min_score={MIN_SCORE}")

    if ADMIN_TG_ID:
        threading.Thread(target=poll_commands, args=(paused, run_event), daemon=True).start()

    category_str = FRESHRSS_CATEGORY or "все категории"
    send_admin(
        f"🤖 RSS-бот v4 запущен (#{restarts})\n"
        f"Интервал: {INTERVAL_MIN} мин | Лимит: {MAX_POSTS_PER_RUN}/прогон | Min score: {MIN_SCORE}\n"
        f"Категория: {category_str} | Дайджест: {DIGEST_HOUR}:00\n"
        f"Команды: /help"
    )

    run_once(rss, db, paused)
    while True:
        # ждём таймер или принудительный /run
        run_event.wait(timeout=INTERVAL_MIN * 60)
        run_event.clear()
        run_once(rss, db, paused)


if __name__ == "__main__":
    main()
