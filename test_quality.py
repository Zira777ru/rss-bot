#!/usr/bin/env python3
"""
Тест качества постов.
Берёт реальные статьи из FreshRSS, прогоняет через двухшаговый AI
и показывает результат без публикации в Telegram.

Запуск:
  docker compose run --rm rss-bot python test_quality.py
  docker compose run --rm rss-bot python test_quality.py --count 20 --write-all
"""

import os
import re
import time
import argparse
import requests
from typing import Optional

from google import genai as google_genai
from google.genai import types as genai_types

# ── Config из env ─────────────────────────────────────────────────────────────
FRESHRSS_URL      = os.environ["FRESHRSS_URL"].rstrip("/")
FRESHRSS_USER     = os.environ["FRESHRSS_USER"]
FRESHRSS_PASS     = os.environ["FRESHRSS_PASS"]
GEMINI_KEY        = os.environ["GEMINI_KEY"]
FRESHRSS_CATEGORY = os.environ.get("FRESHRSS_CATEGORY", "")
BLACKLIST_ENV     = {s.strip().lower() for s in os.environ.get("BLACKLIST_SOURCES", "").split(",") if s.strip()}
KEYWORD_BLACKLIST = {s.strip().lower() for s in os.environ.get("BLACKLIST_KEYWORDS", "").split(",") if s.strip()}
MIN_SCORE         = int(os.environ.get("MIN_SCORE", "5"))

# ── Утилиты (зеркало main.py) ─────────────────────────────────────────────────

_LEADING_EMOJI = re.compile(
    r'^[\U0001F300-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF'
    r'\u23E9-\u23FF\u25AA-\u25FE\u2702-\u27B0⚡↩️🔴🟡⬆⬇]+[\s]*'
)

def clean_title(t: str) -> str:
    return _LEADING_EMOJI.sub("", t).strip()

def strip_html(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()

def is_blacklisted(source: str) -> bool:
    sl = source.lower()
    return any(b in sl for b in BLACKLIST_ENV)

def is_keyword_blocked(title: str) -> bool:
    tl = title.lower()
    return any(kw in tl for kw in KEYWORD_BLACKLIST)


# ── FreshRSS ──────────────────────────────────────────────────────────────────

def fetch_articles(n: int = 15) -> list[dict]:
    r = requests.post(
        f"{FRESHRSS_URL}/api/greader.php/accounts/ClientLogin",
        data={"Email": FRESHRSS_USER, "Passwd": FRESHRSS_PASS}, timeout=15,
    )
    r.raise_for_status()
    token = next(l[5:] for l in r.text.splitlines() if l.startswith("Auth="))
    headers = {"Authorization": f"GoogleLogin auth={token}"}
    stream = f"user/-/label/{FRESHRSS_CATEGORY}" if FRESHRSS_CATEGORY else "user/-/state/com.google/reading-list"
    r = requests.get(
        f"{FRESHRSS_URL}/api/greader.php/reader/api/0/stream/contents/{stream}",
        params={"xt": "user/-/state/com.google/read", "n": n, "output": "json"},
        headers=headers, timeout=20,
    )
    r.raise_for_status()
    return r.json().get("items", [])


# ── Gemini ────────────────────────────────────────────────────────────────────

_gemini: Optional[google_genai.Client] = None

def get_gemini() -> google_genai.Client:
    global _gemini
    if _gemini is None:
        _gemini = google_genai.Client(api_key=GEMINI_KEY)
    return _gemini

def _call(prompt: str, max_tokens: int, temp: float,
          thinking: bool = True, thinking_budget: int = 0,
          model: str = "gemini-2.5-flash") -> str:
    cfg = genai_types.GenerateContentConfig(max_output_tokens=max_tokens, temperature=temp)
    if not thinking:
        cfg.thinking_config = genai_types.ThinkingConfig(thinking_budget=0)
    elif thinking_budget > 0:
        cfg.thinking_config = genai_types.ThinkingConfig(thinking_budget=thinking_budget)
    resp = get_gemini().models.generate_content(model=model, contents=prompt, config=cfg)
    return (resp.text or "").strip()


def ai_filter(title: str, content: str, source: str) -> tuple[str, int, list[str]]:
    prompt = f"""Ты фильтр новостного Telegram-канала.

Статья:
Источник: {source}
Заголовок: {title}
Текст: {content[:800]}

Ответь СТРОГО в формате (ровно три строки):
DECISION: SKIP|PASS
SCORE: <число 1-10>
TAGS: <3-5 тегов через запятую строчными буквами: сначала конкретные имена/организации/продукты/страны, затем тип события>

SKIP — реклама, промо, SEO-мусор, советы/лайфхаки, мнение без фактов, кликбейт без реального события.
PASS — настоящее новостное событие с конкретным фактом.
SCORE — важность: 10=историческое, 9=крупное мировое, 7-8=важное, 5-6=обычное, 3-4=мелкое, 1-2=незначительное.
TAGS — пример: «nvidia, rtx 5090, анонс» или «европол, ddos, арест»."""

    raw = _call(prompt, max_tokens=40, temp=0.1, thinking=False,
                model="gemini-2.5-flash-lite")
    decision, score, tags = "pass", 5, []
    for line in raw.splitlines():
        upper = line.upper()
        if upper.startswith("DECISION:"):
            if "SKIP" in upper.split(":", 1)[1]:
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


def ai_write(title: str, content: str, source: str) -> str:
    prompt = f"""Ты редактор новостного Telegram-канала на русском языке.

Источник: {source}
Заголовок: {title}
Текст статьи: {content[:3000]}

Напиши пост для Telegram. Строгие правила стиля:

1. Первый символ — эмодзи категории:
   🌍 политика/международные отношения
   ⚔️ войны/конфликты/безопасность
   💻 технологии/IT/AI
   💰 экономика/финансы/бизнес
   🔬 наука/медицина/космос
   🎭 общество/культура/спорт/происшествия

2. Структура: 2–3 предложения.
   • Первое — главный факт: кто, что сделал/произошло.
   • Второе — контекст или последствие.
   • Третье (если есть важная деталь) — цифра, имя, дата.

3. Стиль:
   • Активный залог: «США ввели санкции» не «санкции были введены»
   • Конкретика: имена, цифры, страны из текста
   • Живой язык, не канцелярский
   • Для английских источников — адаптируй на русский, не переводи дословно

4. Запрещено:
   • Добавлять факты которых нет в тексте
   • «Стало известно», «по имеющимся данным», «в ходе», «пресс-служба сообщает»
   • Упоминать название источника
   • Заголовок как первое предложение

Пост (только текст, никаких пояснений):"""

    return _call(prompt, max_tokens=3000, temp=0.4,
                 thinking_budget=2048, model="gemini-2.5-pro")


# ── Цветной вывод ─────────────────────────────────────────────────────────────

def c(text, code): return f"\033[{code}m{text}\033[0m"
GREEN  = lambda t: c(t, "32")
RED    = lambda t: c(t, "31")
YELLOW = lambda t: c(t, "33")
BOLD   = lambda t: c(t, "1")
GRAY   = lambda t: c(t, "90")
CYAN   = lambda t: c(t, "36")


# ── Главная функция ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=15, help="Сколько статей проверить (default: 15)")
    parser.add_argument("--write-all", action="store_true", help="Писать посты для всех PASS")
    args = parser.parse_args()

    print(BOLD(f"\n{'='*60}"))
    print(BOLD("  Тест качества постов RSS Bot"))
    print(BOLD(f"{'='*60}\n"))

    cat_label = FRESHRSS_CATEGORY or "все категории"
    print(f"Получаю {args.count} статей из FreshRSS [{cat_label}] | MIN_SCORE={MIN_SCORE}...")
    items = fetch_articles(args.count)
    print(f"Получено: {BOLD(str(len(items)))} статей\n")

    results = []
    pass_count = skip_count = blocked_count = 0

    for i, item in enumerate(items, 1):
        raw_title = item.get("title", "Без заголовка")
        title  = clean_title(raw_title)
        source = item.get("origin", {}).get("title", "Unknown")
        raw_html = item.get("summary", {}).get("content", "") or \
                   "".join(c.get("content", "") for c in item.get("content", []))
        content = strip_html(raw_html)

        print(f"[{i:02d}/{len(items)}] {GRAY(source)}")

        # Blacklist источника
        if is_blacklisted(source):
            blocked_count += 1
            print(f"  {RED('⛔ BLACKLIST (источник)')}\n")
            results.append({"decision": "blacklist", "title": title, "source": source})
            continue

        # Keyword blacklist
        if is_keyword_blocked(title):
            blocked_count += 1
            print(f"  {RED('🔤 BLACKLIST (ключевое слово)')}: {title[:70]}\n")
            results.append({"decision": "blacklist", "title": title, "source": source})
            continue

        if raw_title != title:
            print(f"  Заголовок: {GRAY(raw_title[:80])}")
            print(f"  Очищен:   {YELLOW(title[:80])}")
        else:
            print(f"  Заголовок: {title[:80]}")

        # AI фильтр
        decision, score, tags = ai_filter(title, content, source)
        time.sleep(4)

        if decision == "skip":
            skip_count += 1
            print(f"  {RED('→ SKIP')} (реклама/мусор)\n")
            results.append({"decision": "skip", "title": title, "source": source})
            continue

        if score < MIN_SCORE:
            skip_count += 1
            print(f"  {GRAY(f'→ LOW [{score}/10] (ниже MIN_SCORE={MIN_SCORE})')}\n")
            results.append({"decision": "skip", "title": title, "source": source, "score": score})
            continue

        # PASS
        pass_count += 1
        score_color = GREEN if score >= 7 else YELLOW
        tags_str = ", ".join(tags) if tags else "—"
        print(f"  {GREEN('→ PASS')}  оценка: {score_color(str(score) + '/10')}  {GRAY('теги: ' + tags_str)}")

        write_this = args.write_all or score >= MIN_SCORE
        if write_this:
            post = ai_write(title, content, source)
            time.sleep(4)
            print(f"  {CYAN('Пост:')}")
            for line in post.split("\n"):
                print(f"    {line}")
            results.append({"decision": "pass", "score": score, "title": title,
                            "source": source, "post": post})
        else:
            results.append({"decision": "pass", "score": score, "title": title,
                            "source": source, "post": None})
        print()

    # ── Итог ──────────────────────────────────────────────────────────────────
    print(BOLD(f"{'='*60}"))
    print(BOLD("  Итоги"))
    print(f"{'='*60}")
    total = len(items)
    print(f"  Всего статей:  {total}")
    print(f"  {GREEN('PASS')}:          {pass_count} ({100*pass_count//total if total else 0}%)")
    print(f"  {RED('SKIP')}:          {skip_count} ({100*skip_count//total if total else 0}%)")
    if blocked_count:
        print(f"  {RED('BLACKLIST')}:     {blocked_count}")

    passes = [r for r in results if r["decision"] == "pass"]
    if passes:
        scores = [r["score"] for r in passes]
        print(f"\n  Оценки PASS:   min={min(scores)} | avg={sum(scores)/len(scores):.1f} | max={max(scores)}")
        print(f"\n  Топ по оценке:")
        for r in sorted(passes, key=lambda x: x["score"], reverse=True)[:5]:
            post_preview = (r.get("post") or "")[:60].replace("\n", " ")
            print(f"    [{r['score']}/10] {r['source'][:20]:20s} — {post_preview}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
