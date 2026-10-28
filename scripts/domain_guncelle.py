#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eklenti_domainleri.txt içindeki her siteyi kontrol eder, taşınmışsa yeni
domaini dosyaya yazar.

Dosya biçimi (satır başına bir site):
    |SiteAdi:https://ornek.com

Özellikler
- Siteler paralel kontrol edilir; tek bir sitedeki hata diğerlerini etkilemez.
- cloudscraper -> requests -> (SSL hatasında) doğrulamasız requests sırasıyla denenir,
  geçici hatalarda tekrar denenir.
- HTTP yönlendirmelerine ek olarak <meta refresh> ve basit JavaScript
  yönlendirmeleri de takip edilir.
- Yeni domain; kara liste, park/reklam sayfası, IP adresi, hata durum kodu
  gibi durumlarda KABUL EDİLMEZ, mevcut domain korunur.
- Site tamamen kapalıysa dizipal1592 -> dizipal1593 gibi sayı ile biten
  domainlerin sonraki numaraları denenir (marka adı sayfada doğrulanır).
- Biçim hataları (https:/site.com gibi) otomatik düzeltilir.
- Toplu/anormal değişiklikte (ör. runner'ın interneti bozuksa) dosyaya yazmaz.
- Yorum satırları, boş satırlar ve satır sonları (LF/CRLF) olduğu gibi korunur.
"""
from __future__ import annotations

import ipaddress
import os
import re
import sys
import tempfile
import threading
import time
import traceback
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

import requests
import urllib3

try:
    import cloudscraper  # type: ignore
except Exception:  # paket yoksa requests ile devam edilir
    cloudscraper = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --------------------------------------------------------------------------
# Ayarlar (ortam değişkeniyle değiştirilebilir)
# --------------------------------------------------------------------------
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


FILE_PATH = os.environ.get("FILE_PATH", "eklenti_domainleri.txt")
BLACKLIST_PATH = os.environ.get("BLACKLIST_PATH", "domain_blacklist.txt")
WORKERS = max(1, _env_int("WORKERS", 8))
CONNECT_TIMEOUT = _env_int("CONNECT_TIMEOUT", 10)
READ_TIMEOUT = _env_int("READ_TIMEOUT", 25)
ROUNDS = max(1, _env_int("ROUNDS", 2))           # her site için deneme turu
SITE_DEADLINE = _env_int("SITE_DEADLINE", 90)    # bir site için en fazla saniye
PROBE_DEADLINE = _env_int("PROBE_DEADLINE", 25)  # komşu domain denemesi için
PROBE_NEIGHBORS = os.environ.get("PROBE_NEIGHBORS", "1") != "0"
NEIGHBOR_SPAN = _env_int("NEIGHBOR_SPAN", 5)
MAX_HOPS = 4
MAX_CHANGE_RATIO = 0.5  # değişen site oranı bunu aşarsa dosyaya yazılmaz
MIN_CHANGE_LIMIT = 10   # ... ama en az bu kadar değişikliğe izin verilir

# Yeni domain olarak asla kabul edilmeyecek park / reklam / arama / sosyal alanlar
PARKING_DOMAINS = {
    "google.com", "google.com.tr", "bing.com", "yahoo.com", "duckduckgo.com",
    "yandex.com", "yandex.com.tr", "facebook.com", "instagram.com", "twitter.com",
    "x.com", "t.me", "telegram.org", "telegram.me", "youtube.com", "tiktok.com",
    "sedoparking.com", "sedo.com", "godaddy.com", "hugedomains.com", "dan.com",
    "afternic.com", "parkingcrew.net", "bodis.com", "namecheap.com", "namesilo.com",
    "porkbun.com", "above.com", "domainmarket.com", "undeveloped.com",
    "squadhelp.com", "wix.com", "github.com", "wikipedia.org", "cloudflare.com",
    "btk.gov.tr", "guvenliweb.org.tr", "guvenlinet.org.tr", "tib.gov.tr",
}

PARKED_WORDS = (
    "domain is for sale", "this domain is for sale", "domain name is for sale",
    "buy this domain", "domain may be for sale", "bu domain satılık",
    "domain satılık", "sedo domain parking", "parked free", "parked domain",
)

# Bot koruması olan ama sitenin yaşadığını gösteren durum kodları
CHALLENGE_STATUS = {401, 403, 429, 503}
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 530}

DEFAULT_PORTS = {"http": 80, "https": 443}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
}

Url = namedtuple("Url", "scheme host port")
Page = namedtuple("Page", "url status text")
Result = namedtuple("Result", "idx name old new kind note")
# kind: ok | changed | fixed | failed | skipped


def log(msg: str = "") -> None:
    print(msg, flush=True)


# --------------------------------------------------------------------------
# URL yardımcıları
# --------------------------------------------------------------------------
def parse_url(raw: str) -> Url | None:
    """'https:/x.com', 'x.com', 'https://x.com:443/yol' gibi girdileri çözer."""
    raw = (raw or "").strip().strip("\"'")
    m = re.match(r"^(https?):/*(.*)$", raw, re.I)
    if m:
        scheme, rest = m.group(1).lower(), m.group(2)
    elif "://" in raw:
        return None
    else:
        scheme, rest = "https", raw.lstrip("/")
    try:
        p = urlparse(f"{scheme}://{rest}")
        host = (p.hostname or "").lower().rstrip(".")
        port = p.port
    except ValueError:
        return None
    if not host or "." not in host or not re.fullmatch(r"[^\s/@:]+", host):
        return None
    return Url(scheme, host, port)


def fmt(u: Url) -> str:
    return f"{u.scheme}://{u.host}" + (f":{u.port}" if u.port else "")


def key(u: Url):
    return (u.scheme, u.host, u.port or DEFAULT_PORTS[u.scheme])


def clean_new(u: Url) -> Url:
    """Yeni domain için varsayılan portu at."""
    port = None if u.port in (None, DEFAULT_PORTS[u.scheme]) else u.port
    return Url(u.scheme, u.host, port)


def strip_www(host: str) -> str:
    return host[4:] if host.startswith("www.") else host


def host_in(host: str, domains) -> bool:
    host = strip_www(host.lower())
    return any(host == d or host.endswith("." + d) for d in domains)


def is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


def load_blacklist(path: str) -> set[str]:
    result: set[str] = set()
    if not os.path.exists(path):
        log(f"Kara liste dosyası yok ({path}), kara listesiz devam ediliyor.")
        return result
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                u = parse_url(line)
                host = strip_www(u.host) if u else strip_www(line.lower().strip("/"))
                if host:
                    result.add(host)
    except Exception as e:  # noqa: BLE001
        log(f"Kara liste okunamadı ({e}), kara listesiz devam ediliyor.")
    return result


# --------------------------------------------------------------------------
# Ağ katmanı
# --------------------------------------------------------------------------
_local = threading.local()


def _scraper():
    sc = getattr(_local, "scraper", None)
    if sc is None:
        try:
            sc = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "desktop": True}
            )
        except Exception:  # noqa: BLE001
            sc = False
        _local.scraper = sc
    return sc


def _request(url: str, strategy: str) -> Page:
    timeout = (CONNECT_TIMEOUT, READ_TIMEOUT)
    if strategy == "scraper":
        sc = _scraper()
        if not sc:
            raise RuntimeError("cloudscraper kullanılamıyor")
        r = sc.get(url, timeout=timeout, allow_redirects=True)
    else:
        r = requests.get(
            url,
            headers=BROWSER_HEADERS,
            timeout=timeout,
            allow_redirects=True,
            verify=(strategy != "insecure"),
        )
    try:
        try:
            text = (r.text or "")[:30000]
        except Exception:  # noqa: BLE001
            text = ""
        return Page(r.url, r.status_code, text)
    finally:
        r.close()


def fetch(url: str, rounds: int | None = None, budget: int | None = None) -> Page:
    """
    URL'yi getirir. Ağ hatasında Exception fırlatır; 4xx/5xx durumlarında
    elindeki en iyi sonucu döndürür.
    """
    rounds = rounds or ROUNDS
    deadline = time.time() + (budget or SITE_DEADLINE)
    soft: Page | None = None
    last_exc: Exception | None = None
    ssl_problem = False

    for rnd in range(rounds):
        strategies = (["scraper"] if cloudscraper else []) + ["requests"]
        if ssl_problem:
            strategies.append("insecure")
        for st in strategies:
            if time.time() > deadline:
                break
            try:
                page = _request(url, st)
            except requests.exceptions.SSLError as e:
                last_exc, ssl_problem = e, True
                continue
            except Exception as e:  # noqa: BLE001
                last_exc = e
                continue
            if page.status < 400:
                return page
            if soft is None:
                soft = page
        if soft is not None and soft.status not in RETRY_STATUS:
            break
        if time.time() > deadline:
            break
        if rnd < rounds - 1:
            time.sleep(2 + 2 * rnd)

    if soft is not None:
        return soft
    raise last_exc or RuntimeError("yanıt alınamadı")


_META_TAG = re.compile(r"<meta\b[^>]*>", re.I)
_META_CONTENT = re.compile(r"content\s*=\s*[\"']?([^\"'>]+)", re.I)
_META_REFRESH = re.compile(r"^\s*(\d+)\s*;\s*url\s*=\s*['\"]?(.+?)['\"]?\s*$", re.I)
_JS_REDIRECT = re.compile(
    r"(?:(?:window|document|top|self)\.)?location(?:\.href)?\s*=\s*[\"']([^\"']+)[\"']"
    r"|location\.(?:replace|assign)\(\s*[\"']([^\"']+)[\"']",
    re.I,
)


def client_redirect(page: Page) -> str | None:
    """<meta refresh> ve küçük sayfalardaki JS yönlendirmelerini bulur."""
    text = page.text or ""
    target = None
    for tag in _META_TAG.findall(text[:10000]):
        if "refresh" not in tag.lower():
            continue
        c = _META_CONTENT.search(tag)
        m = _META_REFRESH.match(c.group(1)) if c else None
        if m and int(m.group(1)) <= 5:
            target = m.group(2)
            break
    if not target and len(text) < 6000:
        m = _JS_REDIRECT.search(text)
        if m:
            target = m.group(1) or m.group(2)
    if not target:
        return None
    target = urljoin(page.url, target.strip())
    if not target.lower().startswith(("http://", "https://")):
        return None
    a, b = parse_url(target), parse_url(page.url)
    if not a or not b or a.host == b.host:
        return None
    return target


def resolve(url: str, rounds: int | None = None, budget: int | None = None) -> Page:
    """Tüm yönlendirmeleri (HTTP + meta + JS) takip edip son sayfayı döndürür."""
    page = fetch(url, rounds, budget)  # ilk istek başarısızsa Exception fırlar
    seen = {url}
    for _ in range(MAX_HOPS - 1):
        nxt = client_redirect(page)
        if not nxt or nxt in seen:
            break
        seen.add(nxt)
        try:
            nxt_page = fetch(nxt, rounds, budget)
        except Exception:  # noqa: BLE001  yeni adres kapalıysa eskisinde kal
            break
        page = nxt_page
    return page


# --------------------------------------------------------------------------
# Karar mantığı
# --------------------------------------------------------------------------
def looks_parked(text: str) -> bool:
    low = (text or "").lower()
    return any(w in low for w in PARKED_WORDS)


def reject_reason(base: Url, final: Url, page: Page, blacklist: set[str]) -> str | None:
    if is_ip(final.host):
        return "IP adresine yönlendi"
    if host_in(final.host, blacklist):
        return f"kara listedeki domaine yönlendi ({final.host})"
    if host_in(final.host, PARKING_DOMAINS):
        return f"park/reklam/sosyal domaine yönlendi ({final.host})"
    if page.status >= 400 and page.status not in CHALLENGE_STATUS:
        return f"yeni domain {page.status} döndürdü ({final.host})"
    if looks_parked(page.text):
        return f"yeni domain park sayfası gibi ({final.host})"
    return None


def evaluate(base: Url, page: Page, blacklist: set[str]):
    """-> (yeni Url, not, domain_değişti_mi)"""
    final = parse_url(page.url)
    if final is None:
        return base, "hedef URL çözümlenemedi", False
    if key(final) == key(base):
        return base, "değişiklik yok", False
    if final.host == base.host:
        # Aynı domain, farklı şema/port: yalnızca http -> https yükseltmesini kabul et
        if base.scheme == "http" and final.scheme == "https":
            return clean_new(final), "http -> https", True
        return base, "değişiklik yok", False
    why = reject_reason(base, final, page, blacklist)
    if why:
        return base, f"atlandı: {why}", False
    return clean_new(final), f"{base.host} -> {final.host}", True


def neighbor_candidates(base: Url) -> list[Url]:
    """dizipal1592 -> dizipal1593 ... gibi sayıyla biten adları üretir."""
    labels = base.host.split(".")
    i = 1 if labels[0] == "www" and len(labels) > 2 else 0
    m = re.fullmatch(r"([a-z][a-z-]{2,})(\d+)", labels[i])
    if not m:
        return []
    prefix, digits = m.group(1), m.group(2)
    if len(digits) > 1 and digits.startswith("0"):
        return []
    n = int(digits)
    out = []
    for k in range(1, NEIGHBOR_SPAN + 1):
        new_labels = list(labels)
        new_labels[i] = f"{prefix}{n + k}"
        out.append(Url(base.scheme, ".".join(new_labels), base.port))
    return out


def probe_neighbors(base: Url, blacklist: set[str]):
    labels = base.host.split(".")
    i = 1 if labels[0] == "www" and len(labels) > 2 else 0
    stem = re.sub(r"\d+$", "", labels[i])
    for cand in neighbor_candidates(base):
        try:
            page = resolve(fmt(cand), rounds=1, budget=PROBE_DEADLINE)
        except Exception:  # noqa: BLE001
            continue
        final = parse_url(page.url)
        if final is None or page.status >= 400:
            continue
        if reject_reason(base, final, page, blacklist):
            continue
        body = (page.text or "").lower().replace(final.host, "").replace(cand.host, "")
        if len(body) < 500 or stem not in body:
            continue
        return clean_new(final), f"{base.host} kapalı; {final.host} bulundu"
    return None


def process(idx: int, name: str, raw: str, blacklist: set[str]) -> Result:
    try:
        base = parse_url(raw)
        if base is None:
            return Result(idx, name, raw, raw, "failed", "URL biçimi tanınmadı, olduğu gibi bırakıldı")
        canon = fmt(base)

        if host_in(base.host, blacklist):
            return Result(idx, name, raw, raw, "skipped", "kara listede, dokunulmadı")

        try:
            page = resolve(canon)
        except Exception as e:  # noqa: BLE001 - site kapalı / erişilemiyor
            err = f"{type(e).__name__}: {str(e)[:120]}"
            if PROBE_NEIGHBORS:
                found = probe_neighbors(base, blacklist)
                if found:
                    new, note = found
                    return Result(idx, name, raw, fmt(new), "changed", note)
            if canon != raw:
                return Result(idx, name, raw, canon, "fixed", f"erişilemedi ({err}); yalnızca biçim düzeltildi")
            return Result(idx, name, raw, raw, "failed", f"erişilemedi, mevcut URL korundu ({err})")

        new, note, changed = evaluate(base, page, blacklist)
        new_str = fmt(new)
        if changed:
            return Result(idx, name, raw, new_str, "changed", note)
        if new_str != raw:
            return Result(idx, name, raw, new_str, "fixed", "biçim düzeltildi")
        kind = "ok"
        if page.status >= 400 and page.status not in CHALLENGE_STATUS:
            kind, note = "failed", f"HTTP {page.status}, mevcut URL korundu"
        elif note.startswith("atlandı"):
            kind = "failed"  # şüpheli yönlendirme: özette görünsün
        return Result(idx, name, raw, raw, kind, note)
    except Exception as e:  # noqa: BLE001 - tek site asla çökertmesin
        return Result(idx, name, raw, raw, "failed", f"beklenmeyen hata: {type(e).__name__}: {e}")


# --------------------------------------------------------------------------
# Dosya işlemleri
# --------------------------------------------------------------------------
LINE_RE = re.compile(r"^(?P<pre>\s*\|\s*)(?P<name>[^:|]+?)(?P<sep>\s*:\s*)(?P<url>\S+)(?P<rest>.*)$")
ICONS = {"ok": "✓", "changed": "🔄", "fixed": "🛠", "failed": "✗", "skipped": "⛔"}


def write_summary(results: list[Result], written: bool, aborted: str | None) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        count = {k: sum(1 for r in results if r.kind == k) for k in ICONS}
        lines = ["## Domain kontrol özeti", ""]
        lines.append(
            f"Toplam **{len(results)}** site — değişen: **{count['changed']}**, "
            f"biçim düzeltilen: **{count['fixed']}**, sorunsuz: **{count['ok']}**, "
            f"erişilemeyen: **{count['failed']}**, kara liste: **{count['skipped']}**"
        )
        if aborted:
            lines += ["", f"> ⚠️ Dosya **yazılmadı**: {aborted}"]
        elif written:
            lines += ["", "Dosya güncellendi."]
        for title, kinds in (("Değişenler", ("changed", "fixed")), ("Erişilemeyenler", ("failed",))):
            rows = [r for r in results if r.kind in kinds]
            if rows:
                lines += ["", f"### {title}", "", "| Site | Eski | Yeni | Not |", "|---|---|---|---|"]
                for r in rows:
                    lines.append(f"| {r.name} | {r.old} | {r.new} | {r.note} |")
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception:  # noqa: BLE001
        pass


def main() -> int:
    if not os.path.exists(FILE_PATH):
        log(f"HATA: dosya bulunamadı: {FILE_PATH}")
        return 1

    with open(FILE_PATH, "r", encoding="utf-8-sig", newline="") as f:
        content = f.read()
    lines = content.splitlines(keepends=True)

    entries = []
    for i, line in enumerate(lines):
        m = LINE_RE.match(line.rstrip("\r\n"))
        if m:
            entries.append((i, m))

    blacklist = load_blacklist(BLACKLIST_PATH)
    log(f"{len(entries)} site kontrol edilecek, kara listede {len(blacklist)} domain var.\n")

    results: list[Result] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [
            pool.submit(process, i, m.group("name").strip(), m.group("url"), blacklist)
            for i, m in entries
        ]
        for n, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            results.append(r)
            log(f"[{n}/{len(entries)}] {ICONS[r.kind]} {r.name}: {r.note}"
                + (f"  ({r.old} -> {r.new})" if r.old != r.new else ""))

    results.sort(key=lambda r: r.idx)
    by_idx = {r.idx: r for r in results}

    changed = [r for r in results if r.kind == "changed"]
    to_write = [r for r in results if r.kind in ("changed", "fixed")]

    # Aynı domaini birden fazla site kullanıyorsa uyar (yazmayı engellemez)
    seen: dict[str, str] = {}
    for r in results:
        u = parse_url(r.new)
        if u:
            other = seen.setdefault(u.host, r.name)
            if other != r.name:
                log(f"UYARI: {r.name} ve {other} aynı domaini gösteriyor: {u.host}")

    aborted = None
    limit = max(MIN_CHANGE_LIMIT, int(len(entries) * MAX_CHANGE_RATIO))
    if len(changed) > limit:
        aborted = (f"{len(changed)} site aynı anda değişmek istedi (sınır {limit}); "
                   "ağ sorunu olabilir, güvenlik için dosya değiştirilmedi.")
        log(f"\nUYARI: {aborted}")

    written = False
    if to_write and not aborted:
        new_lines = list(lines)
        for r in to_write:
            i = r.idx
            m = LINE_RE.match(lines[i].rstrip("\r\n"))
            ending = lines[i][len(lines[i].rstrip("\r\n")):]
            new_lines[i] = f"{m.group('pre')}{m.group('name')}{m.group('sep')}{r.new}{m.group('rest')}{ending}"
        new_content = "".join(new_lines)
        if new_content != content:
            directory = os.path.dirname(os.path.abspath(FILE_PATH))
            fd, tmp = tempfile.mkstemp(dir=directory, prefix=".domain_", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
                    f.write(new_content)
                os.replace(tmp, FILE_PATH)
                written = True
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)

    count = {k: sum(1 for r in results if r.kind == k) for k in ICONS}
    log("\n" + "-" * 60)
    log(f"Özet: değişen={count['changed']} biçim_düzeltilen={count['fixed']} "
        f"sorunsuz={count['ok']} erişilemeyen={count['failed']} kara_liste={count['skipped']}")
    log("✓ Dosya güncellendi!" if written else "→ Dosyada değişiklik yapılmadı.")
    write_summary(results, written, aborted)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)
