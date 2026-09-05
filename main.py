import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET

import httpx
from fastapi import FastAPI, Request, Response, HTTPException

# ----------------------------
# Configuration
# ----------------------------

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
CHANNELS_JSON = os.getenv("CHANNELS_JSON", "[]")
PORT = int(os.getenv("PORT", "8000"))

# 待機枠が存在するときだけ、この間隔で videos.list を確認する
UPCOMING_POLL_SECONDS = int(os.getenv("UPCOMING_POLL_SECONDS", "60"))

# WebSub購読を定期的に更新する間隔
RESUBSCRIBE_SECONDS = int(os.getenv("RESUBSCRIBE_SECONDS", "43200"))  # 12h

# RSS安全巡回。WebSubがhealthyでも常時巡回する。
# RSS取得そのものではYouTube Data API quotaを消費しない。
RSS_FALLBACK_SECONDS = int(os.getenv("RSS_FALLBACK_SECONDS", "60"))  # 60秒

# WebSub障害中の再購読間隔
WEBSUB_RECOVERY_SECONDS = int(os.getenv("WEBSUB_RECOVERY_SECONDS", "300"))  # 5分

HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
YOUTUBE_API = "https://www.googleapis.com/youtube/v3/videos"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("youtube-websub")

try:
    CHANNELS: List[dict] = json.loads(CHANNELS_JSON)
except json.JSONDecodeError as e:
    raise RuntimeError(f"CHANNELS_JSON is invalid JSON: {e}")

CHANNEL_MAP: Dict[str, dict] = {
    item["channel_id"]: item
    for item in CHANNELS
    if item.get("channel_id") and item.get("webhook")
}

if not YOUTUBE_API_KEY:
    log.warning("YOUTUBE_API_KEY is not set")
if not PUBLIC_BASE_URL:
    log.warning("PUBLIC_BASE_URL is not set")
if not CHANNEL_MAP:
    log.warning("No valid channels are configured")

DB_PATH = os.getenv("STATE_DB", "state.db")

# WebSubの購読確認状態。
# 起動時は未確認なのでFalse。HubからVERIFYが来たチャンネルだけTrueになる。
WEBSUB_HEALTH: Dict[str, bool] = {channel_id: False for channel_id in CHANNEL_MAP}
WEBSUB_STATE_LOCK = asyncio.Lock()


async def set_websub_health(channel_id: str, healthy: bool, reason: str):
    async with WEBSUB_STATE_LOCK:
        old = WEBSUB_HEALTH.get(channel_id)
        WEBSUB_HEALTH[channel_id] = healthy

    if old != healthy:
        if healthy:
            log.info(
                "WEBSUB HEALTHY channel=%s reason=%s; RSS SAFETY CHECK remains active",
                channel_id,
                reason,
            )
        else:
            log.warning(
                "WEBSUB DEGRADED channel=%s reason=%s; RSS SAFETY CHECK remains active",
                channel_id,
                reason,
            )


def unhealthy_channels() -> List[str]:
    return [cid for cid, healthy in WEBSUB_HEALTH.items() if not healthy]


# ----------------------------
# SQLite state
# ----------------------------

def db_connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    with db_connect() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            title TEXT,
            last_state TEXT,
            waiting_notified INTEGER NOT NULL DEFAULT 0,
            live_notified INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """)

        # RSSで既にAPI確認したVideo ID。
        # 一度確認済みなら、次回以降のRSS巡回ではAPIへ投げない。
        con.execute("""
        CREATE TABLE IF NOT EXISTS rss_checked (
            video_id TEXT PRIMARY KEY,
            checked_at TEXT NOT NULL,
            result TEXT
        )
        """)
        con.commit()


def upsert_video(video_id: str, channel_id: str, title: str, state: str):
    now = datetime.now(timezone.utc).isoformat()
    with db_connect() as con:
        con.execute("""
        INSERT INTO videos(video_id, channel_id, title, last_state, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            title=excluded.title,
            last_state=excluded.last_state,
            updated_at=excluded.updated_at
        """, (video_id, channel_id, title, state, now))
        con.commit()


def get_video_state(video_id: str):
    with db_connect() as con:
        return con.execute(
            "SELECT * FROM videos WHERE video_id=?",
            (video_id,)
        ).fetchone()


def set_notified(video_id: str, kind: str):
    col = "waiting_notified" if kind == "upcoming" else "live_notified"
    with db_connect() as con:
        con.execute(
            f"UPDATE videos SET {col}=1, updated_at=? WHERE video_id=?",
            (datetime.now(timezone.utc).isoformat(), video_id)
        )
        con.commit()


def get_upcoming_video_ids() -> List[str]:
    with db_connect() as con:
        rows = con.execute("""
            SELECT video_id
            FROM videos
            WHERE last_state='upcoming' AND live_notified=0
        """).fetchall()
        return [r["video_id"] for r in rows]


def get_rss_checked_ids(video_ids: List[str]) -> set:
    if not video_ids:
        return set()

    placeholders = ",".join("?" for _ in video_ids)
    with db_connect() as con:
        rows = con.execute(
            f"SELECT video_id FROM rss_checked WHERE video_id IN ({placeholders})",
            video_ids,
        ).fetchall()

    return {r["video_id"] for r in rows}


def mark_rss_checked(video_id: str, result: str):
    with db_connect() as con:
        con.execute("""
            INSERT INTO rss_checked(video_id, checked_at, result)
            VALUES (?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                checked_at=excluded.checked_at,
                result=excluded.result
        """, (
            video_id,
            datetime.now(timezone.utc).isoformat(),
            result,
        ))
        con.commit()


# ----------------------------
# YouTube / Discord
# ----------------------------

async def youtube_videos(video_ids: List[str]) -> Dict[str, dict]:
    """
    videos.list は最大50件までVideo IDをまとめて確認できる。
    50件を超えた場合だけ複数リクエストへ分割する。
    """
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is not set")

    unique_ids = list(dict.fromkeys(video_ids))
    results: Dict[str, dict] = {}

    if not unique_ids:
        return results

    async with httpx.AsyncClient(timeout=20) as client:
        for offset in range(0, len(unique_ids), 50):
            batch = unique_ids[offset:offset + 50]

            params = {
                "part": "snippet,status,liveStreamingDetails",
                "id": ",".join(batch),
                "key": YOUTUBE_API_KEY,
            }

            log.info(
                "API BATCH videos.list requested=%d batch=%d",
                len(unique_ids),
                len(batch),
            )

            r = await client.get(YOUTUBE_API, params=params)
            r.raise_for_status()

            for item in r.json().get("items", []):
                if item.get("id"):
                    results[item["id"]] = item

    return results


async def youtube_video(video_id: str) -> Optional[dict]:
    items = await youtube_videos([video_id])
    return items.get(video_id)


def classify_live(video: dict) -> str:
    live = video.get("liveStreamingDetails")
    if not live:
        return "not-live"

    if live.get("actualEndTime"):
        return "ended"
    if live.get("actualStartTime"):
        return "live"
    if live.get("scheduledStartTime"):
        return "upcoming"

    return "live-resource"


async def discord_notify(channel_cfg: dict, video: dict, state: str):
    snippet = video.get("snippet", {})
    live = video.get("liveStreamingDetails", {})
    title = snippet.get("title", "(no title)")
    video_id = video["id"]
    url = f"https://www.youtube.com/watch?v={video_id}"
    mention = channel_cfg.get("mention", "").strip()

    if state == "upcoming":
        start = live.get("scheduledStartTime")
        content = (
            f"{mention}\n"
            f"📅 **YouTube Live 待機枠**\n"
            f"**{title}**\n"
            f"開始予定: {start or '未定'}\n"
            f"{url}"
        ).strip()
    else:
        content = (
            f"{mention}\n"
            f"🔴 **YouTube Live 配信開始**\n"
            f"**{title}**\n"
            f"{url}"
        ).strip()

    payload = {
        "content": content,
        "allowed_mentions": {"parse": ["everyone"]}
        if mention in ("@here", "@everyone")
        else {"parse": []},
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(channel_cfg["webhook"], json=payload)
        r.raise_for_status()

    log.info("DISCORD state=%s id=%s title=%r", state, video_id, title)


async def process_video_data(video: dict, source: str):
    video_id = video.get("id", "")
    if not video_id:
        return "missing-id"

    snippet = video.get("snippet", {})
    status = video.get("status", {})
    channel_id = snippet.get("channelId", "")
    title = snippet.get("title", "")
    privacy = status.get("privacyStatus", "unknown")
    live_state = classify_live(video)

    log.info(
        "VIDEO source=%s id=%s channel=%s privacy=%s live=%s title=%r",
        source, video_id, channel_id, privacy, live_state, title
    )

    cfg = CHANNEL_MAP.get(channel_id)
    if not cfg:
        log.info("SKIP id=%s reason=channel-not-configured", video_id)
        return "channel-not-configured"

    # 限定公開・非公開を通知しないための安全弁
    if privacy != "public":
        log.info("BLOCK id=%s reason=privacy:%s", video_id, privacy)
        return f"privacy:{privacy}"

    if live_state == "not-live":
        log.info("SKIP id=%s reason=not-live", video_id)
        return "not-live"

    upsert_video(video_id, channel_id, title, live_state)
    saved = get_video_state(video_id)

    if live_state == "upcoming":
        if saved and not saved["waiting_notified"]:
            await discord_notify(cfg, video, "upcoming")
            set_notified(video_id, "upcoming")
        else:
            log.info("SKIP id=%s reason=upcoming-already-notified", video_id)
        return "upcoming"

    elif live_state == "live":
        if saved and not saved["live_notified"]:
            await discord_notify(cfg, video, "live")
            set_notified(video_id, "live")
        else:
            log.info("SKIP id=%s reason=live-already-notified", video_id)
        return "live"

    elif live_state == "ended":
        log.info("SKIP id=%s reason=live-ended", video_id)
        return "ended"

    return live_state


async def process_video(video_id: str, source: str):
    video = await youtube_video(video_id)
    if not video:
        log.warning("MISSING id=%s source=%s not-returned-by-api", video_id, source)
        return "missing"

    return await process_video_data(video, source)


# ----------------------------
# WebSub / RSS
# ----------------------------

def topic_url(channel_id: str) -> str:
    return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


async def subscribe_channel(channel_id: str):
    if not PUBLIC_BASE_URL:
        raise RuntimeError("PUBLIC_BASE_URL is not set")

    callback = f"{PUBLIC_BASE_URL}/youtube/websub"
    form = {
        "hub.mode": "subscribe",
        "hub.topic": topic_url(channel_id),
        "hub.callback": callback,
        "hub.verify": "async",
    }

    timeout = httpx.Timeout(
        connect=20.0,
        read=90.0,
        write=20.0,
        pool=20.0,
    )

    retry_delays = [0, 10, 30]
    last_error = None

    for attempt, delay in enumerate(retry_delays, start=1):
        if delay:
            log.info(
                "SUBSCRIBE retry-wait channel=%s attempt=%d wait=%ss",
                channel_id,
                attempt,
                delay,
            )
            await asyncio.sleep(delay)

        log.info(
            "SUBSCRIBE attempt channel=%s attempt=%d callback=%s",
            channel_id,
            attempt,
            callback,
        )

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(HUB_URL, data=form)

            if r.status_code in (202, 204):
                log.info(
                    "SUBSCRIBE accepted channel=%s status=%s attempt=%d",
                    channel_id,
                    r.status_code,
                    attempt,
                )
                return

            last_error = RuntimeError(
                f"Hub rejected request status={r.status_code} "
                f"body={r.text[:300]!r}"
            )

            log.warning(
                "SUBSCRIBE rejected channel=%s status=%s attempt=%d body=%r",
                channel_id,
                r.status_code,
                attempt,
                r.text[:300],
            )

        except httpx.ReadTimeout as e:
            last_error = e
            log.warning(
                "SUBSCRIBE read-timeout channel=%s attempt=%d",
                channel_id,
                attempt,
            )

        except httpx.ConnectTimeout as e:
            last_error = e
            log.warning(
                "SUBSCRIBE connect-timeout channel=%s attempt=%d",
                channel_id,
                attempt,
            )

        except httpx.HTTPError as e:
            last_error = e
            log.warning(
                "SUBSCRIBE http-error channel=%s attempt=%d error=%r",
                channel_id,
                attempt,
                e,
            )

    raise RuntimeError(
        f"WebSub subscribe failed after {len(retry_delays)} attempts "
        f"channel={channel_id}: {last_error!r}"
    )


async def subscribe_all():
    """
    全チャンネルへ購読要求を送る。
    HTTP受付成功だけでは健全扱いにせず、
    HubからVERIFYが来た時点でhealthy=Trueにする。
    """
    all_ok = True

    for channel_id in CHANNEL_MAP:
        try:
            await subscribe_channel(channel_id)
            log.info(
                "SUBSCRIBE request accepted; waiting VERIFY channel=%s",
                channel_id,
            )
        except Exception as e:
            all_ok = False
            await set_websub_health(
                channel_id,
                False,
                f"subscribe-failed:{type(e).__name__}",
            )
            log.exception("SUBSCRIBE ERROR channel=%s", channel_id)

    return all_ok


def parse_atom(body: bytes):
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }

    root = ET.fromstring(body)
    events = []

    for entry in root.findall("atom:entry", ns):
        video_id = entry.findtext("yt:videoId", default="", namespaces=ns)
        channel_id = entry.findtext("yt:channelId", default="", namespaces=ns)
        title = entry.findtext("atom:title", default="", namespaces=ns)
        if video_id:
            events.append({
                "video_id": video_id,
                "channel_id": channel_id,
                "title": title,
            })

    return events


async def fetch_channel_rss(channel_id: str) -> List[str]:
    """
    YouTubeチャンネルRSSから最近のVideo IDを取得する。
    WebSubの健康状態に関係なく安全網として常時利用する。
    """
    url = topic_url(channel_id)
    timeout = httpx.Timeout(
        connect=15.0,
        read=30.0,
        write=15.0,
        pool=15.0,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        body = r.content

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        raise RuntimeError(
            f"RSS XML parse failed channel={channel_id}: {e}"
        ) from e

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }

    ids = []
    for entry in root.findall("atom:entry", ns):
        video_id = entry.findtext(
            "yt:videoId",
            default="",
            namespaces=ns,
        ).strip()
        if video_id and video_id not in ids:
            ids.append(video_id)

    return ids


async def rss_fallback_monitor():
    """
    WebSubの健康状態に関係なく、全監視チャンネルを60秒ごとにRSS巡回する。

    RSSに出ているVideo IDが既知なら Data API は呼ばない。
    新しいVideo IDを見つけた場合だけ videos.list で公開状態・LIVE状態を確認する。
    """
    while True:
        channels = list(CHANNEL_MAP.keys())

        log.info(
            "RSS SAFETY CHECK channels=%d interval=%ss websub_healthy=%d/%d",
            len(channels),
            RSS_FALLBACK_SECONDS,
            sum(1 for healthy in WEBSUB_HEALTH.values() if healthy),
            len(WEBSUB_HEALTH),
        )

        for channel_id in channels:
            try:
                ids = await fetch_channel_rss(channel_id)

                checked_ids = get_rss_checked_ids(ids)
                new_ids = [
                    video_id
                    for video_id in ids
                    if video_id not in checked_ids
                ]

                log.info(
                    "RSS SAFETY channel=%s entries=%d checked=%d new=%d websub=%s",
                    channel_id,
                    len(ids),
                    len(checked_ids),
                    len(new_ids),
                    "healthy" if WEBSUB_HEALTH.get(channel_id) else "degraded",
                )

                if not new_ids:
                    log.info(
                        "RSS SAFETY channel=%s no-new-video-ids -> API SKIP",
                        channel_id,
                    )
                    continue

                log.info(
                    "RSS SAFETY channel=%s new-video-ids=%s -> API CHECK",
                    channel_id,
                    ",".join(new_ids),
                )

                # 最大50件をまとめて1回の videos.list へ。
                videos = await youtube_videos(new_ids)

                for video_id in new_ids:
                    video = videos.get(video_id)

                    if not video:
                        log.warning(
                            "MISSING id=%s source=rss-safety not-returned-by-api",
                            video_id,
                        )
                        mark_rss_checked(video_id, "missing")
                        continue

                    try:
                        result = await process_video_data(
                            video,
                            "rss-safety",
                        )
                        mark_rss_checked(
                            video_id,
                            result or "processed",
                        )
                    except Exception:
                        # Discord送信など後段が一時失敗した場合は、
                        # checkedに入れず次回RSSで再試行する。
                        log.exception(
                            "RSS VIDEO ERROR channel=%s id=%s",
                            channel_id,
                            video_id,
                        )

            except Exception:
                log.exception(
                    "RSS SAFETY ERROR channel=%s",
                    channel_id,
                )

        await asyncio.sleep(RSS_FALLBACK_SECONDS)


# ----------------------------
# Background workers
# ----------------------------

async def upcoming_monitor():
    while True:
        try:
            ids = get_upcoming_video_ids()
            if ids:
                log.info("UPCOMING-POLL count=%d", len(ids))
            for video_id in ids:
                try:
                    await process_video(video_id, "upcoming-poll")
                except Exception:
                    log.exception("UPCOMING ERROR id=%s", video_id)
        except Exception:
            log.exception("UPCOMING MONITOR ERROR")

        await asyncio.sleep(UPCOMING_POLL_SECONDS)


async def resubscribe_loop():
    # 起動直後に購読要求。
    await subscribe_all()

    while True:
        degraded = unhealthy_channels()

        if degraded:
            wait_seconds = WEBSUB_RECOVERY_SECONDS
            log.warning(
                "WEBSUB RECOVERY scheduled channels=%d retry_in=%ss",
                len(degraded),
                wait_seconds,
            )
        else:
            wait_seconds = RESUBSCRIBE_SECONDS
            log.info(
                "WEBSUB healthy; renewal_in=%ss",
                wait_seconds,
            )

        await asyncio.sleep(wait_seconds)

        if unhealthy_channels():
            log.info("WEBSUB RECOVERY attempt starting")
        else:
            log.info("RESUBSCRIBE renewal starting")

        await subscribe_all()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    tasks = [
        asyncio.create_task(upcoming_monitor()),
        asyncio.create_task(resubscribe_loop()),
        asyncio.create_task(rss_fallback_monitor()),
    ]

    log.info(
        "START channels=%d upcoming_poll=%ss rss_safety=%ss "
        "websub_recovery=%ss public_base=%s",
        len(CHANNEL_MAP),
        UPCOMING_POLL_SECONDS,
        RSS_FALLBACK_SECONDS,
        WEBSUB_RECOVERY_SECONDS,
        PUBLIC_BASE_URL or "(unset)",
    )

    yield

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(
    title="YouTube Live WebSub Notifier",
    lifespan=lifespan,
)


# ----------------------------
# HTTP endpoints
# ----------------------------

@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "youtube-live-websub-notifier",
        "channels": len(CHANNEL_MAP),
        "upcoming_poll_seconds": UPCOMING_POLL_SECONDS,
        "rss_safety_seconds": RSS_FALLBACK_SECONDS,
        "rss_safety_always_on": True,
        "websub_recovery_seconds": WEBSUB_RECOVERY_SECONDS,
        "websub_health": WEBSUB_HEALTH,
        "rss_checked_cache": True,
        "videos_list_batching": True,
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "websub_health": WEBSUB_HEALTH,
        "rss_safety_always_on": True,
        "rss_safety_seconds": RSS_FALLBACK_SECONDS,
    }


@app.get("/youtube/websub")
async def websub_verify(request: Request):
    """
    Google Hubからの購読確認。
    hub.challenge をそのまま返す。
    """
    q = request.query_params
    mode = q.get("hub.mode")
    topic = q.get("hub.topic")
    challenge = q.get("hub.challenge")

    log.info(
        "VERIFY mode=%s topic=%s lease=%s",
        mode,
        topic,
        q.get("hub.lease_seconds"),
    )

    if mode not in ("subscribe", "unsubscribe") or not challenge:
        raise HTTPException(
            status_code=400,
            detail="invalid verification request",
        )

    allowed_topics = {topic_url(cid) for cid in CHANNEL_MAP}
    if topic and topic not in allowed_topics:
        log.warning(
            "VERIFY rejected unknown topic=%s",
            topic,
        )
        raise HTTPException(
            status_code=404,
            detail="unknown topic",
        )

    if mode == "subscribe" and topic:
        verified_channel = next(
            (
                cid
                for cid in CHANNEL_MAP
                if topic_url(cid) == topic
            ),
            None,
        )
        if verified_channel:
            await set_websub_health(
                verified_channel,
                True,
                "hub-verify",
            )

    elif mode == "unsubscribe" and topic:
        unsubscribed_channel = next(
            (
                cid
                for cid in CHANNEL_MAP
                if topic_url(cid) == topic
            ),
            None,
        )
        if unsubscribed_channel:
            await set_websub_health(
                unsubscribed_channel,
                False,
                "hub-unsubscribe",
            )

    return Response(
        content=challenge,
        media_type="text/plain",
    )


@app.post("/youtube/websub")
async def websub_notification(request: Request):
    """
    YouTube WebSub Atom通知を受信。
    受信したVideo IDだけData APIで確認する。
    """
    body = await request.body()

    try:
        events = parse_atom(body)
    except ET.ParseError:
        log.exception("PUSH invalid XML")
        return Response(status_code=204)

    log.info("PUSH entries=%d", len(events))

    for event in events:
        video_id = event["video_id"]
        channel_id = event["channel_id"]

        log.info(
            "PUSH EVENT id=%s channel=%s title=%r",
            video_id,
            channel_id,
            event["title"],
        )

        if channel_id in CHANNEL_MAP:
            await set_websub_health(
                channel_id,
                True,
                "push-received",
            )

        asyncio.create_task(
            process_video(video_id, "websub")
        )

    return Response(status_code=204)


@app.post("/admin/subscribe")
async def admin_subscribe():
    """
    手動で全チャンネル再購読。
    """
    await subscribe_all()
    return {
        "ok": True,
        "channels": list(CHANNEL_MAP.keys()),
    }
