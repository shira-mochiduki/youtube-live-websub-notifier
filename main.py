import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlencode
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


# ----------------------------
# YouTube / Discord
# ----------------------------

async def youtube_video(video_id: str) -> Optional[dict]:
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is not set")

    params = {
        "part": "snippet,status,liveStreamingDetails",
        "id": video_id,
        "key": YOUTUBE_API_KEY,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(YOUTUBE_API, params=params)
        r.raise_for_status()
        items = r.json().get("items", [])
        return items[0] if items else None


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
        "allowed_mentions": {"parse": ["everyone"]} if mention in ("@here", "@everyone") else {"parse": []},
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(channel_cfg["webhook"], json=payload)
        r.raise_for_status()

    log.info("DISCORD state=%s id=%s title=%r", state, video_id, title)


async def process_video(video_id: str, source: str):
    video = await youtube_video(video_id)
    if not video:
        log.warning("MISSING id=%s source=%s not-returned-by-api", video_id, source)
        return

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
        return

    # 最重要の安全弁
    if privacy != "public":
        log.info("BLOCK id=%s reason=privacy:%s", video_id, privacy)
        return

    if live_state == "not-live":
        log.info("SKIP id=%s reason=not-live", video_id)
        return

    upsert_video(video_id, channel_id, title, live_state)
    saved = get_video_state(video_id)

    if live_state == "upcoming":
        if saved and not saved["waiting_notified"]:
            await discord_notify(cfg, video, "upcoming")
            set_notified(video_id, "upcoming")
        else:
            log.info("SKIP id=%s reason=upcoming-already-notified", video_id)

    elif live_state == "live":
        if saved and not saved["live_notified"]:
            await discord_notify(cfg, video, "live")
            set_notified(video_id, "live")
        else:
            log.info("SKIP id=%s reason=live-already-notified", video_id)

    elif live_state == "ended":
        log.info("SKIP id=%s reason=live-ended", video_id)


# ----------------------------
# WebSub
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

    # Google Hubの応答が遅い場合に備え、
    # 購読処理だけ読み取りタイムアウトを長めにする。
    timeout = httpx.Timeout(
        connect=20.0,
        read=90.0,
        write=20.0,
        pool=20.0,
    )

    # 初回 + 10秒後 + 30秒後の最大3回。
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
    for channel_id in CHANNEL_MAP:
        try:
            await subscribe_channel(channel_id)
        except Exception:
            log.exception("SUBSCRIBE ERROR channel=%s", channel_id)


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
    # 起動直後にも購読要求
    await subscribe_all()

    while True:
        await asyncio.sleep(RESUBSCRIBE_SECONDS)
        log.info("RESUBSCRIBE starting")
        await subscribe_all()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    tasks = [
        asyncio.create_task(upcoming_monitor()),
        asyncio.create_task(resubscribe_loop()),
    ]

    log.info(
        "START channels=%d upcoming_poll=%ss public_base=%s",
        len(CHANNEL_MAP), UPCOMING_POLL_SECONDS, PUBLIC_BASE_URL or "(unset)"
    )

    yield

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="YouTube Live WebSub Notifier", lifespan=lifespan)


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
    }


@app.get("/health")
async def health():
    return {"ok": True}


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
        mode, topic, q.get("hub.lease_seconds")
    )

    if mode not in ("subscribe", "unsubscribe") or not challenge:
        raise HTTPException(status_code=400, detail="invalid verification request")

    # 自分が監視対象にしているトピックだけ受け付ける
    allowed_topics = {topic_url(cid) for cid in CHANNEL_MAP}
    if topic and topic not in allowed_topics:
        log.warning("VERIFY rejected unknown topic=%s", topic)
        raise HTTPException(status_code=404, detail="unknown topic")

    return Response(content=challenge, media_type="text/plain")


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
        # 再送ループを避けるため、受信自体は2xxで返す
        return Response(status_code=204)

    log.info("PUSH entries=%d", len(events))

    # Hubには早く2xxを返したいのでバックグラウンド処理
    for event in events:
        video_id = event["video_id"]
        log.info(
            "PUSH EVENT id=%s channel=%s title=%r",
            video_id, event["channel_id"], event["title"]
        )
        asyncio.create_task(process_video(video_id, "websub"))

    return Response(status_code=204)


@app.post("/admin/subscribe")
async def admin_subscribe():
    """
    手動で全チャンネル再購読。
    公開運用では必要に応じて認証を追加してください。
    """
    await subscribe_all()
    return {"ok": True, "channels": list(CHANNEL_MAP.keys())}
