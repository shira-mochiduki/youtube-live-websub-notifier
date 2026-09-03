# YouTube Live WebSub Notifier

YouTubeチャンネル全体を5分ごとに巡回せず、YouTube公式のWebSub（PubSubHubbub）プッシュ通知を入口にするDiscord通知プログラムです。

## 設計

1. YouTube WebSubから「動画が追加・更新された」通知を受信
2. 通知された `videoId` だけ YouTube Data API `videos.list` で確認
3. `privacyStatus != public` は必ず遮断
4. 通常動画 / Shorts は `liveStreamingDetails` が無いので通知しない
5. 公開LIVE待機枠ならDiscordへ待機枠通知
6. 待機枠として検出したVideo IDだけを短時間ポーリング
7. `actualStartTime` が入ったら配信開始通知
8. 配信開始後はポーリング対象から外れる

つまり「常時チャンネル巡回」ではなく、
**プッシュ通知 + 待機枠だけ監視** のハイブリッドです。

## なぜ完全プッシュだけにしない？

YouTube公式WebSubの通知対象は、動画のアップロードやタイトル・説明の更新です。
「scheduled → live」の状態変化だけで必ずWebSub通知が来るとは限らないため、
待機枠を検出した後だけ `videos.list` で開始状態を確認します。

## 必要な環境変数

### YOUTUBE_API_KEY

YouTube Data API v3のAPIキー。

### PUBLIC_BASE_URL

外部からアクセス可能なHTTPS URL。

Render例:

```text
https://youtube-live-websub-notifier.onrender.com
```

`localhost` はGoogleのHubから到達できないため、本番のWebSub受信には使えません。

### CHANNELS_JSON

例:

```json
[
  {
    "channel_id": "UCxxxxxxxxxxxxxxxxxxxxxx",
    "webhook": "https://discord.com/api/webhooks/...",
    "mention": "@here"
  }
]
```

### UPCOMING_POLL_SECONDS

待機枠が存在するときだけ確認する間隔。

デフォルト:

```text
60
```

### RESUBSCRIBE_SECONDS

WebSub購読を再要求する間隔。デフォルト12時間。

```text
43200
```

## ローカル起動

PowerShell:

```powershell
$env:YOUTUBE_API_KEY="..."
$env:PUBLIC_BASE_URL="https://外部公開URL"
$env:CHANNELS_JSON='[{"channel_id":"UC...","webhook":"https://discord.com/api/webhooks/...","mention":"@here"}]'
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

ただしWebSubのコールバックはインターネットから到達できる必要があります。

## Render

Web Serviceとして作成。

Build Command:

```text
pip install -r requirements.txt
```

Start Command:

```text
uvicorn main:app --host 0.0.0.0 --port $PORT
```

環境変数:

```text
YOUTUBE_API_KEY
PUBLIC_BASE_URL
CHANNELS_JSON
UPCOMING_POLL_SECONDS=60
```

デプロイ後、起動時に自動でWebSub購読要求を送ります。

成功するとログ例:

```text
SUBSCRIBE requested channel=UC...
VERIFY mode=subscribe topic=https://www.youtube.com/feeds/videos.xml?channel_id=UC...
```

YouTube側で更新が発生すると:

```text
PUSH entries=1
PUSH EVENT id=xxxx channel=UC... title='配信タイトル'
VIDEO source=websub id=xxxx privacy=public live=upcoming ...
DISCORD state=upcoming ...
```

限定公開を受信・API確認した場合:

```text
VIDEO source=websub id=xxxx privacy=unlisted live=upcoming ...
BLOCK id=xxxx reason=privacy:unlisted
```

通常動画:

```text
VIDEO source=websub id=xxxx privacy=public live=not-live ...
SKIP id=xxxx reason=not-live
```

待機枠が存在する間だけ:

```text
UPCOMING-POLL count=1
VIDEO source=upcoming-poll id=xxxx privacy=public live=upcoming ...
```

配信開始すると:

```text
VIDEO source=upcoming-poll id=xxxx privacy=public live=live ...
DISCORD state=live ...
```

## APIクォータ

`videos.list` は1リクエスト1 unitです。

このプログラムでは:

- 何も起きていないチャンネル → API問い合わせなし
- 通常動画が公開された → 原則1回
- LIVE待機枠が作られた → 検出時1回 + 待機中だけ定期確認
- 配信開始後 → 待機監視終了

となります。

## 注意

`/admin/subscribe` は簡易的な再購読用エンドポイントです。
インターネット公開サービスで厳密に運用するなら管理用シークレット認証を追加してください。

`state.db` はローカルSQLiteです。Renderの再デプロイで永続化したい場合はPersistent Diskや外部DBへ移すのがおすすめです。
