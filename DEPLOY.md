# Deploy to Railway

## 1 — Push repo

```bash
railway login
railway init         # creates new project
railway up           # first deploy (will fail — no vars yet)
```

## 2 — Add Redis addon

Railway dashboard → your project → **+ New** → **Database** → **Redis**  
Railway auto-injects `REDIS_URL` into your service. Nothing else needed.

## 3 — Set env vars

Railway dashboard → your service → **Variables**:

```
API_ID              = (from my.telegram.org)
API_HASH            = (from my.telegram.org)
SESSION_STRING      = (generate below)
CHANNEL_USERNAME    = @yourchannel (or -100xxxxxxxxx for private channels)
BASE_URL            = https://<your-service>.up.railway.app
SYNC_INTERVAL       = 300
STREAM_CONCURRENCY  = 5
```

> [!IMPORTANT]
> **For Private Channels**:
> 1. Use the numeric chat ID starting with `-100` (e.g. `CHANNEL_USERNAME = -1001234567890`) instead of a username.
> 2. Ensure that the Telegram account used to generate the `SESSION_STRING` has joined the private channel, as it acts as a userbot to fetch messages/media.

### Generate SESSION_STRING

```bash
pip install pyrogram tgcrypto
python3 -c "
from pyrogram import Client
with Client(':memory:', api_id=YOUR_ID, api_hash='YOUR_HASH') as c:
    print(c.export_session_string())
"
```

## 4 — Redeploy

```bash
railway up
```

## 5 — Verify

```bash
curl https://<your-domain>/
# {"status":"ok","movies":0,"channel":"@yourchannel","sync_age_min":null}

# Trigger first sync
curl https://<your-domain>/sync
```

## 6 — Install in Stremio

```
https://<your-domain>/manifest.json
```

Or open: `stremio://<your-domain>/manifest.json`

Dashboard: `https://<your-domain>/dashboard`

---

## Scaling

Railway free tier: 512MB RAM, shared CPU — fine for personal use.  
Hobby plan ($5/mo): 8GB RAM, dedicated CPU, always-on (no cold starts).

To scale horizontally: bump to multiple replicas in Railway dashboard.  
Redis distributed lock in sync prevents stampede. Stateless API nodes work fine.

## Logs

```bash
railway logs --tail
```
