# DMScraper

Mirrors a Discord user's DMs (1:1 and group) into a destination Discord server, one bot-created channel per DM partner. On first run it backfills the full history of every existing DM. Afterwards it stays running and forwards new messages live.

Modelled on [Copycord](https://github.com/Copycord/Copycord); same two-process architecture (selfbot reader + bot writer), same WebSocket IPC, same SQLite-backed state.

> **WARNING.** Reading DMs requires a Discord **user token** (selfbot via `discord.py-self`). Discord's ToS prohibits user-token automation and your account can be suspended. Use only for personal archival of your own DMs.

## Architecture

```
┌──────────┐ user token ┌────────────┐    WS :8765     ┌──────────┐ bot token
│ Discord  │◀───────────│  client    │────────────────▶│  server  │────────────▶ Discord (dest guild)
│ (DMs)    │            │ (selfbot)  │                 │ (bot)    │
└──────────┘            └────────────┘                 └─────┬────┘
                                                             │
                                                       ┌─────▼─────┐
                                                       │ data.db   │
                                                       │ (SQLite)  │
                                                       └─────▲─────┘
                                                             │
                                                       ┌─────┴────┐ http :6767
                                                       │  admin   │
                                                       │ (FastAPI)│
                                                       └──────────┘
```

| Service | Role | Library | Port |
|---|---|---|---|
| `client` | Reads DMs from your account via Discord gateway | `discord.py-self` | — |
| `server` | Creates channels and posts via webhooks in destination guild | `py-cord` | WS 8765 |
| `admin` | Status dashboard | FastAPI | HTTP 6767 |

## Setup

1. **Create a destination server** and a bot:
   - https://discord.com/developers/applications → new application → Bot tab → "Reset Token", copy
   - Invite the bot to your destination server with **Manage Channels** + **Manage Webhooks** + **Send Messages**
2. **Get your user token.** (Out of scope — search "discord user token" if you don't already know how.)
3. **Configure**:
   ```sh
   cp .env.example .env
   # edit .env: CLIENT_TOKEN, SERVER_TOKEN, DEST_GUILD_ID
   ```
4. **Run**:
   ```sh
   docker compose up -d --build
   ```
5. **Watch the dashboard**: http://localhost:6767

The first launch will enumerate every open DM and start backfilling them in parallel (up to `BACKFILL_CONCURRENCY`, default 3). Each DM becomes its own text channel under a category named `DMs` (configurable via `DMS_CATEGORY_NAME`).

## How it works

- **Channels.** On first sight of a DM, the server creates a text channel `dm-<slug>` (or `group-<slug>` for group DMs) under the `DMs` category, plus one webhook in that channel.
- **Posting.** Messages are posted via that webhook with `username`/`avatar_url` set to the original sender, so DMs look authentic. For group DMs the message body is also prefixed with the sender's name (a single webhook can't impersonate multiple users at once, so the prefix carries identity).
- **Attachments.** Re-uploaded into the destination — the file lives in the destination server even after the original is deleted. Files larger than `ATTACHMENT_MAX_BYTES` (default 25 MB) are linked instead.
- **Resume.** Backfill checkpoints to SQLite every 25 messages. If the client crashes or you restart mid-backfill, it picks up from the last checkpointed message id.
- **Edits & deletes.** Tracked via the `messages` table — when you edit/delete a DM, the mirrored copy is updated/removed.

## Config (env vars)

| Var | Default | Notes |
|---|---|---|
| `CLIENT_TOKEN` | — | **Required.** User token. |
| `SERVER_TOKEN` | — | **Required.** Bot token. |
| `DEST_GUILD_ID` | — | **Required.** ID of the destination server. |
| `DMS_CATEGORY_NAME` | `DMs` | Category for mirrored channels. |
| `BACKFILL_CONCURRENCY` | `3` | Parallel channel backfills. |
| `ATTACHMENT_MAX_BYTES` | `25000000` | Per-file cap before linking. |
| `INCLUDE_GROUP_DMS` | `true` | Mirror group DMs too. |
| `ADMIN_PORT` | `6767` | Dashboard port. |

## Verification

After `docker compose up -d --build`:

- `docker compose logs -f client server` — both should show `Logged in as ...`
- Open http://localhost:6767 — backfill progress per DM
- Send yourself a DM from another account — should appear in the mirrored channel within 1-2 seconds
- Edit and delete that DM — mirrored copy should update/disappear

## Running without Docker

Each service has its own `requirements.txt`. Because `discord.py-self` and `py-cord` both occupy the `discord` namespace, **client and server need separate Python venvs**.

```sh
# server
python -m venv .venv-server && .venv-server/Scripts/activate
pip install -r code/server/requirements.txt
PYTHONPATH=code python -m server.server

# client (separate shell, separate venv)
python -m venv .venv-client && .venv-client/Scripts/activate
pip install -r code/client/requirements.txt
PYTHONPATH=code python -m client.client

# admin (any venv with fastapi+uvicorn works)
PYTHONPATH=code python -m admin.app
```
