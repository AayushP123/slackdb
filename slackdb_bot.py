"""
SlackDB Bot - Collaborative Database Operations in Slack
Powered by AutoDB — with Oracle RAG channel memory
"""
from dotenv import load_dotenv
load_dotenv()

import os
import json
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.fastapi.async_handler import AsyncSlackRequestHandler
import httpx

# ── Config ─────────────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN      = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")
AUTODB_API_KEY       = os.environ.get("AUTODB_API_KEY")
AUTODB_BASE_URL      = "http://api.autodb.app/api/v1"
APPROVER_SLACK_ID    = os.environ.get("APPROVER_SLACK_ID", "")
HIGH_RISK_THRESHOLD  = 50

# Oracle RAG config
# ORACLE_CONNECTION_ID: the AutoDB connection where Slack messages get stored
# Set to your existing connection ID — we'll create a slack_messages table there
ORACLE_CONNECTION_ID = os.environ.get("ORACLE_CONNECTION_ID", "")
ANTHROPIC_API_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")
ORACLE_SCHEMA_READY  = False  # flips True after table is confirmed

# ── App init ───────────────────────────────────────────────────────────────────

slack_app = AsyncApp(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
api       = FastAPI(title="SlackDB Bot")
handler   = AsyncSlackRequestHandler(slack_app)

connections: Dict[str, Dict] = {}
approvals:   Dict[str, Dict] = {}
audit_log:   List[Dict]      = []


# ── AutoDB client ──────────────────────────────────────────────────────────────

class AutoDBClient:
    def __init__(self, api_key: str):
        self.api_key  = api_key
        self.base_url = AUTODB_BASE_URL
        self._h       = {"Content-Type": "application/json", "X-API-Key": api_key}

    async def _get(self, path):
        async with httpx.AsyncClient(timeout=30.0) as c:
            return (await c.get(f"{self.base_url}{path}", headers=self._h)).json()

    async def _post(self, path, body):
        async with httpx.AsyncClient(timeout=30.0) as c:
            return (await c.post(f"{self.base_url}{path}", headers=self._h, json=body)).json()

    async def list_connections(self):
        return await self._get("/connections")

    async def analyze_migration(self, conn_id, sql):
        return await self._post(f"/connections/{conn_id}/migrations/analyze", {"sql": sql})

    async def execute_migration(self, conn_id, sql, token):
        return await self._post("/migrations/execute", {"connection_id": conn_id, "sql": sql, "approval_token": token})

    async def get_migration_status(self, request_id):
        return await self._get(f"/migrations/requests/{request_id}")

    async def query_database(self, conn_id, query):
        return await self._post(f"/connections/{conn_id}/queries/generate", {"query": query})

    async def optimize_query(self, conn_id, sql):
        return await self._post(f"/connections/{conn_id}/queries/optimize", {"sql": sql})

    async def execute_sql(self, conn_id, sql, caller="human"):
        """Direct SQL execution via AutoDB's /execute endpoint"""
        return await self._post("/execute", {
            "connection_id": conn_id,
            "sql": sql,
            "caller": caller,
            "guardrail": "strict" if caller == "agent" else "strict",
            "row_limit": 200
        })

    async def execute_sql_write(self, conn_id, sql):
        """Write SQL (INSERT/CREATE) with permissive guardrail"""
        return await self._post("/execute", {
            "connection_id": conn_id,
            "sql": sql,
            "caller": "human",
            "row_limit": 1
        })

    async def text_to_sql(self, conn_id, query):
        """AutoDB text-to-SQL endpoint"""
        return await self._post(f"/connections/{conn_id}/queries/generate", {"query": query})

    async def introspect(self, conn_id):
        """Trigger schema re-introspection"""
        return await self._post(f"/connections/{conn_id}/introspect", {})


# ── Oracle RAG: channel memory system ─────────────────────────────────────────

ORACLE_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS slack_messages (
    id BIGSERIAL PRIMARY KEY,
    message_ts VARCHAR(30) UNIQUE NOT NULL,
    thread_ts  VARCHAR(30),
    channel_id VARCHAR(20) NOT NULL,
    channel_name VARCHAR(100),
    user_id    VARCHAR(20) NOT NULL,
    username   VARCHAR(100),
    text       TEXT NOT NULL,
    message_type VARCHAR(20) DEFAULT 'message',
    has_thread BOOLEAN DEFAULT FALSE,
    reply_count INT DEFAULT 0,
    keywords   TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_slack_messages_channel ON slack_messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_slack_messages_ts ON slack_messages(message_ts);
CREATE INDEX IF NOT EXISTS idx_slack_messages_thread ON slack_messages(thread_ts);
CREATE INDEX IF NOT EXISTS idx_slack_messages_text ON slack_messages USING gin(to_tsvector('english', text));
"""


async def ensure_oracle_schema():
    """Create the slack_messages table if it doesn't exist."""
    global ORACLE_SCHEMA_READY
    if ORACLE_SCHEMA_READY or not ORACLE_CONNECTION_ID:
        return ORACLE_SCHEMA_READY
    try:
        db = AutoDBClient(AUTODB_API_KEY)
        await db.execute_sql_write(ORACLE_CONNECTION_ID, ORACLE_CREATE_TABLE)
        # Re-introspect so AutoDB knows about the new table
        asyncio.create_task(db.introspect(ORACLE_CONNECTION_ID))
        ORACLE_SCHEMA_READY = True
        return True
    except Exception as e:
        print(f"Oracle schema setup error: {e}")
        return False


def extract_keywords(text: str) -> str:
    """Extract meaningful keywords from a message for easier searching."""
    stop = {"the","a","an","is","it","in","on","at","to","for","of","and","or",
            "but","i","we","you","he","she","they","this","that","was","be","are",
            "have","has","had","do","did","with","from","by","as","my","your"}
    words = re.findall(r'\b[a-z]{3,}\b', text.lower())
    keywords = [w for w in words if w not in stop]
    # Deduplicate and take top 20
    seen = set()
    unique = []
    for w in keywords:
        if w not in seen:
            seen.add(w)
            unique.append(w)
    return " ".join(unique[:20])


async def store_message(event: dict, client):
    """Store a Slack message into AutoDB via SQL execute."""
    if not ORACLE_CONNECTION_ID:
        return
    await ensure_oracle_schema()
    if not ORACLE_SCHEMA_READY:
        return

    try:
        text       = event.get("text", "")
        ts         = event.get("ts", "")
        thread_ts  = event.get("thread_ts", ts)
        channel_id = event.get("channel", "")
        user_id    = event.get("user", "unknown")
        has_thread = event.get("reply_count", 0) > 0
        reply_count= event.get("reply_count", 0)
        keywords   = extract_keywords(text)

        # Get channel name
        channel_name = channel_id
        try:
            info = await client.conversations_info(channel=channel_id)
            channel_name = info["channel"].get("name", channel_id)
        except Exception:
            pass

        # Get username
        username = user_id
        try:
            uinfo = await client.users_info(user=user_id)
            username = uinfo["user"].get("display_name") or uinfo["user"].get("real_name", user_id)
        except Exception:
            pass

        # Escape single quotes
        safe_text     = text.replace("'", "''")
        safe_username = username.replace("'", "''")
        safe_keywords = keywords.replace("'", "''")
        safe_chan_name = channel_name.replace("'", "''")

        sql = f"""
INSERT INTO slack_messages
    (message_ts, thread_ts, channel_id, channel_name, user_id, username, text, has_thread, reply_count, keywords)
VALUES
    ('{ts}', '{thread_ts}', '{channel_id}', '{safe_chan_name}', '{user_id}', '{safe_username}',
     '{safe_text}', {str(has_thread).upper()}, {reply_count}, '{safe_keywords}')
ON CONFLICT (message_ts) DO UPDATE SET
    text = EXCLUDED.text,
    has_thread = EXCLUDED.has_thread,
    reply_count = EXCLUDED.reply_count,
    keywords = EXCLUDED.keywords;
"""
        db = AutoDBClient(AUTODB_API_KEY)
        await db.execute_sql_write(ORACLE_CONNECTION_ID, sql.strip())

    except Exception as e:
        print(f"Oracle store_message error: {e}")


async def search_messages(query: str, channel_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
    """Search stored messages using AutoDB text-to-SQL or direct SQL."""
    if not ORACLE_CONNECTION_ID or not ORACLE_SCHEMA_READY:
        return []
    try:
        db = AutoDBClient(AUTODB_API_KEY)
        chan_filter = f"AND channel_id = '{channel_id}'" if channel_id else ""

        # Full-text search using PostgreSQL tsvector
        safe_query = query.replace("'", "''")
        sql = f"""
SELECT message_ts, thread_ts, channel_id, channel_name, username, text, created_at, has_thread, reply_count
FROM slack_messages
WHERE to_tsvector('english', text) @@ plainto_tsquery('english', '{safe_query}')
  {chan_filter}
ORDER BY created_at DESC
LIMIT {limit};
"""
        result = await db.execute_sql(ORACLE_CONNECTION_ID, sql.strip())
        if result.get("success") and result.get("data"):
            cols = result["data"]["columns"]
            rows = result["data"]["rows"]
            return [dict(zip(cols, row)) for row in rows]
        return []
    except Exception as e:
        print(f"Oracle search error: {e}")
        return []


async def get_recent_messages(channel_id: str, limit: int = 100) -> List[Dict]:
    """Fetch the most recent N messages from a channel."""
    if not ORACLE_CONNECTION_ID or not ORACLE_SCHEMA_READY:
        return []
    try:
        db = AutoDBClient(AUTODB_API_KEY)
        sql = f"""
SELECT message_ts, thread_ts, username, text, created_at, has_thread, reply_count
FROM slack_messages
WHERE channel_id = '{channel_id}'
  AND thread_ts = message_ts
ORDER BY created_at DESC
LIMIT {limit};
"""
        result = await db.execute_sql(ORACLE_CONNECTION_ID, sql.strip())
        if result.get("success") and result.get("data"):
            cols = result["data"]["columns"]
            rows = result["data"]["rows"]
            msgs = [dict(zip(cols, row)) for row in rows]
            return list(reversed(msgs))  # chronological order
        return []
    except Exception as e:
        print(f"Oracle get_recent error: {e}")
        return []


async def summarize_with_claude(messages: List[Dict], prompt_context: str) -> str:
    """Use Claude to summarize a list of messages."""
    if not ANTHROPIC_API_KEY:
        # Fallback: simple text summary without Claude
        if not messages:
            return "No messages found."
        lines = [f"• [{m.get('username','?')}]: {m.get('text','')[:120]}" for m in messages[:20]]
        return f"Last {len(messages)} messages:\n" + "\n".join(lines)

    try:
        convo = "\n".join([
            f"[{m.get('created_at','')[:16]}] {m.get('username','unknown')}: {m.get('text','')}"
            for m in messages
        ])

        async with httpx.AsyncClient(timeout=60.0) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1000,
                    "messages": [{
                        "role": "user",
                        "content": f"{prompt_context}\n\nMessages:\n{convo}"
                    }]
                }
            )
            data = r.json()
            return data["content"][0]["text"]
    except Exception as e:
        return f"Summary unavailable: {e}"


# ── Message event listener (stores every message) ─────────────────────────────

@slack_app.event("message")
async def handle_message_events(event, client, logger):
    """Intercept every message and store it in AutoDB."""
    subtype = event.get("subtype")
    # Skip bot messages, edits, deletes
    if subtype in ("bot_message", "message_changed", "message_deleted", "channel_join"):
        return
    if event.get("bot_id"):
        return

    # Store asynchronously so it doesn't slow down the bot
    asyncio.create_task(store_message(event, client))


# ── /catchup command ───────────────────────────────────────────────────────────

@slack_app.command("/catchup")
async def handle_catchup(ack, command, client):
    """
    /catchup              — summarize last 100 messages in this channel
    /catchup 50           — summarize last 50 messages
    /catchup billing bug  — semantic search: find messages about billing bug
    """
    await ack()

    text       = command.get("text", "").strip()
    channel_id = command.get("channel_id")
    user_id    = command.get("user_id")

    if not ORACLE_CONNECTION_ID:
        await client.chat_postMessage(channel=channel_id,
            text="⚠️ Oracle not configured. Add `ORACLE_CONNECTION_ID` to your Railway environment variables.")
        return

    await ensure_oracle_schema()

    loading = await client.chat_postMessage(channel=channel_id, text="🔍 Searching channel memory...")

    # Determine mode: number = recent N, text = semantic search
    if text.isdigit():
        limit = min(int(text), 200)
        messages = await get_recent_messages(channel_id, limit=limit)
        prompt = f"Summarize these {len(messages)} Slack messages into a concise catchup. Group by topic. Use bullet points. Be specific about decisions made, problems raised, and anything unresolved."
        mode = f"last {limit} messages"
    elif text:
        messages = await search_messages(text, channel_id=channel_id, limit=50)
        if not messages:
            # Broaden search without channel filter
            messages = await search_messages(text, limit=50)
        prompt = f'The user is searching for messages related to: "{text}". Summarize the most relevant findings. Highlight the thread where the topic was most discussed. Quote specific messages if they\'re important.'
        mode = f'search: "{text}"'
    else:
        messages = await get_recent_messages(channel_id, limit=100)
        prompt = "Summarize these Slack messages into a concise catchup for someone who was away. Group by topic. Use bullet points. Highlight decisions, blockers, and anything that needs follow-up."
        mode = "last 100 messages"

    if not messages:
        await client.chat_update(channel=channel_id, ts=loading["ts"],
            text=f"📭 No messages found for *{mode}*. The Oracle needs more messages — it only knows about messages sent after the bot was added to the channel.")
        return

    summary = await summarize_with_claude(messages, prompt)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🔮 Channel Oracle"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Mode:* {mode} · *Messages analyzed:* {len(messages)}"
            }
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary[:2800]}
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": f"Powered by AutoDB + Claude · Try `/catchup billing bug` to search semantically"}]
        }
    ]

    await client.chat_update(channel=channel_id, ts=loading["ts"],
        text="Channel Oracle summary", blocks=blocks)


# ── /oracle command (advanced queries) ────────────────────────────────────────

@slack_app.command("/oracle")
async def handle_oracle(ack, command, client):
    """
    /oracle stats         — show channel activity stats
    /oracle who           — who's most active?
    /oracle when          — when is this channel most active?
    /oracle <question>    — ask anything about channel history
    """
    await ack()

    text       = command.get("text", "stats").strip().lower()
    channel_id = command.get("channel_id")

    if not ORACLE_CONNECTION_ID:
        await client.chat_postMessage(channel=channel_id,
            text="⚠️ Oracle not configured. Add `ORACLE_CONNECTION_ID` to Railway env vars.")
        return

    await ensure_oracle_schema()
    loading = await client.chat_postMessage(channel=channel_id, text="🔮 Consulting the Oracle...")

    try:
        db = AutoDBClient(AUTODB_API_KEY)

        if text == "stats":
            sql = f"""
SELECT
    COUNT(*) as total_messages,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT DATE(created_at)) as active_days,
    COUNT(CASE WHEN has_thread THEN 1 END) as threaded_messages,
    MIN(created_at)::date as first_message,
    MAX(created_at)::date as last_message
FROM slack_messages
WHERE channel_id = '{channel_id}';
"""
            result = await db.execute_sql(ORACLE_CONNECTION_ID, sql.strip())
            if result.get("success") and result["data"]["rows"]:
                row = dict(zip(result["data"]["columns"], result["data"]["rows"][0]))
                blocks = [
                    {"type": "header", "text": {"type": "plain_text", "text": "🔮 Oracle — Channel Stats"}},
                    {"type": "section", "fields": [
                        {"type": "mrkdwn", "text": f"*Total messages:*\n{row.get('total_messages',0)}"},
                        {"type": "mrkdwn", "text": f"*Unique users:*\n{row.get('unique_users',0)}"},
                        {"type": "mrkdwn", "text": f"*Active days:*\n{row.get('active_days',0)}"},
                        {"type": "mrkdwn", "text": f"*Threaded convos:*\n{row.get('threaded_messages',0)}"},
                        {"type": "mrkdwn", "text": f"*First message:*\n{row.get('first_message','?')}"},
                        {"type": "mrkdwn", "text": f"*Last message:*\n{row.get('last_message','?')}"},
                    ]}
                ]
                await client.chat_update(channel=channel_id, ts=loading["ts"],
                    text="Oracle stats", blocks=blocks)
            else:
                await client.chat_update(channel=channel_id, ts=loading["ts"],
                    text="📭 No data yet. The Oracle needs messages to analyze.")

        elif text == "who":
            sql = f"""
SELECT username, COUNT(*) as message_count
FROM slack_messages
WHERE channel_id = '{channel_id}'
GROUP BY username
ORDER BY message_count DESC
LIMIT 10;
"""
            result = await db.execute_sql(ORACLE_CONNECTION_ID, sql.strip())
            if result.get("success") and result["data"]["rows"]:
                rows = [dict(zip(result["data"]["columns"], r)) for r in result["data"]["rows"]]
                lines = [f"{i+1}. *{r['username']}* — {r['message_count']} messages" for i, r in enumerate(rows)]
                await client.chat_update(channel=channel_id, ts=loading["ts"],
                    text="Oracle: most active users",
                    blocks=[
                        {"type": "header", "text": {"type": "plain_text", "text": "🔮 Oracle — Most Active Users"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
                    ])
            else:
                await client.chat_update(channel=channel_id, ts=loading["ts"], text="📭 No data yet.")

        elif text == "when":
            sql = f"""
SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count
FROM slack_messages
WHERE channel_id = '{channel_id}'
GROUP BY hour
ORDER BY count DESC
LIMIT 5;
"""
            result = await db.execute_sql(ORACLE_CONNECTION_ID, sql.strip())
            if result.get("success") and result["data"]["rows"]:
                rows = [dict(zip(result["data"]["columns"], r)) for r in result["data"]["rows"]]
                lines = [f"• {int(r['hour']):02d}:00 — {r['count']} messages" for r in rows]
                await client.chat_update(channel=channel_id, ts=loading["ts"],
                    text="Oracle: peak hours",
                    blocks=[
                        {"type": "header", "text": {"type": "plain_text", "text": "🔮 Oracle — Peak Activity Hours"}},
                        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
                    ])
            else:
                await client.chat_update(channel=channel_id, ts=loading["ts"], text="📭 No data yet.")

        else:
            # Use AutoDB text-to-SQL to answer arbitrary questions about channel history
            natural_query = f"{text} from the slack_messages table for channel {channel_id}"
            r = await db.text_to_sql(ORACLE_CONNECTION_ID, natural_query)
            if r.get("success"):
                gen_sql = r.get("data", {}).get("sql", "")
                if gen_sql:
                    exec_r = await db.execute_sql(ORACLE_CONNECTION_ID, gen_sql)
                    if exec_r.get("success") and exec_r["data"]["rows"]:
                        cols = exec_r["data"]["columns"]
                        rows = exec_r["data"]["rows"]
                        msgs = [dict(zip(cols, row)) for row in rows[:50]]
                        summary = await summarize_with_claude(msgs,
                            f'Answer this question about Slack channel history: "{text}". Use the data provided.')
                        await client.chat_update(channel=channel_id, ts=loading["ts"],
                            text="Oracle answer",
                            blocks=[
                                {"type": "header", "text": {"type": "plain_text", "text": "🔮 Oracle"}},
                                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Q:* {text}"}},
                                {"type": "section", "text": {"type": "mrkdwn", "text": summary[:2800]}},
                                {"type": "context", "elements": [{"type": "mrkdwn",
                                    "text": f"Generated SQL: `{gen_sql[:100]}...`"}]}
                            ])
                        return
            await client.chat_update(channel=channel_id, ts=loading["ts"],
                text=f"❌ Couldn't answer: `{text}`. Try `/oracle stats`, `/oracle who`, `/oracle when`, or `/catchup <keywords>`.")

    except Exception as e:
        await client.chat_update(channel=channel_id, ts=loading["ts"], text=f"❌ Oracle error: {e}")


# ── Formatters ─────────────────────────────────────────────────────────────────

RISK_EMOJI = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}


def format_risk_card(data: Dict, needs_second: bool = False) -> List[Dict]:
    score    = data.get("risk_score", 0)
    category = data.get("risk_category", "unknown")
    token    = data.get("approval_token", "none")
    emoji    = RISK_EMOJI.get(category, "⚪")
    sandbox  = (data.get("sandbox_result") or {}).get("passed", False)
    irreversible = (data.get("rollback_plan") or {}).get("has_irreversible", False)
    affected = data.get("affected_tables", [])
    sql      = data.get("sql", "")[:120]

    warning = ""
    if needs_second:
        who = f"<@{APPROVER_SLACK_ID}>" if APPROVER_SLACK_ID else "a designated approver"
        warning = f"\n\n⚠️ *High risk — {who} must approve this before it runs.*"

    confirm_block = {}
    if score > 30:
        confirm_block = {"confirm": {
            "title": {"type": "plain_text", "text": "Really run this?"},
            "text": {"type": "mrkdwn", "text": f"*{category.upper()}* risk migration. {'Cannot be rolled back.' if irreversible else 'Can be rolled back.'} Proceed?"},
            "confirm": {"type": "plain_text", "text": "Yes, run it"},
            "deny": {"type": "plain_text", "text": "Cancel"}
        }}

    return [
        {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} Migration Risk: {category.upper()} ({score}/100)"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            f"*SQL:*\n```{sql}```\n"
            f"*Affected tables:* {', '.join(t['table'] for t in affected) if affected else 'none detected'}\n"
            f"*Sandbox:* {'✅ Passed' if sandbox else '❌ Failed'}   "
            f"*Rollback:* {'⚠️ Irreversible' if irreversible else '✅ Available'}"
            f"{warning}"
        )}},
        {"type": "actions", "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "✅ Approve & Run"},
             "style": "primary", "value": token, "action_id": "approve_migration", **confirm_block},
            {"type": "button", "text": {"type": "plain_text", "text": "❌ Reject"},
             "style": "danger", "value": token, "action_id": "reject_migration"},
            {"type": "button", "text": {"type": "plain_text", "text": "📋 Full Details"},
             "value": token, "action_id": "view_details"},
        ]},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": f"Risk score {score}/100 · AutoDB · {datetime.now().strftime('%H:%M:%S')}"}]}
    ]


def format_query_results(result: Dict) -> List[Dict]:
    if not result.get("success"):
        return [{"type": "section", "text": {"type": "mrkdwn",
            "text": f"❌ Query failed:\n```{result.get('error', result)}```"}}]
    data   = result.get("data", {})
    output = data.get("markdown_output", str(data))
    sql    = data.get("sql", "")
    conf   = int(data.get("confidence", 0) * 100)
    tables = ", ".join(data.get("referenced_tables", [])) or "n/a"
    blocks = []
    if sql:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Generated SQL:*\n```{sql}```"}})
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Results:*\n```{output[:2800]}```"}})
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"✨ Confidence: {conf}% · Tables: {tables}"}]})
    return blocks


def format_audit_log() -> str:
    if not audit_log:
        return "_No migrations have been run yet._"
    lines = []
    for e in reversed(audit_log[-10:]):
        icon = {"approved": "✅", "rejected": "❌", "failed": "💥"}.get(e["status"], "❓")
        lines.append(
            f"{icon} `{e['sql'][:70]}...`\n"
            f"   {e['status'].upper()} by <@{e['actor']}> · risk: *{e['risk']}* ({e['score']}/100) · {e['time']}"
        )
    return "\n\n".join(lines)


# ── /db command router ─────────────────────────────────────────────────────────

@slack_app.command("/db")
async def handle_db_command(ack, command, client):
    await ack()
    text    = command.get("text", "").strip()
    user_id = command.get("user_id")
    chan    = command.get("channel_id")
    parts   = text.split(maxsplit=1)

    if not parts:
        await client.chat_postMessage(channel=chan, text=(
            "*SlackDB* — collaborative database ops + channel memory 🗄️\n\n"
            "*Database commands:*\n"
            "• `/db connect <id>` — set your active database\n"
            "• `/db connections` — list available databases\n"
            "• `/db query <question>` — ask your DB in plain English\n"
            "• `/db analyze <SQL>` — risk-check a migration before running it\n"
            "• `/db optimize <SQL>` — get query performance suggestions\n"
            "• `/db audit` — migration history\n"
            "• `/db status` — health check\n\n"
            "*Oracle commands:*\n"
            "• `/catchup` — summarize last 100 messages\n"
            "• `/catchup 50` — summarize last 50 messages\n"
            "• `/catchup billing bug` — find messages about a topic\n"
            "• `/oracle stats` — channel activity stats\n"
            "• `/oracle who` — most active users\n"
            "• `/oracle when` — peak activity hours\n"
            "• `/oracle <question>` — ask anything about channel history"
        ))
        return

    sub  = parts[0].lower()
    args = parts[1] if len(parts) > 1 else ""
    db   = AutoDBClient(AUTODB_API_KEY)

    if   sub == "connect":     await cmd_connect(client, chan, user_id, args)
    elif sub == "connections": await cmd_list_connections(client, chan, db)
    elif sub == "query":       await cmd_query(client, chan, user_id, args, db)
    elif sub == "analyze":     await cmd_analyze(client, chan, user_id, args, db)
    elif sub == "optimize":    await cmd_optimize(client, chan, user_id, args, db)
    elif sub == "audit":       await cmd_audit(client, chan)
    elif sub == "status":      await cmd_status(client, chan, user_id)
    else:
        await client.chat_postMessage(channel=chan, text=f"Unknown command `{sub}`. Type `/db` for help.")


# ── Subcommand implementations ─────────────────────────────────────────────────

async def cmd_connect(client, chan, user_id, conn_id):
    if not conn_id.strip():
        await client.chat_postMessage(channel=chan,
            text="Usage: `/db connect <connection_id>`\nRun `/db connections` to list available IDs.")
        return
    connections[user_id] = {"default_connection": conn_id.strip()}
    await client.chat_postMessage(channel=chan, text=(
        f"✅ *Connected!* Active database: `{conn_id.strip()}`\n"
        f"Try `/db query show me all tables` or `/db analyze ALTER TABLE users ADD COLUMN phone TEXT`"
    ))


async def cmd_list_connections(client, chan, db):
    msg = await client.chat_postMessage(channel=chan, text="🔄 Fetching connections...")
    try:
        r    = await db.list_connections()
        data = r.get("data", r)
        if isinstance(data, list) and data:
            lines = [f"• `{c.get('id', c.get('connection_id','?'))}` — {c.get('name', c.get('db_name','unnamed'))}" for c in data]
            text  = "*Your AutoDB Connections:*\n" + "\n".join(lines) + "\n\nRun `/db connect <id>` to activate one."
        else:
            text = "No connections found. Add one at autodb.app."
        await client.chat_update(channel=chan, ts=msg["ts"], text=text)
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_query(client, chan, user_id, query, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan, text="⚠️ No database connected. Use `/db connect <id>` first.")
        return
    msg = await client.chat_postMessage(channel=chan, text="🔄 Translating to SQL and querying...")
    try:
        r = await db.query_database(conn, query)
        await client.chat_update(channel=chan, ts=msg["ts"], text="Results", blocks=format_query_results(r))
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_analyze(client, chan, user_id, sql, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan, text="⚠️ No database connected. Use `/db connect <id>` first.")
        return
    if not sql.strip():
        await client.chat_postMessage(channel=chan, text="Usage: `/db analyze <SQL>`")
        return

    msg = await client.chat_postMessage(channel=chan, text="🔄 Running AutoDB risk analysis...")
    try:
        r = await db.analyze_migration(conn, sql)
        if not r.get("success"):
            await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ Analysis failed: {r.get('error', r)}")
            return

        data         = r.get("data", {})
        score        = data.get("risk_score", 0)
        token        = data.get("approval_token")
        needs_second = score >= HIGH_RISK_THRESHOLD

        if token:
            approvals[token] = {
                "sql": sql, "connection_id": conn, "data": data,
                "user_id": user_id, "channel_id": chan,
                "approved_by": [], "needs_second": needs_second, "status": "pending"
            }

        await client.chat_update(channel=chan, ts=msg["ts"], text="Migration analysis",
                                  blocks=format_risk_card(data, needs_second))

        if needs_second and APPROVER_SLACK_ID and APPROVER_SLACK_ID != user_id:
            await client.chat_postMessage(channel=chan, text=(
                f"🚨 <@{APPROVER_SLACK_ID}> — your approval is needed for a "
                f"*{data.get('risk_category','high').upper()}* risk migration "
                f"(score {score}/100) submitted by <@{user_id}>.\n"
                f"Please click *Approve & Run* or *Reject* on the card above."
            ))

    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_optimize(client, chan, user_id, sql, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan, text="⚠️ No database connected.")
        return
    msg = await client.chat_postMessage(channel=chan, text="🔄 Analyzing query performance...")
    try:
        r = await db.optimize_query(conn, sql)
        if r.get("success"):
            alts    = r.get("alternatives", [])
            indexes = r.get("index_recommendations", [])
            cost    = r.get("execution_plan", {}).get("total_cost", "N/A")
            blocks  = [
                {"type": "header", "text": {"type": "plain_text", "text": "⚡ Query Optimization Report"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Original cost:* {cost} units"}}
            ]
            if alts:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                    "*Suggested rewrites:*\n" + "\n".join(f"• {a['explanation']} _({a['estimated_improvement']})_" for a in alts[:3])}})
            if indexes:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                    "*Recommended indexes:*\n" + "\n".join(f"• `{i['create_sql'][:80]}`" for i in indexes[:3])}})
            if not alts and not indexes:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "✅ Query looks good — no changes needed."}})
            await client.chat_update(channel=chan, ts=msg["ts"], text="Optimization", blocks=blocks)
        else:
            await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {r.get('error', r)}")
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_audit(client, chan):
    await client.chat_postMessage(channel=chan, blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": "📋 Migration Audit Log"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": format_audit_log()}}
    ])


async def cmd_status(client, chan, user_id):
    conn    = connections.get(user_id, {}).get("default_connection", "_None set_")
    pending = sum(1 for a in approvals.values() if a.get("status") == "pending")
    oracle_status = "✅ Ready" if ORACLE_SCHEMA_READY else ("⚙️ Configured" if ORACLE_CONNECTION_ID else "❌ Not configured")
    await client.chat_postMessage(channel=chan, blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": "📊 SlackDB Status"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Your active DB:*\n`{conn}`"},
            {"type": "mrkdwn", "text": f"*Pending approvals:*\n{pending}"},
            {"type": "mrkdwn", "text": f"*Migrations run:*\n{len(audit_log)}"},
            {"type": "mrkdwn", "text": f"*Approver:*\n{'<@'+APPROVER_SLACK_ID+'>' if APPROVER_SLACK_ID else '⚠️ Not set'}"},
            {"type": "mrkdwn", "text": f"*Oracle (RAG):*\n{oracle_status}"},
            {"type": "mrkdwn", "text": f"*Claude:*\n{'✅ Set' if ANTHROPIC_API_KEY else '⚠️ Not set (summaries use fallback)'}"},
        ]}
    ])


# ── Button handlers ────────────────────────────────────────────────────────────

@slack_app.action("approve_migration")
async def handle_approve(ack, body, client):
    await ack()
    token    = body["actions"][0]["value"]
    actor_id = body["user"]["id"]
    approval = approvals.get(token)

    if not approval:
        await client.chat_postMessage(channel=body["channel"]["id"], text="❌ Approval not found or already processed.")
        return
    if approval["status"] != "pending":
        await client.chat_postMessage(channel=body["channel"]["id"],
            text=f"ℹ️ This migration was already *{approval['status']}*.")
        return

    if approval["needs_second"] and APPROVER_SLACK_ID:
        if actor_id not in (APPROVER_SLACK_ID, approval["user_id"]):
            await client.chat_postMessage(channel=body["channel"]["id"],
                thread_ts=body["message"]["ts"],
                text=f"⛔ Only <@{APPROVER_SLACK_ID}> can approve this high-risk migration.")
            return

    approval["status"] = "approved"
    approval["approved_by"].append(actor_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    db     = AutoDBClient(AUTODB_API_KEY)
    result = await db.execute_migration(approval["connection_id"], approval["sql"], token)

    if result.get("success"):
        req_id = result.get("data", {}).get("request_id", "unknown")
        await client.chat_update(
            channel=body["channel"]["id"], ts=body["message"]["ts"],
            text="Migration approved",
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": (
                f"✅ *Migration approved & executed*\n"
                f"Approved by: <@{actor_id}>  ·  Requested by: <@{approval['user_id']}>\n"
                f"Execution ID: `{req_id}`  ·  {now}"
            )}}]
        )
        await client.chat_postMessage(
            channel=approval["channel_id"], thread_ts=body["message"]["ts"],
            text=f"✅ Migration running. Execution ID: `{req_id}`\n```{approval['sql'][:300]}```"
        )
        audit_log.append({"sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category","?"), "score": approval["data"].get("risk_score",0),
            "status": "approved", "request_id": req_id, "time": now})
        asyncio.create_task(_poll_status(client, approval["channel_id"], body["message"]["ts"], req_id, db))
    else:
        approval["status"] = "failed"
        err = result.get("error", result)
        await client.chat_postMessage(channel=approval["channel_id"], thread_ts=body["message"]["ts"],
            text=f"❌ *Execution failed:*\n```{err}```")
        audit_log.append({"sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category","?"), "score": approval["data"].get("risk_score",0),
            "status": "failed", "time": now})


@slack_app.action("reject_migration")
async def handle_reject(ack, body, client):
    await ack()
    token    = body["actions"][0]["value"]
    actor_id = body["user"]["id"]
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    approval = approvals.get(token)
    if approval:
        approval["status"] = "rejected"
        audit_log.append({"sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category","?"), "score": approval["data"].get("risk_score",0),
            "status": "rejected", "time": now})
    await client.chat_update(
        channel=body["channel"]["id"], ts=body["message"]["ts"],
        text="Migration rejected",
        blocks=[{"type": "section", "text": {"type": "mrkdwn",
            "text": f"❌ *Migration rejected* by <@{actor_id}> at {now}"}}]
    )
    await client.chat_postMessage(channel=body["channel"]["id"], thread_ts=body["message"]["ts"],
        text=f"❌ Migration rejected by <@{actor_id}>.")


@slack_app.action("view_details")
async def handle_view_details(ack, body, client):
    await ack()
    token    = body["actions"][0]["value"]
    approval = approvals.get(token)
    if not approval:
        await client.chat_postMessage(channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"], text="❌ Details not found.")
        return
    d = approval["data"]
    details = json.dumps({
        "risk_score": d.get("risk_score"), "risk_category": d.get("risk_category"),
        "affected_tables": d.get("affected_tables"), "sandbox_result": d.get("sandbox_result"),
        "rollback_plan": d.get("rollback_plan"), "warnings": d.get("warnings", []),
    }, indent=2)
    await client.chat_postMessage(channel=body["channel"]["id"], thread_ts=body["message"]["ts"],
        text=f"📋 *Full Analysis*\n```{details[:2800]}```")


# ── Background polling ─────────────────────────────────────────────────────────

async def _poll_status(client, chan, thread_ts, request_id, db: AutoDBClient):
    for _ in range(10):
        await asyncio.sleep(3)
        try:
            s     = await db.get_migration_status(request_id)
            state = s.get("data", {}).get("status", "")
            if state == "completed":
                await client.chat_postMessage(channel=chan, thread_ts=thread_ts,
                    text=f"✅ Migration `{request_id}` completed successfully.")
                return
            elif state in ("failed", "error"):
                await client.chat_postMessage(channel=chan, thread_ts=thread_ts,
                    text=f"❌ Migration `{request_id}` failed: {s.get('data',{}).get('error','unknown')}")
                return
        except Exception:
            pass


# ── FastAPI routes ─────────────────────────────────────────────────────────────

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@api.post("/slack/events")
async def slack_events(request: Request):
    return await handler.handle(request)


@api.get("/health")
async def health():
    return {
        "status": "healthy",
        "autodb_key": bool(AUTODB_API_KEY),
        "approver_set": bool(APPROVER_SLACK_ID),
        "migrations_run": len(audit_log),
        "pending_approvals": sum(1 for a in approvals.values() if a.get("status") == "pending"),
        "oracle_ready": ORACLE_SCHEMA_READY,
        "oracle_configured": bool(ORACLE_CONNECTION_ID),
        "claude_configured": bool(ANTHROPIC_API_KEY),
    }


@api.get("/audit")
async def get_audit():
    return {
        "logs": audit_log,
        "pending_approvals": sum(1 for a in approvals.values() if a.get("status") == "pending"),
        "total": len(audit_log),
        "approved": sum(1 for l in audit_log if l.get("status") == "approved"),
        "rejected": sum(1 for l in audit_log if l.get("status") == "rejected"),
        "avg_risk": round(sum(l.get("score", 0) for l in audit_log) / len(audit_log)) if audit_log else 0,
    }


@api.on_event("startup")
async def startup():
    """Initialize Oracle schema on boot if configured."""
    if ORACLE_CONNECTION_ID and AUTODB_API_KEY:
        asyncio.create_task(ensure_oracle_schema())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(api, host="0.0.0.0", port=int(os.environ.get("PORT", 3000)))
