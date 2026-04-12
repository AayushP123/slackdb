"""
SlackDB Bot - Collaborative Database Operations in Slack
Powered by AutoDB
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List
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

APPROVER_SLACK_ID   = os.environ.get("APPROVER_SLACK_ID", "")
HIGH_RISK_THRESHOLD = 50

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
        self._h = {"Content-Type": "application/json", "X-API-Key": api_key}

    async def _get(self, path):
        async with httpx.AsyncClient(timeout=30.0) as c:
            return (await c.get(f"{self.base_url}{path}", headers=self._h)).json()

    async def _post(self, path, body):
        async with httpx.AsyncClient(timeout=30.0) as c:
            resp = await c.post(f"{self.base_url}{path}", headers=self._h, json=body)
            print(f"[AutoDB] POST {path} → {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        
    async def run_sandbox(self, conn_id, sql):
        async with httpx.AsyncClient(timeout=120.0) as c:
            resp = await c.post(
                f"{self.base_url}/connections/{conn_id}/migrations/sandbox",
                headers=self._h,
                json={"sql": sql}
            )
            print(f"[AutoDB] POST sandbox → {resp.status_code}: {resp.text[:300]}")
            return resp.json()

    async def list_connections(self):
        return await self._get("/connections")

    async def analyze_migration(self, conn_id, sql):
        return await self._post(f"/connections/{conn_id}/migrations/analyze", {"sql": sql})

    async def execute_migration(self, conn_id, sql, token):
        return await self._post("/migrations/execute", {
            "connection_id": conn_id, "sql": sql, "approval_token": token
        })

    async def get_migration_status(self, request_id):
        return await self._get(f"/migrations/requests/{request_id}")

    async def query_database(self, conn_id, query):
        return await self._post(f"/connections/{conn_id}/queries/generate", {"query": query})

    async def optimize_query(self, conn_id, sql):
        return await self._post(f"/connections/{conn_id}/queries/optimize", {"sql": sql})

# ── Formatters ─────────────────────────────────────────────────────────────────

RISK_EMOJI = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}

def format_risk_card(data: Dict, needs_second: bool = False) -> List[Dict]:
    score        = data.get("risk_score", 0)
    category     = data.get("risk_category", "unknown")
    token        = data.get("approval_token", "none")
    emoji        = RISK_EMOJI.get(category, "⚪")
    sandbox_raw  = data.get("sandbox_result")
    sandbox_ran  = sandbox_raw is not None
    sandbox_ok   = (sandbox_raw or {}).get("passed", False)
    irreversible = (data.get("rollback_plan") or {}).get("has_irreversible", False)
    affected     = data.get("affected_tables", [])
    sql          = data.get("sql", "")[:120]

    # Sandbox status message
    if not sandbox_ran:
        sandbox_text = "⚠️ Unavailable (DB not reachable by AutoDB)"
    elif sandbox_ok:
        sandbox_text = "✅ Passed"
    else:
        sandbox_text = "❌ Failed"

    warning = ""
    if needs_second:
        who     = f"<@{APPROVER_SLACK_ID}>" if APPROVER_SLACK_ID else "a designated approver"
        warning = f"\n\n⚠️ *High risk — {who} must approve this before it runs.*"

    confirm_block = {}
    if score > 30:
        confirm_block = {"confirm": {
            "title":   {"type": "plain_text", "text": "Really run this?"},
            "text":    {"type": "mrkdwn", "text": (
                f"*{category.upper()}* risk migration. "
                f"{'Cannot be rolled back.' if irreversible else 'Can be rolled back.'} Proceed?"
            )},
            "confirm": {"type": "plain_text", "text": "Yes, run it"},
            "deny":    {"type": "plain_text", "text": "Cancel"},
        }}

    return [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"{emoji} Migration Risk: {category.upper()} ({score}/100)"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            f"*SQL:*\n```{sql}```\n"
            f"*Affected tables:* {', '.join(t['table'] if isinstance(t, dict) else t for t in affected) if affected else 'none detected'}\n"
            f"*Sandbox:* {sandbox_text}  "
            f"*Rollback:* {'⚠️ Irreversible' if irreversible else '✅ Available'}"
            f"{warning}"
        )}},
        {"type": "actions", "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "✅ Approve & Run"},
             "style": "primary", "value": token, "action_id": "approve_migration", **confirm_block},
            {"type": "button", "text": {"type": "plain_text", "text": "❌ Reject"},
             "style": "danger",  "value": token, "action_id": "reject_migration"},
            {"type": "button", "text": {"type": "plain_text", "text": "📋 Full Details"},
             "value": token, "action_id": "view_details"},
        ]},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": f"Risk score {score}/100 · AutoDB · {datetime.now().strftime('%H:%M:%S')}"}]},
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
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": f"*Generated SQL:*\n```{sql}```"}})
    blocks.append({"type": "section", "text": {"type": "mrkdwn",
        "text": f"*Results:*\n```{output[:2800]}```"}})
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
        "text": f"✨ Confidence: {conf}% · Tables: {tables}"}]})
    return blocks

def format_audit_log() -> str:
    if not audit_log:
        return "_No migrations have been run yet._"
    lines = []
    for e in reversed(audit_log[-10:]):
        icon = {"approved": "✅", "rejected": "❌", "failed": "💥"}.get(e["status"], "❓")
        lines.append(
            f"{icon} `{e['sql'][:70]}...`\n"
            f"  {e['status'].upper()} by <@{e['actor']}> · "
            f"risk: *{e['risk']}* ({e['score']}/100) · {e['time']}"
        )
    return "\n\n".join(lines)

# ── App Home Tab ───────────────────────────────────────────────────────────────

@slack_app.event("app_home_opened")
async def handle_home_opened(event, client):
    user_id       = event["user"]
    conn          = connections.get(user_id, {}).get("default_connection", "_None set_")
    pending_count = sum(1 for a in approvals.values() if a.get("status") == "pending")
    total         = len(audit_log)
    approved_n    = sum(1 for l in audit_log if l.get("status") == "approved")
    avg_risk      = round(sum(l.get("score", 0) for l in audit_log) / total) if total else 0
    success_pct   = round(approved_n / total * 100) if total else 0

    recent_blocks = []
    for entry in reversed(audit_log[-5:]):
        icon = {"approved": "✅", "rejected": "❌", "failed": "💥"}.get(entry["status"], "❓")
        recent_blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": (
            f"{icon} `{entry['sql'][:60]}...`\n"
            f"_{entry['status'].upper()} · risk {entry['score']}/100 · "
            f"by <@{entry['actor']}> · {entry['time']}_"
        )}})

    if not recent_blocks:
        recent_blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": "_No migrations yet. Try `/db analyze <SQL>` to get started._"}})

    pending_blocks = []
    for token, rec in approvals.items():
        if rec.get("status") == "pending":
            risk  = rec.get("data", {}).get("risk_category", "?")
            score = rec.get("data", {}).get("risk_score", 0)
            pending_blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": (
                f"⏳ `{rec['sql'][:60]}...`\n"
                f"_Risk: {risk} ({score}/100) · by <@{rec['user_id']}>_"
            )}})

    await client.views_publish(
        user_id=user_id,
        view={
            "type": "home",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "🗄️  SlackDB — Your Database Co-Pilot"}},
                {"type": "section", "text": {"type": "mrkdwn",
                    "text": "Safe, collaborative database operations for your whole team. Powered by AutoDB risk analysis."}},
                {"type": "divider"},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Active DB*\n`{conn}`"},
                    {"type": "mrkdwn", "text": f"*Pending approvals*\n{'⚠️ ' if pending_count else ''}{pending_count}"},
                    {"type": "mrkdwn", "text": f"*Migrations run*\n{total}"},
                    {"type": "mrkdwn", "text": f"*Avg risk score*\n{avg_risk}/100"},
                    {"type": "mrkdwn", "text": f"*Success rate*\n{success_pct}%"},
                    {"type": "mrkdwn", "text": f"*Approver*\n{'<@'+APPROVER_SLACK_ID+'>' if APPROVER_SLACK_ID else '⚠️ Not configured'}"},
                ]},
                {"type": "divider"},
                *([
                    {"type": "section", "text": {"type": "mrkdwn", "text": "*⏳ Pending approvals*"}},
                    *pending_blocks,
                    {"type": "divider"},
                ] if pending_blocks else []),
                {"type": "section", "text": {"type": "mrkdwn", "text": "*Recent migrations*"}},
                *recent_blocks,
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "💡 *Quick commands*\n"
                    "`/db connect <id>` · connect a database\n"
                    "`/db introspect` · refresh schema snapshot\n"
                    "`/db query <question>` · ask your DB in plain English\n"
                    "`/db analyze <SQL>` · risk-check a migration\n"
                    "`/db optimize <SQL>` · get performance suggestions\n"
                    "`/db ask <question>` · AI database intelligence\n"
                    "`/db panic` · emergency rollback\n"
                    "`/db audit` · full migration history\n"
                    "`/db status` · health check"
                )}},
            ]
        }
    )

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
            "*SlackDB* — collaborative database ops for your whole team 🗄️\n\n"
            "• `/db connect <id>` — set your active database\n"
            "• `/db introspect` — refresh schema snapshot\n"
            "• `/db connections` — list available databases\n"
            "• `/db query <question>` — ask your DB in plain English\n"
            "• `/db ask <question>` — AI database intelligence & diagnostics\n"
            "• `/db analyze <SQL>` — risk-check a migration before running it\n"
            "• `/db optimize <SQL>` — get query performance suggestions\n"
            "• `/db panic [token]` — emergency rollback\n"
            "• `/db audit` — see migration history\n"
            "• `/db status` — health check"
        ))
        return

    sub  = parts[0].lower()
    args = parts[1] if len(parts) > 1 else ""
    db   = AutoDBClient(AUTODB_API_KEY)

    if   sub == "connect":     await cmd_connect(client, chan, user_id, args)
    elif sub == "introspect":  await cmd_introspect(client, chan, user_id)
    elif sub == "connections": await cmd_list_connections(client, chan, db)
    elif sub == "query":       await cmd_query(client, chan, user_id, args, db)
    elif sub == "ask":         await cmd_ask(client, chan, user_id, args, db)
    elif sub == "analyze":     await cmd_analyze(client, chan, user_id, args, db)
    elif sub == "optimize":    await cmd_optimize(client, chan, user_id, args, db)
    elif sub == "panic":       await cmd_panic(client, chan, user_id, args, db)
    elif sub == "audit":       await cmd_audit(client, chan)
    elif sub == "status":      await cmd_status(client, chan, user_id)
    else:
        await client.chat_postMessage(channel=chan,
            text=f"Unknown command `{sub}`. Type `/db` for help.")

# ── Subcommand implementations ─────────────────────────────────────────────────

async def cmd_connect(client, chan, user_id, conn_id):
    if not conn_id.strip():
        await client.chat_postMessage(channel=chan,
            text="Usage: `/db connect <connection_id>`\nRun `/db connections` to list available IDs.")
        return

    conn_id = conn_id.strip()
    connections[user_id] = {"default_connection": conn_id}

    msg = await client.chat_postMessage(channel=chan,
        text=f"✅ *Connected to* `{conn_id}`\n🔄 Introspecting schema — this takes a few seconds...")
    try:
        async with httpx.AsyncClient(timeout=60.0) as c:
            resp = await c.post(
                f"{AUTODB_BASE_URL}/connections/{conn_id}/introspect",
                headers={"Content-Type": "application/json", "X-API-Key": AUTODB_API_KEY}
            )
            r = resp.json()

        if r.get("success"):
            data        = r.get("data", {})
            table_count = data.get("table_count", "?")
            tables      = data.get("tables", [])
            preview     = ", ".join(tables[:5])
            if len(tables) > 5:
                preview += f" +{len(tables)-5} more"
            await client.chat_update(channel=chan, ts=msg["ts"], text="Connected", blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    f"✅ *Connected & schema loaded*\n"
                    f"Database: `{conn_id}`\n"
                    f"*{table_count} tables found:* {preview}\n\n"
                    f"Ready to use `/db analyze`, `/db query`, and `/db ask`."
                )}}
            ])
        else:
            err = r.get("error", r)
            await client.chat_update(channel=chan, ts=msg["ts"], text="Connected", blocks=[
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    f"✅ *Connected to* `{conn_id}`\n"
                    f"⚠️ Schema introspection failed: `{err}`\n"
                    f"Run `/db introspect` to retry."
                )}}
            ])
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"],
            text=f"✅ Connected to `{conn_id}` but introspection errored: `{e}`\nRun `/db introspect` to retry.")


async def cmd_introspect(client, chan, user_id):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan,
            text="⚠️ No database connected. Use `/db connect <id>` first.")
        return

    msg = await client.chat_postMessage(channel=chan,
        text=f"🔄 Introspecting schema for `{conn}`...")
    try:
        async with httpx.AsyncClient(timeout=60.0) as c:
            resp = await c.post(
                f"{AUTODB_BASE_URL}/connections/{conn}/introspect",
                headers={"Content-Type": "application/json", "X-API-Key": AUTODB_API_KEY}
            )
            r = resp.json()

        if r.get("success"):
            data        = r.get("data", {})
            table_count = data.get("table_count", "?")
            tables      = data.get("tables", [])
            preview     = ", ".join(tables[:8])
            if len(tables) > 8:
                preview += f" +{len(tables)-8} more"
            await client.chat_update(channel=chan, ts=msg["ts"],
                text="Introspection complete", blocks=[
                    {"type": "header", "text": {"type": "plain_text", "text": "🗂️  Schema Loaded"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": (
                        f"*Database:* `{conn}`\n"
                        f"*Tables found:* {table_count}\n"
                        f"*Preview:* {preview}\n\n"
                        f"AutoDB sandbox is ready — migrations will now validate correctly."
                    )}}
                ])
        else:
            err = r.get("error", r)
            await client.chat_update(channel=chan, ts=msg["ts"],
                text=f"❌ Introspection failed: `{err}`")
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"],
            text=f"❌ Introspection error: `{e}`")


async def cmd_list_connections(client, chan, db):
    msg = await client.chat_postMessage(channel=chan, text="🔄 Fetching connections...")
    try:
        r    = await db.list_connections()
        data = r.get("data", r)
        if isinstance(data, list) and data:
            lines = [
                f"• `{c.get('id', c.get('connection_id','?'))}` — {c.get('name', c.get('db_name','unnamed'))}"
                for c in data
            ]
            text = "*Your AutoDB Connections:*\n" + "\n".join(lines) + "\n\nRun `/db connect <id>` to activate one."
        else:
            text = "No connections found. Add one at autodb.app."
        await client.chat_update(channel=chan, ts=msg["ts"], text=text)
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_query(client, chan, user_id, query, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan,
            text="⚠️ No database connected. Use `/db connect <id>` first.")
        return
    msg = await client.chat_postMessage(channel=chan, text="🔄 Translating to SQL and querying...")
    try:
        r = await db.query_database(conn, query)
        await client.chat_update(channel=chan, ts=msg["ts"], text="Results",
            blocks=format_query_results(r))
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_ask(client, chan, user_id, question, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan,
            text="⚠️ No database connected. Use `/db connect <id>` first.")
        return
    if not question.strip():
        await client.chat_postMessage(channel=chan,
            text="Usage: `/db ask <question>`\nExample: `/db ask which customers have the most orders?`")
        return

    msg = await client.chat_postMessage(channel=chan, text="🤔 Thinking about your database...")
    try:
        query_result = await db.query_database(conn, question)
        data         = query_result.get("data", {})
        sql          = data.get("sql", "")
        output       = data.get("markdown_output", "")
        conf         = int(data.get("confidence", 0) * 100)

        opt_tip = ""
        if sql:
            opt_result = await db.optimize_query(conn, sql)
            if opt_result.get("success"):
                alts = opt_result.get("alternatives", [])
                if alts:
                    tip     = alts[0]
                    opt_tip = (
                        f"\n\n💡 *Performance tip:* {tip.get('explanation', '')}"
                        f" _({tip.get('estimated_improvement', '')})_"
                    )
                indexes = opt_result.get("index_recommendations", [])
                if indexes:
                    opt_tip += f"\n🔍 *Recommended index:* `{indexes[0].get('create_sql', '')[:100]}`"

        blocks = [{"type": "header", "text": {"type": "plain_text", "text": f"🧠  {question[:72]}"}}]
        if sql:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": f"*Query used:*\n```{sql[:600]}```"}})
        if output:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": f"*Results:*\n```{output[:1800]}```{opt_tip}"}})
        elif not sql:
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": "_No SQL could be generated. Try rephrasing your question._"}})
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
            "text": f"Confidence: {conf}% · AutoDB text-to-SQL"}]})

        await client.chat_update(channel=chan, ts=msg["ts"], text="Answer", blocks=blocks)
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_analyze(client, chan, user_id, sql, db):
    conn = connections.get(user_id, {}).get("default_connection")
    if not conn:
        await client.chat_postMessage(channel=chan,
            text="⚠️ No database connected. Use `/db connect <id>` first.")
        return
    if not sql.strip():
        await client.chat_postMessage(channel=chan, text="Usage: `/db analyze <SQL>`")
        return

    msg = await client.chat_postMessage(channel=chan, text="🔄 Running AutoDB risk analysis & sandbox...")
    try:
        # Run both in parallel
        analysis_task = asyncio.create_task(db.analyze_migration(conn, sql))
        sandbox_task  = asyncio.create_task(db.run_sandbox(conn, sql))
        r, sandbox_r  = await asyncio.gather(analysis_task, sandbox_task)

        if not r.get("success"):
            await client.chat_update(channel=chan, ts=msg["ts"],
                text=f"❌ Analysis failed: {r.get('error', r)}")
            return

        data         = r.get("data", {})
        score        = data.get("risk_score", 0)
        token        = data.get("approval_token")
        needs_second = score >= HIGH_RISK_THRESHOLD

        # Inject sandbox result into data so format_risk_card picks it up
        if sandbox_r.get("success"):
            data["sandbox_result"] = sandbox_r.get("data")
        else:
            print(f"[Sandbox] failed: {sandbox_r}")

        if token:
            approvals[token] = {
                "sql": sql, "connection_id": conn, "data": data,
                "user_id": user_id, "channel_id": chan,
                "approved_by": [], "needs_second": needs_second, "status": "pending",
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
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Original cost:* {cost} units"}},
            ]
            if alts:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": "*Suggested rewrites:*\n" + "\n".join(
                        f"• {a['explanation']} _({a['estimated_improvement']})_" for a in alts[:3]
                    )}})
            if indexes:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": "*Recommended indexes:*\n" + "\n".join(
                        f"• `{i['create_sql'][:80]}`" for i in indexes[:3]
                    )}})
            if not alts and not indexes:
                blocks.append({"type": "section", "text": {"type": "mrkdwn",
                    "text": "✅ Query looks good — no changes needed."}})
            await client.chat_update(channel=chan, ts=msg["ts"], text="Optimization", blocks=blocks)
        else:
            await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {r.get('error', r)}")
    except Exception as e:
        await client.chat_update(channel=chan, ts=msg["ts"], text=f"❌ {e}")


async def cmd_panic(client, chan, user_id, arg, db):
    target = None

    if arg.strip():
        target = approvals.get(arg.strip())
        if not target:
            await client.chat_postMessage(channel=chan,
                text=f"❌ No approval record found for token `{arg.strip()}`.")
            return
    else:
        for rec in reversed(list(approvals.values())):
            if rec.get("status") == "approved":
                if not (rec.get("data", {}).get("rollback_plan") or {}).get("has_irreversible"):
                    target = rec
                    break

    if not target:
        await client.chat_postMessage(channel=chan, blocks=[
            {"type": "header", "text": {"type": "plain_text", "text": "🚨  PANIC — No target found"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                "No reversible migration found to roll back.\n"
                "Use `/db audit` to find a specific approval token, "
                "then run `/db panic <token>`."
            )}},
        ])
        return

    rollback_sql = (target["data"].get("rollback_plan") or {}).get("combined_script", "")
    category     = target["data"].get("risk_category", "unknown")
    score        = target["data"].get("risk_score", 0)
    original_sql = target.get("sql", "")

    payload = json.dumps({"sql": rollback_sql, "conn": target["connection_id"]})

    await client.chat_postMessage(channel=chan, blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": "🚨  PANIC — Emergency Rollback"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            f"*Rolling back:*\n```{original_sql[:200]}```\n"
            f"*Rollback SQL (auto-generated by AutoDB):*\n```{rollback_sql[:300]}```\n"
            f"*Original risk:* {category} ({score}/100) · "
            f"*Originally run by:* <@{target['user_id']}>"
        )}},
        {"type": "actions", "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "🔴 EXECUTE ROLLBACK"},
                "style": "danger",
                "value": payload,
                "action_id": "execute_panic_rollback",
                "confirm": {
                    "title":   {"type": "plain_text", "text": "This will roll back production"},
                    "text":    {"type": "mrkdwn", "text": "This executes immediately and cannot be undone. Proceed?"},
                    "confirm": {"type": "plain_text", "text": "Yes, roll it back"},
                    "deny":    {"type": "plain_text", "text": "Cancel"},
                },
            },
            {"type": "button", "text": {"type": "plain_text", "text": "Cancel"},
             "value": "cancel", "action_id": "cancel_panic"},
        ]},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": f"<!channel> 🚨 Emergency rollback initiated by <@{user_id}>"}]},
    ])


async def cmd_audit(client, chan):
    await client.chat_postMessage(channel=chan, blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": "📋 Migration Audit Log"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": format_audit_log()}},
    ])


async def cmd_status(client, chan, user_id):
    conn    = connections.get(user_id, {}).get("default_connection", "_None set_")
    pending = sum(1 for a in approvals.values() if a.get("status") == "pending")
    await client.chat_postMessage(channel=chan, blocks=[
        {"type": "header", "text": {"type": "plain_text", "text": "📊 SlackDB Status"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Your active DB:*\n`{conn}`"},
            {"type": "mrkdwn", "text": f"*Pending approvals:*\n{pending}"},
            {"type": "mrkdwn", "text": f"*Migrations run:*\n{len(audit_log)}"},
            {"type": "mrkdwn", "text": f"*Approver:*\n{'<@'+APPROVER_SLACK_ID+'>' if APPROVER_SLACK_ID else '⚠️ Not set'}"},
            {"type": "mrkdwn", "text": f"*AutoDB key:*\n{'✅ Set' if AUTODB_API_KEY else '❌ Missing'}"},
            {"type": "mrkdwn", "text": "*Health:*\n✅ Operational"},
        ]},
    ])

# ── Button handlers ────────────────────────────────────────────────────────────

@slack_app.action("approve_migration")
async def handle_approve(ack, body, client):
    await ack()
    token    = body["actions"][0]["value"]
    actor_id = body["user"]["id"]
    approval = approvals.get(token)

    if not approval:
        await client.chat_postMessage(channel=body["channel"]["id"],
            text="❌ Approval not found or already processed.")
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
    db  = AutoDBClient(AUTODB_API_KEY)

    # Re-analyze to get a fresh token in case the original expired
    try:
        fresh = await db.analyze_migration(approval["connection_id"], approval["sql"])
        live_token = fresh["data"].get("approval_token", token) if fresh.get("success") else token
    except Exception:
        live_token = token

    result = await db.execute_migration(approval["connection_id"], approval["sql"], live_token)

    if result.get("success"):
        req_id = result.get("data", {}).get("request_id", "unknown")
        await client.chat_update(
            channel=body["channel"]["id"], ts=body["message"]["ts"],
            text="Migration approved",
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": (
                f"✅ *Migration approved & executed*\n"
                f"Approved by: <@{actor_id}> · Requested by: <@{approval['user_id']}>\n"
                f"Execution ID: `{req_id}` · {now}"
            )}}],
        )
        await client.chat_postMessage(
            channel=approval["channel_id"], thread_ts=body["message"]["ts"],
            text=f"✅ Migration running. Execution ID: `{req_id}`\n```{approval['sql'][:300]}```",
        )
        audit_log.append({
            "sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category", "?"),
            "score": approval["data"].get("risk_score", 0),
            "status": "approved", "request_id": req_id, "time": now,
        })
        asyncio.create_task(_poll_status(
            client, approval["channel_id"], body["message"]["ts"], req_id, db))
    else:
        approval["status"] = "failed"
        err = result.get("error") or result.get("message") or result.get("detail") or json.dumps(result)
        await client.chat_postMessage(
            channel=approval["channel_id"], thread_ts=body["message"]["ts"],
            text=f"❌ *Execution failed:*\n```{str(err)[:600]}```"
        )
        audit_log.append({
            "sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category", "?"),
            "score": approval["data"].get("risk_score", 0),
            "status": "failed", "time": now,
        })


@slack_app.action("reject_migration")
async def handle_reject(ack, body, client):
    await ack()
    token    = body["actions"][0]["value"]
    actor_id = body["user"]["id"]
    now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    approval = approvals.get(token)

    if approval:
        approval["status"] = "rejected"
        audit_log.append({
            "sql": approval["sql"], "actor": actor_id, "requester": approval["user_id"],
            "risk": approval["data"].get("risk_category", "?"),
            "score": approval["data"].get("risk_score", 0),
            "status": "rejected", "time": now,
        })

    await client.chat_update(
        channel=body["channel"]["id"], ts=body["message"]["ts"],
        text="Migration rejected",
        blocks=[{"type": "section", "text": {"type": "mrkdwn",
            "text": f"❌ *Migration rejected* by <@{actor_id}> at {now}"}}],
    )
    await client.chat_postMessage(channel=body["channel"]["id"],
        thread_ts=body["message"]["ts"],
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
        "risk_score":      d.get("risk_score"),
        "risk_category":   d.get("risk_category"),
        "affected_tables": d.get("affected_tables"),
        "sandbox_result":  d.get("sandbox_result"),
        "rollback_plan":   d.get("rollback_plan"),
        "warnings":        d.get("warnings", []),
    }, indent=2)

    await client.chat_postMessage(channel=body["channel"]["id"],
        thread_ts=body["message"]["ts"],
        text=f"📋 *Full Analysis*\n```{details[:2800]}```")


@slack_app.action("execute_panic_rollback")
async def handle_panic_rollback(ack, body, client):
    await ack()
    payload      = json.loads(body["actions"][0]["value"])
    actor_id     = body["user"]["id"]
    now          = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rollback_sql = payload.get("sql", "")
    conn_id      = payload.get("conn", "")

    db = AutoDBClient(AUTODB_API_KEY)
    try:
        analysis = await db.analyze_migration(conn_id, rollback_sql)
        if analysis.get("success"):
            token  = analysis["data"].get("approval_token")
            result = await db.execute_migration(conn_id, rollback_sql, token)
            if result.get("success"):
                req_id = result.get("data", {}).get("request_id", "unknown")
                await client.chat_update(
                    channel=body["channel"]["id"], ts=body["message"]["ts"],
                    text="Rollback executed",
                    blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": (
                        f"🔴 *Emergency rollback executed* by <@{actor_id}> at {now}\n"
                        f"Execution ID: `{req_id}`\n```{rollback_sql[:300]}```"
                    )}}],
                )
                audit_log.append({
                    "sql": f"[ROLLBACK] {rollback_sql}", "actor": actor_id,
                    "requester": actor_id, "risk": "rollback", "score": 0,
                    "status": "approved", "request_id": req_id, "time": now,
                })
                asyncio.create_task(_poll_status(
                    client, body["channel"]["id"], body["message"]["ts"], req_id, db))
                return

        await client.chat_postMessage(channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"],
            text=f"❌ Rollback failed: {analysis.get('error', 'unknown error')}")
    except Exception as e:
        await client.chat_postMessage(channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"], text=f"❌ Rollback error: {e}")


@slack_app.action("cancel_panic")
async def handle_cancel_panic(ack, body, client):
    await ack()
    await client.chat_update(
        channel=body["channel"]["id"], ts=body["message"]["ts"],
        text="Panic rollback cancelled.",
        blocks=[{"type": "section", "text": {"type": "mrkdwn",
            "text": f"🟡 Emergency rollback cancelled by <@{body['user']['id']}>"}}],
    )

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
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@api.post("/slack/events")
async def slack_events(request: Request):
    return await handler.handle(request)

@api.get("/health")
async def health():
    return {
        "status":            "healthy",
        "autodb_key":        bool(AUTODB_API_KEY),
        "approver_set":      bool(APPROVER_SLACK_ID),
        "migrations_run":    len(audit_log),
        "pending_approvals": sum(1 for a in approvals.values() if a.get("status") == "pending"),
    }

@api.get("/audit")
async def get_audit():
    total      = len(audit_log)
    approved_n = sum(1 for l in audit_log if l.get("status") == "approved")
    rejected_n = sum(1 for l in audit_log if l.get("status") == "rejected")
    avg_risk   = round(sum(l.get("score", 0) for l in audit_log) / total) if total else 0

    risk_dist = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for l in audit_log:
        cat = l.get("risk", "low")
        if cat in risk_dist:
            risk_dist[cat] += 1

    team = defaultdict(int)
    for l in audit_log:
        team[l.get("actor", "unknown")] += 1
    team_stats = [{"user_id": k, "runs": v} for k, v in sorted(team.items(), key=lambda x: -x[1])]

    today = datetime.now().date()
    daily = {}
    for i in range(6, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        daily[d] = {"approved": 0, "rejected": 0}
    for l in audit_log:
        try:
            d = l.get("time", "")[:10]
            if d in daily:
                if l.get("status") == "approved":
                    daily[d]["approved"] += 1
                elif l.get("status") == "rejected":
                    daily[d]["rejected"] += 1
        except Exception:
            pass
    daily_activity = [{"date": k, **v} for k, v in daily.items()]

    pending_items = []
    for token, rec in approvals.items():
        if rec.get("status") == "pending":
            data = rec.get("data", {})
            pending_items.append({
                "token":   token,
                "sql":     rec.get("sql", ""),
                "risk":    data.get("risk_category", "unknown"),
                "score":   data.get("risk_score", 0),
                "user_id": rec.get("user_id", ""),
            })

    return {
        "total":             total,
        "approved":          approved_n,
        "rejected":          rejected_n,
        "pending_approvals": len(pending_items),
        "avg_risk":          avg_risk,
        "risk_distribution": risk_dist,
        "team_stats":        team_stats,
        "daily_activity":    daily_activity,
        "pending_items":     pending_items,
        "recent_logs":       list(reversed(audit_log[-10:])),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(api, host="0.0.0.0", port=int(os.environ.get("PORT", 3000)))