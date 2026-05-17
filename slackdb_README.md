# SlackDB

Collaborative database operations inside Slack. Connects Slack slash commands and interactive approval cards to AutoDB so teams can ask questions, risk-check migrations, approve changes, and keep an audit trail without leaving the channel.

Built as a FastAPI + Slack Bolt bot with an optional live dashboard for migration activity.

## Quick start

1. Install dependencies

```bash
pip install -r requirements.txt
```

2. Configure environment

Copy `env.example` to `.env` and fill in:

```bash
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_SIGNING_SECRET=your-signing-secret
AUTODB_API_KEY=adb_your-api-key
APPROVER_SLACK_ID=U1234567890
PORT=3000
HOST=0.0.0.0
```

3. Run the bot

```bash
python3 slackdb_bot.py
```

4. Point Slack events to:

```text
https://<your-domain>/slack/events
```

The app exposes a health check at `/health` and audit data at `/audit`.

## Docker

```bash
docker compose up --build
```

The compose file starts the SlackDB bot and includes optional PostgreSQL and Redis services for future state/caching work.

## Slack commands

```text
/db connect <id>       - set the active AutoDB connection
/db connections        - list available AutoDB connections
/db introspect         - refresh schema metadata
/db query <question>   - ask a database question in plain English
/db ask <question>     - answer with generated SQL plus optimization hints
/db analyze <SQL>      - risk-check a migration before execution
/db optimize <SQL>     - get query performance suggestions
/db panic [token]      - launch an emergency rollback flow
/db audit              - show recent migration history
/db status             - show bot, approver, and migration state
```

## Approval flow

1. A user runs `/db analyze <SQL>`.
2. SlackDB sends the SQL to AutoDB for risk scoring and rollback analysis.
3. SlackDB posts an interactive Slack card with risk category, affected tables, rollback availability, and action buttons.
4. Low-risk changes can be approved directly. High-risk changes can require a configured approver.
5. Approved migrations execute through AutoDB and are written to the in-memory audit log.
6. SlackDB polls execution status and posts completion/failure updates in-thread.

## Dashboard

`dashboard.html` is a read-only live view for the audit API. It visualizes:

- total migrations
- pending approvals
- average risk score
- success rate
- recent migration feed
- risk distribution
- team activity
- daily approved/rejected activity

Open it alongside the running API and set its API base if needed.

## Structure

```text
slackdb_bot.py       - FastAPI app, Slack command router, AutoDB client, approvals
dashboard.html       - live audit dashboard powered by /audit
mockup.html          - static Slack-style visual mockup
requirements.txt     - Python dependencies
env.example          - required environment variables
Dockerfile           - container image for the bot
docker-compose.yml   - bot plus optional Postgres/Redis services
Procfile             - platform deployment entrypoint
vercel.json          - deployment configuration
```

## Notes

SlackDB keeps runtime state in memory in the current version: active connections, pending approvals, and audit entries reset when the process restarts. The compose file already sketches PostgreSQL and Redis as natural next steps for durable state and caching.

