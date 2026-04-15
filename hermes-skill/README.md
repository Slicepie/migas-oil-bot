# usoil — Hermes / Claude Code skill

Free skill exposing the public [usoil.ai](https://usoil.ai) API to any agent that supports the [agentskills.io](https://agentskills.io) format (Hermes, Claude Code, MCP-compatible runtimes).

Lets the agent answer questions like:

- *"What has Trump posted about oil today?"*
- *"What's WTI trading at right now?"*
- *"Any unusual volume on crude?"*
- *"Should I long or short oil based on the latest signal?"*

The backend scores every Trump Truth Social post for oil-market impact (LLM), tracks real-time Hyperliquid WTI perp price, and detects volume spikes on CME + HL. Everything in this skill is **free** and requires no authentication.

## Install (Claude Code)

Copy `usoil/` into `~/.claude/skills/` or your project's `.claude/skills/` directory. Claude will auto-load it and match the description when you ask about oil.

## Install (Hermes)

Drop `usoil/` into your Hermes `skills/` directory.

## Configuration

Default API base is hardcoded to the public pod. Override via env var if you're running a proxy or self-hosting:

```bash
export USOIL_API_BASE="https://api.usoil.ai"
```

## Upgrade

Pro tier ([usoil.ai/pricing](https://usoil.ai/pricing)) adds real-time SSE signal streaming, historical export, and higher rate limits. This free skill is the same data 15-min delayed (eventually — currently live; will flip when the paywall is activated).
