# Invoice Demo with Temporal + MCP

### Video:

[![Watch the demo](./demo-image.png)](https://www.youtube.com/watch?v=jlYYCT0K1cw)

### Prerequisites:

- Python3+
- `uv` (curl -LsSf https://astral.sh/uv/install.sh | sh)
- Temporal [Local Setup Guide](https://learn.temporal.io/getting_started/?_gl=1*1bxho70*_gcl_au*MjE1OTM5MzU5LjE3NDUyNjc4Nzk.*_ga*MjY3ODg1NzM5LjE2ODc0NTcxOTA.*_ga_R90Q9SJD3D*czE3NDc0MDg0NTIkbzk0NyRnMCR0MTc0NzQwODQ1MiRqMCRsMCRoMA..)
- [Claude for Desktop](https://claude.ai/download)

## 1. Clone & install

```
 git clone https://github.com/your-org/temporal-mcp-invoice-demo.git
 cd temporal-mcp-invoice-demo
 uv venv
 source .venv/bin/activate
 uv pip install temporalio fastmcp
```

## 2. Launch Temporal locally

```
 temporal server start-dev
```

## 3. Start the worker

```
 python worker.py [--fail-validate] [--fail-payment]
```

## Quick demo boot

Instead of starting the server and worker manually you can launch them in a
`tmux` session using the `boot-demo.sh` helper script:

```
 ./boot-demo.sh
```

# Claude for Desktop Instructions (Sonnet 4)

## 1. Follow steps 1-3 above

## 2. Edit your Claude Config (Claude > Settings > Developer > Edit Config)

```json
{
  "mcpServers": {
    "invoice_processor": {
      "command": "/Path/To/Your/Install/Of/uv",
      "args": [
        "--directory",
        "/Path/To/temporal-invoice-mcp",
        "run",
        "server.py"
      ]
    }
  }
}
```

## 3. Restart Claude for Desktop after editing your config

- If successful you'll see `invoice_processor` under 'Search & Tools'

## 4. To kick off processing the mock invoice, run:

```
trigger <paste: samples/invoice_acme.json>
```

Use your MCP client (e.g., Claude Desktop) to call the `trigger`, `approve`,
`reject`, and `status` tools. The `trigger` tool now returns both the
`workflow_id` and `run_id` of the started workflow. Pass these values to the
`approve`, `reject`, and `status` tools. The sample invoice lives at
`samples/invoice_acme.json`. Inspect Temporal Web at `http://localhost:8233`.
Kill and restart the worker at any time to observe deterministic replay.

## 5. Results

Claude submits the invoice workflow:

<img src="./assets/claude-mcp-invoice-submission.png" width="50%" alt="Claude MCP Invoice Submission" style="display: block; margin: auto;">

It can get status:

<img src="./assets/claude-mcp-invoice-status.png" width="50%" alt="Claude MCP Status" style="display: block; margin: auto;">

Claude + MCP can send inputs and updates to the process workflow such as approvals, or even do a combination of actions - all agentically, explaining in human analogies what's going on if you ask it to:

<img src="./assets/claude-mcp-submit-approve-status.png" width="50%" alt="Claude MCP Combo" style="display: block; margin: auto;">

### What's Cool About This:

1. Agents and applications connected by MCP can provide a powerful way for humans to interact with processes and applications
   - (as long as the applications have an API to interact with)
2. MCP tools don't have to be just one API call - you can get process status and even send it more information as it proceeds
3. Temporal makes modeling a long-running, durable, interactive transaction simple to integrate with MCP

<img src="./assets/interactive-workflows-with-agentic-power.png" width="80%" alt="Interactive Agentic Applications Powered By Workflows" style="display: block; margin: auto;">
