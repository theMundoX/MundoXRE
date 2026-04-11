#!/usr/bin/env tsx
/**
 * MundoX Agent — autonomous tool-using loop powered by the local 27B.
 *
 * Think: a tiny Claude Code clone that runs against the local llama.cpp
 * endpoint. Give it a goal in natural language and a set of tools, and it
 * loops {think → call tool → observe → repeat} until done or step limit.
 *
 * Why it exists:
 *   - The 30-day MXRE sprint needs background research, data cleanup, county
 *     discovery, OCR labeling, etc. — work that's repetitive but requires
 *     judgement. MundoX (Qwen3.5-27B) is good enough for ~10-15 step loops
 *     and runs free 24/7 on the GPU.
 *
 * Available tools:
 *   - web_search(query)              DuckDuckGo HTML search
 *   - web_fetch(url)                  Raw HTTP GET, returns text/HTML
 *   - file_read(path)                 Read a file
 *   - file_write(path, content)       Write a file
 *   - file_glob(pattern)              List files matching a glob (under MXRE)
 *   - run_shell(cmd)                  Execute a shell command (sandboxed list)
 *   - supabase_query(sql)             SELECT-only SQL against Supabase
 *   - finish(summary)                 Signal completion
 *
 * Usage:
 *   npx tsx scripts/mundox-agent.ts "Find ArcGIS parcel feature service URLs for all 254 Texas counties"
 *   npx tsx scripts/mundox-agent.ts --max-steps 30 --model mundox "..."
 */

import "dotenv/config";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { glob as globSync } from "node:fs/promises";
import { execSync } from "node:child_process";
import { resolve, join, dirname } from "node:path";

// ─── Config ─────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const MAX_STEPS = Number(args.find((_, i) => args[i - 1] === "--max-steps") || 25);
const MODEL = (args.find((_, i) => args[i - 1] === "--model") as string) || "mundox";
const ENDPOINT = process.env.MUNDOX_URL || "http://127.0.0.1:18791/v1/chat/completions";
const GOAL = args.filter((a) => !a.startsWith("--") && args[args.indexOf(a) - 1] !== "--max-steps" && args[args.indexOf(a) - 1] !== "--model").join(" ").trim();
const MXRE_ROOT = "C:/Users/msanc/mxre";
const LOGS_DIR = join(MXRE_ROOT, "data", "agent-runs");

if (!existsSync(LOGS_DIR)) mkdirSync(LOGS_DIR, { recursive: true });

if (!GOAL) {
  console.error("Usage: npx tsx scripts/mundox-agent.ts \"<goal in plain English>\"");
  console.error("       --max-steps N (default 25)");
  console.error("       --model <name>  (default mundox)");
  process.exit(1);
}

// ─── Tool definitions (OpenAI tool-calling format) ─────────────────

const tools = [
  {
    type: "function",
    function: {
      name: "web_search",
      description: "Search the web. Returns top results as a JSON list of {title, url, snippet}.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Search query" },
        },
        required: ["query"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "web_fetch",
      description: "HTTP GET a URL and return the text body (HTML or JSON). Capped at 30KB.",
      parameters: {
        type: "object",
        properties: {
          url: { type: "string", description: "Full URL including https://" },
        },
        required: ["url"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "file_read",
      description: "Read a file from the MXRE project. Path is relative to MXRE root.",
      parameters: {
        type: "object",
        properties: {
          path: { type: "string", description: "Relative path under MXRE/" },
        },
        required: ["path"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "file_write",
      description: "Write a file under the MXRE project. Path relative to MXRE root. Creates dirs.",
      parameters: {
        type: "object",
        properties: {
          path: { type: "string", description: "Relative path under MXRE/" },
          content: { type: "string", description: "File content" },
        },
        required: ["path", "content"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "file_glob",
      description: "Find files matching a glob under the MXRE project.",
      parameters: {
        type: "object",
        properties: {
          pattern: { type: "string", description: "Glob pattern, e.g. 'data/counties/*.json'" },
        },
        required: ["pattern"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "supabase_query",
      description: "Run a read-only SQL query against the MXRE Supabase. SELECT only.",
      parameters: {
        type: "object",
        properties: {
          sql: { type: "string", description: "SELECT statement" },
        },
        required: ["sql"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "run_shell",
      description: "Run a shell command. Limited to: ls, cat, head, tail, wc, grep, find, jq, curl. NO writes outside MXRE.",
      parameters: {
        type: "object",
        properties: {
          cmd: { type: "string", description: "Shell command" },
        },
        required: ["cmd"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "finish",
      description: "Call this when the goal is complete. Provide a 1-paragraph summary of what was done.",
      parameters: {
        type: "object",
        properties: {
          summary: { type: "string", description: "What was accomplished" },
        },
        required: ["summary"],
      },
    },
  },
];

// ─── Tool implementations ─────────────────────────────────────────

const ALLOWED_SHELL_COMMANDS = ["ls", "cat", "head", "tail", "wc", "grep", "find", "jq", "curl", "echo"];

async function tool_web_search(query: string): Promise<string> {
  try {
    const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 MundoX-Agent" } });
    const html = await r.text();
    // Quick-and-dirty parse of DDG HTML results
    const results: Array<{ title: string; url: string; snippet: string }> = [];
    const re = /<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>([^<]+)<\/a>[\s\S]*?<a[^>]+class="[^"]*result__snippet[^"]*"[^>]*>([^<]+)<\/a>/g;
    let m;
    while ((m = re.exec(html)) && results.length < 10) {
      results.push({
        title: m[2].trim(),
        url: decodeURIComponent(m[1].replace(/^\/\/duckduckgo\.com\/l\/\?uddg=/, "").split("&rut=")[0]),
        snippet: m[3].trim(),
      });
    }
    return JSON.stringify(results, null, 2);
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

async function tool_web_fetch(url: string): Promise<string> {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 20_000);
    const r = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 MundoX-Agent" },
      signal: ctrl.signal,
    });
    clearTimeout(t);
    let body = await r.text();
    if (body.length > 30_000) body = body.slice(0, 30_000) + `\n\n... [truncated, original ${body.length} bytes]`;
    return `HTTP ${r.status}\n\n${body}`;
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

function safePath(rel: string): string {
  const abs = resolve(MXRE_ROOT, rel);
  if (!abs.startsWith(resolve(MXRE_ROOT))) throw new Error(`refused: path ${rel} is outside MXRE root`);
  return abs;
}

function tool_file_read(path: string): string {
  try {
    const abs = safePath(path);
    if (!existsSync(abs)) return `error: file not found: ${path}`;
    let content = readFileSync(abs, "utf8");
    if (content.length > 30_000) content = content.slice(0, 30_000) + `\n\n... [truncated, original ${content.length} bytes]`;
    return content;
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

function tool_file_write(path: string, content: string): string {
  try {
    const abs = safePath(path);
    mkdirSync(dirname(abs), { recursive: true });
    writeFileSync(abs, content, "utf8");
    return `wrote ${content.length} bytes to ${path}`;
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

async function tool_file_glob(pattern: string): Promise<string> {
  try {
    const matches: string[] = [];
    for await (const f of globSync(pattern, { cwd: MXRE_ROOT })) {
      matches.push(String(f));
      if (matches.length >= 200) break;
    }
    return JSON.stringify(matches, null, 2);
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

async function tool_supabase_query(sql: string): Promise<string> {
  try {
    const trimmed = sql.trim().toLowerCase();
    if (!trimmed.startsWith("select") && !trimmed.startsWith("with")) {
      return "error: only SELECT (or WITH ... SELECT) queries allowed";
    }
    const r = await fetch(`${process.env.SUPABASE_URL}/pg/query`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: process.env.SUPABASE_SERVICE_KEY!,
        Authorization: `Bearer ${process.env.SUPABASE_SERVICE_KEY!}`,
      },
      body: JSON.stringify({ query: sql }),
    });
    let body = await r.text();
    if (body.length > 20_000) body = body.slice(0, 20_000) + `\n\n... [truncated]`;
    return body;
  } catch (e) {
    return `error: ${e instanceof Error ? e.message : e}`;
  }
}

function tool_run_shell(cmd: string): string {
  try {
    const head = cmd.trim().split(/\s+/)[0];
    if (!ALLOWED_SHELL_COMMANDS.includes(head)) {
      return `error: shell command '${head}' not in allowlist (${ALLOWED_SHELL_COMMANDS.join(", ")})`;
    }
    const out = execSync(cmd, { cwd: MXRE_ROOT, encoding: "utf8", timeout: 30_000, maxBuffer: 1024 * 1024 });
    return out.length > 20_000 ? out.slice(0, 20_000) + "\n\n... [truncated]" : out;
  } catch (e: any) {
    return `error: ${e?.message || String(e)}`;
  }
}

async function callTool(name: string, args: any): Promise<string> {
  switch (name) {
    case "web_search":
      return await tool_web_search(args.query);
    case "web_fetch":
      return await tool_web_fetch(args.url);
    case "file_read":
      return tool_file_read(args.path);
    case "file_write":
      return tool_file_write(args.path, args.content);
    case "file_glob":
      return await tool_file_glob(args.pattern);
    case "supabase_query":
      return await tool_supabase_query(args.sql);
    case "run_shell":
      return tool_run_shell(args.cmd);
    case "finish":
      return `FINISH: ${args.summary}`;
    default:
      return `error: unknown tool ${name}`;
  }
}

// ─── Agent loop ──────────────────────────────────────────────────

async function chat(messages: any[]): Promise<any> {
  const r = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: MODEL,
      messages,
      tools,
      tool_choice: "auto",
      max_tokens: 1500,
      temperature: 0.3,
      // disable thinking — agentic loops perform better without internal CoT taking up tokens
      chat_template_kwargs: { enable_thinking: false },
    }),
  });
  if (!r.ok) throw new Error(`LLM error: HTTP ${r.status} ${await r.text()}`);
  return await r.json();
}

const SYSTEM = `You are MundoX Agent, an autonomous worker for the MXRE real estate data project.

Your job is to use tools to accomplish the user's goal. Work in small steps: think briefly, call ONE tool, observe the result, then decide the next step.

Key rules:
1. ALWAYS use tools — do not just describe what you would do.
2. When you've accomplished the goal, call the finish() tool with a short summary.
3. If a tool errors, try a different approach. Don't repeat the same call.
4. supabase_query is SELECT-only. file_write can create new files. run_shell is sandboxed (ls/cat/grep/curl/jq).
5. Be concise. Don't explain your reasoning at length — just take the next action.

The MXRE project lives at /c/Users/msanc/mxre. Common paths:
  src/discovery/adapters/   — county scraper adapters
  data/counties/            — county config JSON files
  src/db/migrations/        — database schema migrations
  scripts/                  — one-off scripts

Goal: ${GOAL}`;

async function main() {
  const runId = `run-${Date.now()}`;
  const logPath = join(LOGS_DIR, `${runId}.log`);
  const log = (msg: string) => {
    console.log(msg);
    try {
      writeFileSync(logPath, msg + "\n", { flag: "a" });
    } catch {}
  };

  log(`MundoX Agent — run ${runId}`);
  log(`Endpoint: ${ENDPOINT}`);
  log(`Model:    ${MODEL}`);
  log(`Goal:     ${GOAL}`);
  log(`Log:      ${logPath}\n`);

  const messages: any[] = [
    { role: "system", content: SYSTEM },
    { role: "user", content: `GOAL: ${GOAL}\n\nBegin.` },
  ];

  for (let step = 1; step <= MAX_STEPS; step++) {
    log(`\n──── step ${step}/${MAX_STEPS} ────`);

    let resp;
    try {
      resp = await chat(messages);
    } catch (e) {
      log(`LLM error: ${e instanceof Error ? e.message : e}`);
      break;
    }

    const choice = resp.choices?.[0];
    if (!choice) {
      log(`no choice in response: ${JSON.stringify(resp).slice(0, 400)}`);
      break;
    }

    const msg = choice.message;
    const toolCalls = msg.tool_calls || [];

    if (msg.content) log(`assistant: ${msg.content.slice(0, 600)}`);

    messages.push(msg);

    if (toolCalls.length === 0) {
      log("no tool calls — agent stopped");
      break;
    }

    let finished = false;
    for (const tc of toolCalls) {
      const fnName = tc.function?.name || tc.name;
      let fnArgs: any = {};
      try {
        fnArgs = JSON.parse(tc.function?.arguments || tc.arguments || "{}");
      } catch {
        fnArgs = {};
      }

      log(`> tool ${fnName}(${JSON.stringify(fnArgs).slice(0, 200)})`);
      const result = await callTool(fnName, fnArgs);
      const truncated = result.length > 800 ? result.slice(0, 800) + "..." : result;
      log(`< ${truncated}`);

      messages.push({
        role: "tool",
        tool_call_id: tc.id,
        name: fnName,
        content: result,
      });

      if (fnName === "finish") {
        log(`\n=== AGENT FINISHED ===\n${fnArgs.summary || "(no summary)"}`);
        finished = true;
      }
    }

    if (finished) break;
  }

  log(`\nFull transcript: ${logPath}`);
}

main().catch((e) => {
  console.error("agent failed:", e);
  process.exit(1);
});
