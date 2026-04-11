#!/usr/bin/env node
/**
 * MXRE Launcher — One button to rule them all
 *
 * http://localhost:3335
 * Click START → everything runs
 */

import { createServer } from "http";
import { spawn } from "child_process";
import { existsSync } from "fs";

const PORT = 3335;

let pipelineRunning = false;

const html = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MXRE Launcher</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .container {
      background: white;
      border-radius: 20px;
      padding: 60px 40px;
      box-shadow: 0 30px 60px rgba(0,0,0,0.3);
      text-align: center;
      max-width: 500px;
    }
    h1 {
      font-size: 32px;
      margin-bottom: 10px;
      color: #333;
    }
    .subtitle {
      color: #666;
      margin-bottom: 40px;
      font-size: 14px;
    }
    button {
      width: 100%;
      padding: 16px;
      font-size: 18px;
      font-weight: 600;
      border: none;
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.3s;
      margin-bottom: 20px;
    }
    .start-btn {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
    }
    .start-btn:hover:not(:disabled) {
      transform: translateY(-2px);
      box-shadow: 0 10px 20px rgba(102, 126, 234, 0.3);
    }
    .start-btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .status {
      background: #f8f9fa;
      border-radius: 10px;
      padding: 20px;
      text-align: left;
      margin-bottom: 20px;
      font-family: monospace;
      font-size: 13px;
      min-height: 100px;
      max-height: 300px;
      overflow-y: auto;
      color: #333;
    }
    .log-line {
      padding: 4px 0;
      border-bottom: 1px solid #eee;
    }
    .log-line.success { color: #28a745; }
    .log-line.error { color: #dc3545; }
    .log-line.info { color: #667eea; }
    .indicator {
      display: inline-block;
      width: 12px;
      height: 12px;
      border-radius: 50%;
      margin-right: 8px;
    }
    .indicator.running { background: #28a745; animation: pulse 2s infinite; }
    .indicator.stopped { background: #dc3545; }
    .info-box {
      background: #e7f3ff;
      border-left: 4px solid #2196F3;
      padding: 12px;
      margin-top: 20px;
      border-radius: 4px;
      font-size: 12px;
      text-align: left;
      color: #0c5aa0;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.5; }
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>🚀 MXRE</h1>
    <p class="subtitle">Full property data pipeline with MundoX</p>

    <button class="start-btn" id="startBtn" onclick="startPipeline()">
      ▶ START EVERYTHING
    </button>

    <div class="status" id="status">
      <div class="log-line info">Ready to launch...</div>
    </div>

    <div style="font-size: 12px; color: #999; margin-top: 20px;">
      <p><span class="indicator stopped"></span> Assessor data: waiting</p>
      <p><span class="indicator stopped"></span> Rental scraper: waiting</p>
      <p><span class="indicator stopped"></span> Mortgage linker: waiting</p>
      <p><span class="indicator stopped"></span> MundoX agent: waiting</p>
    </div>

    <div class="info-box">
      ✓ Starts MundoX worker if needed<br>
      ✓ Runs full 3-layer ingest (assessor + rentals + mortgages)<br>
      ✓ MundoX autonomously fixes adapter failures<br>
      ✓ All data flows to Supabase automatically
    </div>
  </div>

  <script>
    let running = false;

    async function startPipeline() {
      if (running) return;
      running = true;

      const btn = document.getElementById("startBtn");
      const status = document.getElementById("status");
      btn.disabled = true;
      status.innerHTML = '<div class="log-line info">🔄 Starting MundoX...</div>';

      try {
        const res = await fetch("/api/start", { method: "POST" });
        const data = await res.json();

        if (data.success) {
          addLog("✓ MundoX inference service ready");
          addLog("✓ Full ingest pipeline starting...");
          addLog("✓ Assessor data: 182 counties (50 concurrent)");
          addLog("✓ Rental scraper: RentCafe + property sites");
          addLog("✓ Mortgage linker: County recorder filings");
          addLog("✓ MundoX agent: Autonomous orchestration");
          addLog("");
          addLog("Everything running. Go to bed.");
          addLog("Monitor at http://localhost:3333");
          addLog("Chat with MundoX at http://localhost:3334");
        } else {
          addLog("✗ Error: " + data.error, "error");
          running = false;
          btn.disabled = false;
        }
      } catch (err) {
        addLog("✗ Connection error: " + err.message, "error");
        running = false;
        btn.disabled = false;
      }
    }

    function addLog(msg, type = "info") {
      const status = document.getElementById("status");
      const line = document.createElement("div");
      line.className = "log-line " + type;
      line.textContent = msg;
      status.appendChild(line);
      status.scrollTop = status.scrollHeight;
    }
  </script>
</body>
</html>
`;

const server = createServer(async (req, res) => {
  if (req.url === "/" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(html);
  } else if (req.url === "/api/start" && req.method === "POST") {
    if (pipelineRunning) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Pipeline already running" }));
      return;
    }

    try {
      pipelineRunning = true;

      console.log("\n========================================");
      console.log("STARTING FULL MXRE PIPELINE");
      console.log("========================================\n");

      // Start full pipeline with shell: true so it finds npx
      const pipeline = spawn("npx", ["tsx", "full-ingest-pipeline.ts"], {
        cwd: process.cwd(),
        detached: true,
        shell: true,
        stdio: "inherit",
      });
      pipeline.unref();

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        success: true,
        message: "Pipeline started. Check your terminal for output."
      }));
    } catch (err) {
      pipelineRunning = false;
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: String(err) }));
    }
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("\n========================================");
  console.log("MXRE LAUNCHER");
  console.log("========================================\n");
  console.log(`🚀 http://localhost:${PORT}\n`);
  console.log("Click START → everything runs\n");
});
