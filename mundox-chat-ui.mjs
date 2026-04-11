#!/usr/bin/env node
/**
 * MundoX Chat UI — Web interface to the local MundoX brain
 * Runs on port 3334
 */

import Fastify from "fastify";
import serveStatic from "@fastify/static";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fastify = Fastify({ logger: false });

// HTML UI
const htmlUI = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MundoX Chat</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .container {
      width: 100%;
      max-width: 700px;
      background: white;
      border-radius: 12px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      height: 600px;
    }
    .header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 20px;
      text-align: center;
    }
    .header h1 {
      font-size: 24px;
      margin-bottom: 5px;
    }
    .header p {
      font-size: 12px;
      opacity: 0.9;
    }
    .messages {
      flex: 1;
      overflow-y: auto;
      padding: 20px;
      background: #f8f9fa;
    }
    .message {
      margin-bottom: 15px;
      animation: slideIn 0.3s ease;
    }
    @keyframes slideIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
    .message.user {
      text-align: right;
    }
    .message.user .text {
      background: #667eea;
      color: white;
      border-radius: 12px 12px 0 12px;
    }
    .message.mundox .text {
      background: #e9ecef;
      color: #212529;
      border-radius: 12px 12px 12px 0;
    }
    .text {
      display: inline-block;
      padding: 10px 15px;
      max-width: 80%;
      word-wrap: break-word;
      font-size: 14px;
      line-height: 1.4;
    }
    .thinking {
      color: #667eea;
      font-style: italic;
      font-size: 12px;
    }
    .input-area {
      border-top: 1px solid #e9ecef;
      padding: 15px;
      background: white;
      display: flex;
      gap: 10px;
    }
    input {
      flex: 1;
      border: 1px solid #dee2e6;
      border-radius: 6px;
      padding: 10px 15px;
      font-size: 14px;
      font-family: inherit;
    }
    input:focus {
      outline: none;
      border-color: #667eea;
      box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    button {
      background: #667eea;
      color: white;
      border: none;
      border-radius: 6px;
      padding: 10px 20px;
      cursor: pointer;
      font-weight: 600;
      font-size: 14px;
      transition: background 0.2s;
    }
    button:hover {
      background: #5568d3;
    }
    button:disabled {
      background: #ccc;
      cursor: not-allowed;
    }
    .status {
      font-size: 11px;
      color: #999;
      text-align: center;
      padding: 5px;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>🧠 MundoX</h1>
      <p>Qwen3.5-27B (Claude Opus distilled) • GPU Native • Zero Cloud Tokens</p>
    </div>
    <div class="messages" id="messages"></div>
    <div class="input-area">
      <input
        type="text"
        id="input"
        placeholder="Ask MundoX anything..."
        autocomplete="off"
      />
      <button id="send" onclick="sendMessage()">Send</button>
    </div>
    <div class="status" id="status"></div>
  </div>

  <script>
    const messagesDiv = document.getElementById("messages");
    const input = document.getElementById("input");
    const sendBtn = document.getElementById("send");
    const statusDiv = document.getElementById("status");

    async function sendMessage() {
      const text = input.value.trim();
      if (!text || sendBtn.disabled) return;

      // Add user message
      addMessage("user", text);
      input.value = "";
      sendBtn.disabled = true;

      // Add thinking indicator
      const thinkingId = "thinking-" + Date.now();
      addMessage("mundox", "Thinking...", thinkingId);

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ prompt: text }),
        });

        const data = await response.json();

        if (data.error) {
          addMessage("mundox", "Error: " + data.error);
        } else {
          // Replace thinking with actual response
          const elem = document.getElementById(thinkingId);
          if (elem) elem.remove();
          addMessage("mundox", data.response);
        }
      } catch (err) {
        addMessage("mundox", "Connection error: " + err.message);
      } finally {
        sendBtn.disabled = false;
        input.focus();
      }
    }

    function addMessage(role, text, id = null) {
      const msg = document.createElement("div");
      msg.className = "message " + role;
      if (id) msg.id = id;
      const textEl = document.createElement("div");
      textEl.className = "text";
      textEl.textContent = text;
      msg.appendChild(textEl);
      messagesDiv.appendChild(msg);
      messagesDiv.scrollTop = messagesDiv.scrollHeight;
    }

    // Enter to send
    input.addEventListener("keypress", (e) => {
      if (e.key === "Enter") sendMessage();
    });

    // Check MundoX health
    async function checkHealth() {
      try {
        const res = await fetch("/api/health");
        const data = await res.json();
        statusDiv.textContent = "🟢 MundoX: " + data.model;
      } catch (err) {
        statusDiv.textContent = "🔴 MundoX offline";
      }
    }

    checkHealth();
    setInterval(checkHealth, 5000);
    input.focus();
  </script>
</body>
</html>
`;

// MundoX brain (Qwen3.5-27B) on :18791. Override via env if needed.
const MUNDOX_URL = process.env.MUNDOX_URL || "http://127.0.0.1:18791";
const MUNDOX_MODEL = process.env.MUNDOX_MODEL || "mundox";

// Health endpoint
fastify.get("/api/health", async (request, reply) => {
  try {
    const res = await fetch(`${MUNDOX_URL}/health`);
    const data = await res.json();
    return data;
  } catch (err) {
    return { error: "MundoX offline" };
  }
});

// Chat endpoint
fastify.post("/api/chat", async (request, reply) => {
  const { prompt } = request.body;

  try {
    const res = await fetch(`${MUNDOX_URL}/v1/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: MUNDOX_MODEL,
        messages: [{ role: "user", content: prompt }],
        max_tokens: 800,
        temperature: 0.7,
      }),
    });

    const data = await res.json();
    return { response: data.choices?.[0]?.message?.content || "No response" };
  } catch (err) {
    return { error: err.message };
  }
});

// Serve HTML
fastify.get("/", async (request, reply) => {
  return reply.type("text/html").send(htmlUI);
});

// Start server
fastify.listen({ port: 3334, host: "0.0.0.0" }, (err, address) => {
  if (err) {
    console.error("Failed to start MundoX Chat UI:", err);
    process.exit(1);
  }
  console.log("\n========================================");
  console.log("MundoX Chat UI");
  console.log("========================================");
  console.log(`\nOpen: http://localhost:3334\n`);
  console.log("- Local GPU inference only");
  console.log("- Zero Claude tokens");
  console.log("- MundoX brain (Qwen3.5-27B-Opus-Distilled)\n");
});
