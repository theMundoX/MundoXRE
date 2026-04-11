#!/usr/bin/env node
/**
 * MundoX Chat — Direct conversation with the local MundoX brain
 * Completely independent from cloud. GPU-native only.
 *
 * Usage: node chat-mundox.mjs
 */

import readline from "readline";

// Default to mundox-brain (27B reasoning) on :18791. Fall back to worker on :18792.
const MUNDOX_URL = process.env.MUNDOX_URL || "http://127.0.0.1:18791";
const MODEL_ALIAS = process.env.MUNDOX_MODEL || "mundox";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function chatWithMundoX(prompt) {
  try {
    const response = await fetch(`${MUNDOX_URL}/v1/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: MODEL_ALIAS,
        messages: [{ role: "user", content: prompt }],
        max_tokens: 800,
        temperature: 0.7,
      }),
    });

    if (!response.ok) {
      throw new Error(`MundoX error: ${response.status}`);
    }

    const data = await response.json();
    return data.choices?.[0]?.message?.content || "No response from MundoX";
  } catch (err) {
    return `Error: ${err.message}`;
  }
}

async function main() {
  console.log("========================================");
  console.log("MundoX Chat - Local GPU Only");
  console.log("Model: Qwen3.5-27B (MundoX brain)");
  console.log(`Endpoint: ${MUNDOX_URL}`);
  console.log("========================================\n");

  // Check MundoX is alive
  try {
    const health = await fetch(`${MUNDOX_URL}/health`);
    const status = await health.json();
    console.log(`✓ MundoX Ready: ${JSON.stringify(status)}\n`);
  } catch (err) {
    console.error(`✗ MundoX not responding at ${MUNDOX_URL}`);
    console.error("Start it: powershell C:\\Users\\msanc\\mundox-services\\start-mundox-brain.ps1");
    process.exit(1);
  }

  console.log('Type your message (Ctrl+C to exit):\n');

  const askQuestion = () => {
    rl.question("You: ", async (input) => {
      if (!input.trim()) {
        askQuestion();
        return;
      }

      console.log("\nMundoX: Thinking...");
      const response = await chatWithMundoX(input);
      console.log(`MundoX: ${response}\n`);

      askQuestion();
    });
  };

  askQuestion();
}

main().catch(console.error);
