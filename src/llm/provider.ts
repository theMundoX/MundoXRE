/**
 * LLM Provider Abstraction
 * Supports Claude (Anthropic API) and the local MundoX worker (llama.cpp on Qwen3-8B)
 * Toggle via environment variable or per-request.
 *
 * MundoX worker endpoint: http://127.0.0.1:18792/v1 (OpenAI-compatible)
 * Configure with MUNDOX_WORKER_URL env var.
 */

export type LLMProvider = "claude" | "mundox";

export interface LLMRequest {
  prompt: string;
  maxTokens?: number;
  temperature?: number;
  system?: string;
  provider?: LLMProvider; // Override default
}

export interface LLMResponse {
  text: string;
  tokensUsed: {
    input: number;
    output: number;
  };
  provider: LLMProvider;
}

class AnthropicProvider {
  private apiKey: string;

  constructor(apiKey: string) {
    this.apiKey = apiKey;
  }

  async call(request: LLMRequest): Promise<LLMResponse> {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": this.apiKey,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "claude-opus-4-1",
        max_tokens: request.maxTokens || 2048,
        temperature: request.temperature || 0.7,
        system: request.system,
        messages: [
          {
            role: "user",
            content: request.prompt,
          },
        ],
      }),
    });

    if (!response.ok) {
      throw new Error(`Claude API error: ${response.statusText}`);
    }

    const data: any = await response.json();
    return {
      text: data.content[0]?.text || "",
      tokensUsed: {
        input: data.usage?.input_tokens || 0,
        output: data.usage?.output_tokens || 0,
      },
      provider: "claude",
    };
  }
}

/**
 * MundoX worker — llama.cpp serving Qwen3-8B Q4_K_M on the local GPU.
 * OpenAI-compatible /v1/chat/completions endpoint.
 */
class MundoxWorkerProvider {
  private url: string;
  private model: string;

  constructor(url: string = "http://127.0.0.1:18791", model: string = "mundox") {
    this.url = url;
    this.model = model;
  }

  async call(request: LLMRequest): Promise<LLMResponse> {
    const messages: Array<{ role: string; content: string }> = [];
    if (request.system) {
      messages.push({ role: "system", content: request.system });
    }
    messages.push({ role: "user", content: request.prompt });

    const response = await fetch(`${this.url}/v1/chat/completions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: this.model,
        messages,
        max_tokens: request.maxTokens || 500,
        temperature: request.temperature ?? 0.1,
        // Qwen3 is a reasoning model — disable <think> blocks for extraction
        // tasks where we just want the JSON, not the reasoning trace.
        chat_template_kwargs: { enable_thinking: false },
      }),
    });

    if (!response.ok) {
      throw new Error(
        `MundoX worker error: ${response.statusText}. Start: powershell C:\\Users\\msanc\\mundox-services\\start-mundox-worker.ps1`
      );
    }

    const data: any = await response.json();
    return {
      text: data.choices?.[0]?.message?.content || "",
      tokensUsed: {
        input: data.usage?.prompt_tokens || 0,
        output: data.usage?.completion_tokens || 0,
      },
      provider: "mundox",
    };
  }
}

export class LLMRouter {
  private claudeProvider: AnthropicProvider | null = null;
  private mundoxProvider: MundoxWorkerProvider;
  private defaultProvider: LLMProvider;

  constructor(options?: { anthropicApiKey?: string; mundoxUrl?: string; defaultProvider?: LLMProvider }) {
    if (options?.anthropicApiKey) {
      this.claudeProvider = new AnthropicProvider(options.anthropicApiKey);
    }

    this.mundoxProvider = new MundoxWorkerProvider(options?.mundoxUrl);
    this.defaultProvider = options?.defaultProvider || "mundox";
  }

  async call(request: LLMRequest): Promise<LLMResponse> {
    const provider = request.provider || this.defaultProvider;

    if (provider === "claude") {
      if (!this.claudeProvider) {
        throw new Error("Claude provider not configured. Set ANTHROPIC_API_KEY environment variable.");
      }
      return this.claudeProvider.call(request);
    }

    if (provider === "mundox") {
      return this.mundoxProvider.call(request);
    }

    throw new Error(`Unknown provider: ${provider}`);
  }

  setDefaultProvider(provider: LLMProvider) {
    this.defaultProvider = provider;
  }

  getDefaultProvider(): LLMProvider {
    return this.defaultProvider;
  }

  // Test connectivity to the MundoX worker
  async testMundox(): Promise<boolean> {
    try {
      const url = process.env.MUNDOX_WORKER_URL || "http://127.0.0.1:18791";
      const response = await fetch(`${url}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }

  async testClaude(): Promise<boolean> {
    if (!this.claudeProvider) return false;
    try {
      await this.claudeProvider.call({
        prompt: "Say 'ok'",
        maxTokens: 10,
      });
      return true;
    } catch {
      return false;
    }
  }
}

// Singleton instance
let router: LLMRouter | null = null;

export function getOrCreateRouter(): LLMRouter {
  if (!router) {
    router = new LLMRouter({
      anthropicApiKey: process.env.ANTHROPIC_API_KEY,
      mundoxUrl: process.env.MUNDOX_WORKER_URL || "http://127.0.0.1:18791",
      defaultProvider: (process.env.LLM_PROVIDER as LLMProvider) || "mundox",
    });
  }
  return router;
}
