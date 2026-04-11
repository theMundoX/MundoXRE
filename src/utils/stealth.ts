/**
 * Stealth configuration for Playwright browser contexts.
 * Generates current-version user agents and randomized fingerprints.
 */

// Base Chrome version as of early 2024; incremented by elapsed months
const CHROME_BASE_VERSION = 122;
const CHROME_BASE_DATE = new Date("2024-02-01");

function currentChromeVersion(): number {
  const elapsed = Date.now() - CHROME_BASE_DATE.getTime();
  const months = Math.floor(elapsed / (30 * 24 * 60 * 60 * 1000));
  return CHROME_BASE_VERSION + months;
}

function generateUserAgents(): string[] {
  const v = currentChromeVersion();
  return [
    `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v - 1}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
    `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15`,
    `Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:${v + 5}.0) Gecko/20100101 Firefox/${v + 5}.0`,
    `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/${v}.0.0.0 Safari/537.36`,
  ];
}

const VIEWPORTS = [
  { width: 1920, height: 1080 },
  { width: 1536, height: 864 },
  { width: 1440, height: 900 },
  { width: 1366, height: 768 },
  { width: 1280, height: 720 },
];

const TIMEZONES = [
  "America/Chicago",
  "America/New_York",
  "America/Denver",
  "America/Los_Angeles",
];

function randomItem<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

/**
 * Generate a randomized browser context config for Playwright.
 */
export function getStealthConfig() {
  return {
    userAgent: randomItem(generateUserAgents()),
    viewport: randomItem(VIEWPORTS),
    timezoneId: randomItem(TIMEZONES),
    locale: "en-US",
    extraHTTPHeaders: {
      "Accept-Language": "en-US,en;q=0.9",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
    },
    javaScriptEnabled: true,
    // Do NOT set ignoreHTTPSErrors — we want proper cert validation
  };
}

/**
 * Init script to inject into every page to hide automation signals.
 * Call via page.addInitScript() in Playwright.
 */
export const STEALTH_INIT_SCRIPT = `
  // Hide navigator.webdriver
  Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

  // Override navigator.plugins to appear non-empty
  Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5],
  });

  // Override navigator.languages
  Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
  });

  // Mask Chrome automation indicators
  if (window.chrome) {
    window.chrome.runtime = undefined;
  }

  // Canvas fingerprint noise — adds imperceptible pixel-level variation
  const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
  HTMLCanvasElement.prototype.toDataURL = function(type) {
    const ctx = this.getContext('2d');
    if (ctx) {
      const style = ctx.fillStyle;
      ctx.fillStyle = 'rgba(1,1,1,0.01)';
      ctx.fillRect(0, 0, 1, 1);
      ctx.fillStyle = style;
    }
    return origToDataURL.apply(this, arguments);
  };

  // WebGL renderer randomization
  const getParam = WebGLRenderingContext.prototype.getParameter;
  WebGLRenderingContext.prototype.getParameter = function(param) {
    if (param === 37445) return 'Intel Inc.';
    if (param === 37446) return 'Intel Iris OpenGL Engine';
    return getParam.apply(this, arguments);
  };

  // AudioContext fingerprint noise
  const origGetFloatFrequencyData = AnalyserNode.prototype.getFloatFrequencyData;
  AnalyserNode.prototype.getFloatFrequencyData = function(array) {
    origGetFloatFrequencyData.call(this, array);
    for (let i = 0; i < array.length; i++) {
      array[i] += (Math.random() - 0.5) * 0.1;
    }
  };
`;

/**
 * Get a random user agent string (for non-Playwright HTTP requests).
 */
export function getRandomUserAgent(): string {
  return randomItem(generateUserAgents());
}

// ─── Human-Like Behavior (for listing scrapers) ─────────────────────

/**
 * Simulate human-like scrolling on a page.
 * Scrolls down in increments with random pauses.
 */
export async function humanScroll(page: import("playwright").Page): Promise<void> {
  const scrolls = 2 + Math.floor(Math.random() * 3); // 2-4 scrolls
  for (let i = 0; i < scrolls; i++) {
    const distance = 200 + Math.floor(Math.random() * 400); // 200-600px
    await page.mouse.wheel(0, distance);
    await page.waitForTimeout(300 + Math.floor(Math.random() * 700)); // 300-1000ms
  }
}

/**
 * Random mouse movement to look alive.
 */
export async function humanMouseMove(page: import("playwright").Page): Promise<void> {
  const x = 100 + Math.floor(Math.random() * 800);
  const y = 100 + Math.floor(Math.random() * 500);
  await page.mouse.move(x, y, { steps: 5 + Math.floor(Math.random() * 10) });
}

/**
 * Wait a human-like amount of time (1-3 seconds).
 */
export async function humanPause(page: import("playwright").Page): Promise<void> {
  await page.waitForTimeout(1000 + Math.floor(Math.random() * 2000));
}
