import { chromium } from "playwright";
const browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.goto("https://records.manateeclerk.com/LandmarkWeb", { waitUntil: "networkidle", timeout: 30000 }).catch(e => console.log("err:", e.message));
console.log("title:", await page.title());
console.log("url:", page.url());
console.log("length:", (await page.content()).length);
await browser.close();
