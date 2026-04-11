import { readFileSync } from "node:fs";
const img = readFileSync("C:/Users/msanc/mxre/data/labeling-sample/fidlar-202600004828-full/page01.png");
const b64 = img.toString("base64");
const resp = await fetch("http://127.0.0.1:18791/v1/chat/completions", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    model: "mundox",
    messages: [{
      role: "user",
      content: [
        { type: "text", text: "What do you see? Describe document type and quote the first 5 lines of printed text." },
        { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
      ],
    }],
    max_tokens: 500,
    temperature: 0.1,
  }),
});
const d = await resp.json();
console.log(d.choices?.[0]?.message?.content || JSON.stringify(d).slice(0, 400));
