import { readFileSync } from "node:fs";

const img = readFileSync("C:/Users/msanc/mxre/data/labeling-sample/fidlar-201900009659/page1.png");
const b64 = img.toString("base64");

const resp = await fetch("http://127.0.0.1:18791/v1/chat/completions", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    model: "mundox",
    messages: [
      {
        role: "user",
        content: [
          {
            type: "text",
            text: "Describe this image in detail. Is it a mortgage document, a blank page, a website with UI controls, or something else? If you see any text from a document, quote the first 200 characters you can read.",
          },
          { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
        ],
      },
    ],
    max_tokens: 500,
    temperature: 0.1,
  }),
});

const data = await resp.json();
console.log(data.choices?.[0]?.message?.content || JSON.stringify(data).slice(0, 500));
