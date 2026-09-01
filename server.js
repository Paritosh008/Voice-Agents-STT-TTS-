const express = require("express");
const dotenv = require("dotenv");
const path = require("path");

dotenv.config();

const app = express();
const PORT = 3000;

const API_KEY = process.env.OPENAI_API_KEY;

if (!API_KEY) {
  console.error("❌ OPENAI_API_KEY is missing in .env");
  process.exit(1);
}

app.use(express.json());

/*
 * Serve frontend
 */
app.use(express.static(path.join(__dirname, "public")));

/*
 * LLM streaming
 */
app.post("/api/llm-stream", async (req, res) => {
  try {
    const { userText = "" } = req.body;

    console.log("🧠 User:", userText);

    const response = await fetch(
      "https://api.openai.com/v1/responses",
      {
        method: "POST",

        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${API_KEY}`,
        },

        body: JSON.stringify({
          model: "gpt-5.6-luna",

          input: `You are part of a Speech To Text and Text To Speech pipeline.

Always answer in complete sentences so that the response can be converted to speech sentence by sentence.

User Query:
${userText}`,

          stream: true,
        }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();

      console.error(
        "❌ OpenAI LLM Error:",
        response.status,
        errorText
      );

      res.status(response.status).json({
        error: errorText,
      });

      return;
    }

    /*
     * Tell browser this is SSE
     */
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });

    /*
     * Forward OpenAI stream to browser
     */
    const reader = response.body.getReader();

    try {
      while (true) {
        const { value, done } = await reader.read();

        if (done) {
          break;
        }

        res.write(Buffer.from(value));
      }
    } finally {
      reader.releaseLock();
      res.end();
    }
  } catch (error) {
    console.error("❌ LLM server error:", error);

    if (!res.headersSent) {
      res.status(500).json({
        error: error.message,
      });
    } else {
      res.end();
    }
  }
});

/*
 * Text To Speech
 */
app.post("/api/tts", async (req, res) => {
  try {
    const {
      text = "",
    } = req.body;

    if (!text.trim()) {
      return res.status(400).json({
        error: "Text is required",
      });
    }

    console.log("🔊 TTS:", text);

    const response = await fetch(
      "https://api.openai.com/v1/audio/speech",
      {
        method: "POST",

        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${API_KEY}`,
        },

        body: JSON.stringify({
          model: "gpt-4o-mini-tts",
          voice: "coral",
          input: text,
          instructions:
            "Speak in a cheerful, warm and natural tone.",
        }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();

      console.error(
        "❌ OpenAI TTS Error:",
        response.status,
        errorText
      );

      res.status(response.status).json({
        error: errorText,
      });

      return;
    }

    const audioBuffer = Buffer.from(
      await response.arrayBuffer()
    );

    res.setHeader("Content-Type", "audio/mpeg");

    res.send(audioBuffer);
  } catch (error) {
    console.error("❌ TTS server error:", error);

    res.status(500).json({
      error: error.message,
    });
  }
});

/*
 * Start server
 */
app.listen(PORT, () => {
  console.log("");
  console.log("🚀 Voice Agent server running");
  console.log(`👉 http://localhost:${PORT}`);
  console.log("");
});