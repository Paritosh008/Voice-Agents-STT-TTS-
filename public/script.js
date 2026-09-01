const state = {
  currentAudioObj: null,
  audioQueue: [],
  isPlaying: false,
  playbackSession: 0,
};

const BARGE_IN = {
  rmsThreshold: 0.1,
  consecutiveFrames: 5,
};

/* =========================================================
   LLM STREAMING
   Browser -> Node.js -> OpenAI
   ========================================================= */

async function* llmStreaming(userText = "") {
  const response = await fetch("/api/llm-stream", {
    method: "POST",

    headers: {
      "Content-Type": "application/json",
    },

    body: JSON.stringify({
      userText,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();

    throw new Error(
      `LLM API Error ${response.status}: ${errorText}`
    );
  }

  if (!response.body) {
    throw new Error("LLM response body is empty.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();

  let buffer = "";
  let textContent = "";
  let sentenceBuffer = "";

  function takeSentences(text) {
    const sentences = [];
    let rest = text;

    while (true) {
      // Detect ., ! or ?
      const end = rest.search(/[.!?]/);

      if (end === -1) {
        break;
      }

      const sentence = rest.slice(0, end + 1).trim();

      rest = rest.slice(end + 1);

      if (sentence) {
        sentences.push(sentence);
      }
    }

    return {
      sentences,
      rest,
    };
  }

  try {
    while (true) {
      const { value, done } = await reader.read();

      if (done) {
        break;
      }

      buffer += decoder.decode(value, {
        stream: true,
      });

      const events = buffer.split("\n\n");

      buffer = events.pop() || "";

      for (const event of events) {
        const payload = event
          .split("\n")
          .filter((line) => line.startsWith("data: "))
          .map((line) => line.slice(6))
          .join("\n");

        if (!payload || payload === "[DONE]") {
          continue;
        }

        let parsed;

        try {
          parsed = JSON.parse(payload);
        } catch (error) {
          console.warn(
            "Could not parse SSE payload:",
            payload
          );

          continue;
        }

        /*
         * OpenAI Responses API text delta
         */
        if (parsed.type === "response.output_text.delta") {
          const delta = parsed.delta || "";

          textContent += delta;
          sentenceBuffer += delta;

          const {
            sentences,
            rest,
          } = takeSentences(sentenceBuffer);

          sentenceBuffer = rest;

          for (const sentence of sentences) {
            yield {
              textContent,
              isFinal: false,
              delta: sentence,
            };
          }
        }

        /*
         * OpenAI Responses API text complete
         */
        if (parsed.type === "response.output_text.done") {
          if (parsed.text) {
            textContent = parsed.text;
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  /*
   * If final sentence doesn't contain
   * punctuation, speak it anyway.
   */
  const leftover = sentenceBuffer.trim();

  if (leftover) {
    yield {
      textContent,
      isFinal: true,
      delta: leftover,
    };
  }
}


/* =========================================================
   NORMAL LLM FUNCTION
   Currently not used by main().
   Kept here in case you need non-streaming LLM later.
   ========================================================= */

async function llm(userText = "") {
  const response = await fetch("/api/llm-stream", {
    method: "POST",

    headers: {
      "Content-Type": "application/json",
    },

    body: JSON.stringify({
      userText,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();

    throw new Error(
      `LLM API Error ${response.status}: ${errorText}`
    );
  }

  /*
   * This endpoint is streaming, so this helper
   * collects all streamed text.
   */

  const reader = response.body.getReader();
  const decoder = new TextDecoder();

  let buffer = "";
  let result = "";

  while (true) {
    const { value, done } = await reader.read();

    if (done) {
      break;
    }

    buffer += decoder.decode(value, {
      stream: true,
    });

    const events = buffer.split("\n\n");

    buffer = events.pop() || "";

    for (const event of events) {
      const payload = event
        .split("\n")
        .filter((line) => line.startsWith("data: "))
        .map((line) => line.slice(6))
        .join("\n");

      if (!payload || payload === "[DONE]") {
        continue;
      }

      try {
        const parsed = JSON.parse(payload);

        if (parsed.type === "response.output_text.delta") {
          result += parsed.delta || "";
        }
      } catch (error) {
        console.warn("LLM parsing error:", error);
      }
    }
  }

  return result;
}


/* =========================================================
   TTS
   Browser -> Node.js -> OpenAI TTS
   ========================================================= */

async function speak(text = "") {
  if (!text.trim()) {
    return;
  }

  const session = state.playbackSession;

  try {
    console.log("🔊 TTS request:", text);

    const response = await fetch("/api/tts", {
      method: "POST",

      headers: {
        "Content-Type": "application/json",
      },

      body: JSON.stringify({
        text,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();

      throw new Error(
        `TTS API Error ${response.status}: ${errorText}`
      );
    }

    const audioBlob = await response.blob();

    /*
     * User interrupted the agent while
     * TTS request was still running.
     */
    if (session !== state.playbackSession) {
      return;
    }

    state.audioQueue.push(audioBlob);

    /*
     * Start the audio worker only once.
     */
    if (!state.isPlaying) {
      processAudioQueue();
    }
  } catch (error) {
    console.error("❌ TTS error:", error);
  }
}


/* =========================================================
   AUDIO QUEUE WORKER
   ========================================================= */

async function processAudioQueue() {
  /*
   * Prevent multiple audio workers.
   */
  if (state.isPlaying) {
    return;
  }

  state.isPlaying = true;

  const session = state.playbackSession;

  try {
    while (
      state.audioQueue.length > 0 &&
      session === state.playbackSession
    ) {
      const queuedBlob = state.audioQueue.shift();

      if (!queuedBlob) {
        continue;
      }

      const audioUrl =
        URL.createObjectURL(queuedBlob);

      const audio = new Audio(audioUrl);

      await new Promise((resolve, reject) => {
        let finished = false;

        function cleanup() {
          if (finished) {
            return;
          }

          finished = true;

          if (
            state.currentAudioObj?.audio === audio
          ) {
            state.currentAudioObj = null;
          }

          URL.revokeObjectURL(audioUrl);
        }

        function handleEnded() {
          cleanup();
          resolve();
        }

        function handleError() {
          cleanup();

          reject(
            audio.error ||
              new Error("Audio playback failed")
          );
        }

        state.currentAudioObj = {
          audio,
          audioUrl,
          resolve,
          reject,
        };

        audio.onended = handleEnded;

        audio.onerror = handleError;

        audio
          .play()
          .catch((error) => {
            cleanup();
            reject(error);
          });
      });
    }
  } catch (error) {
    console.error(
      "❌ Audio playback error:",
      error
    );
  } finally {
    state.isPlaying = false;
  }
}


/* =========================================================
   INTERRUPT / BARGE-IN
   ========================================================= */

function interruptPlayback() {
  console.log("🛑 Interrupting playback");

  /*
   * Create a new playback session.
   * Any old TTS request returning later
   * will be ignored.
   */
  state.playbackSession += 1;

  if (state.currentAudioObj) {
    const {
      audio,
      audioUrl,
      resolve,
    } = state.currentAudioObj;

    audio.onended = null;
    audio.onerror = null;

    audio.pause();

    audio.src = "";

    URL.revokeObjectURL(audioUrl);

    state.currentAudioObj = null;

    /*
     * Resolve the Promise that is waiting
     * for the current audio to finish.
     */
    resolve?.();
  }

  /*
   * Remove all queued audio.
   */
  state.audioQueue = [];
}


/* =========================================================
   CHECK WHETHER AGENT IS SPEAKING
   ========================================================= */

function isAgentSpeaking() {
  return (
    Boolean(state.currentAudioObj) ||
    state.isPlaying ||
    state.audioQueue.length > 0
  );
}


/* =========================================================
   MICROPHONE RMS
   ========================================================= */

function getMicRms(analyser, samples) {
  analyser.getByteTimeDomainData(samples);

  let sum = 0;

  for (let i = 0; i < samples.length; i++) {
    const normalized =
      (samples[i] - 128) / 128;

    sum += normalized * normalized;
  }

  return Math.sqrt(
    sum / samples.length
  );
}


/* =========================================================
   BARGE-IN MONITOR
   ========================================================= */

async function startBargeInMonitor() {
  const stream =
    await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
      },
    });

  const audioContext =
    new AudioContext();

  const source =
    audioContext.createMediaStreamSource(
      stream
    );

  const analyser =
    audioContext.createAnalyser();

  analyser.fftSize = 2048;

  analyser.smoothingTimeConstant = 0.3;

  source.connect(analyser);

  if (audioContext.state === "suspended") {
    await audioContext.resume();
  }

  const samples =
    new Uint8Array(analyser.fftSize);

  let loudFrames = 0;

  function tick() {
    const rms =
      getMicRms(
        analyser,
        samples
      );

    if (
      isAgentSpeaking() &&
      rms >= BARGE_IN.rmsThreshold
    ) {
      loudFrames += 1;

      if (
        loudFrames >=
        BARGE_IN.consecutiveFrames
      ) {
        console.log(
          "🛑 Barge-in detected. RMS:",
          rms.toFixed(3)
        );

        interruptPlayback();

        loudFrames = 0;
      }
    } else {
      loudFrames = 0;
    }

    requestAnimationFrame(tick);
  }

  requestAnimationFrame(tick);

  console.log(
    "🎧 Barge-in monitor started"
  );
}


/* =========================================================
   MAIN VOICE AGENT
   ========================================================= */

async function main() {
  /*
   * Chrome supports webkitSpeechRecognition.
   * Some browsers expose SpeechRecognition.
   */
  const SpeechRecognition =
    window.SpeechRecognition ||
    window.webkitSpeechRecognition;

  if (!SpeechRecognition) {
    console.error(
      "❌ Speech Recognition is not supported in this browser."
    );

    alert(
      "Speech Recognition is not supported. Please use Google Chrome."
    );

    return;
  }

  const speechRecognition =
    new SpeechRecognition();

  speechRecognition.continuous = true;

  speechRecognition.interimResults = false;

  speechRecognition.maxAlternatives = 1;


  /* =======================================================
     SPEECH START
     ======================================================= */

  speechRecognition.onstart =
    function () {
      console.log(
        "🎤 SpeechRecognition has started"
      );
    };


  /* =======================================================
     SPEECH RESULT
     ======================================================= */

  speechRecognition.onresult =
    async function (event) {
      const result =
        event.results[
          event.results.length - 1
        ];

      if (!result || !result[0]) {
        return;
      }

      const transcript =
        result[0].transcript.trim();

      if (!transcript) {
        return;
      }

      console.log(
        "👤 User:",
        transcript
      );

      /*
       * Stop agent speech immediately
       * when user starts a new request.
       */
      interruptPlayback();

      try {
        /*
         * Stream LLM response sentence by sentence.
         */
        for await (
          const chunk of llmStreaming(
            transcript
          )
        ) {
          console.log(
            "🤖 Agent chunk:",
            chunk.delta
          );

          /*
           * Send each complete sentence
           * to TTS.
           */
          speak(chunk.delta);
        }
      } catch (error) {
        console.error(
          "❌ LLM streaming error:",
          error
        );
      }
    };


  /* =======================================================
     SPEECH ERROR
     ======================================================= */

  speechRecognition.onerror =
    function (event) {
      console.error(
        "❌ SpeechRecognition error:",
        event.error
      );
    };


  /* =======================================================
     SPEECH END
     ======================================================= */

  speechRecognition.onend =
    function () {
      console.log(
        "🎤 SpeechRecognition ended"
      );

      /*
       * Restart recognition automatically.
       *
       * Chrome can stop continuous recognition
       * after silence.
       */
      try {
        speechRecognition.start();
      } catch (error) {
        /*
         * Ignore "already started" errors.
         */
      }
    };


  /* =======================================================
     START BARGE-IN MONITOR
     ======================================================= */

  try {
    await startBargeInMonitor();
  } catch (error) {
    console.error(
      "❌ Microphone monitor error:",
      error
    );

    return;
  }


  /* =======================================================
     START SPEECH RECOGNITION
     ======================================================= */

  try {
    speechRecognition.start();
  } catch (error) {
    console.error(
      "❌ Could not start SpeechRecognition:",
      error
    );
  }
}


/* =========================================================
   START APPLICATION
   ========================================================= */

main().catch((error) => {
  console.error(
    "❌ Application startup error:",
    error
  );
});