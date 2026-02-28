import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { extname, join, resolve } from "node:path";
import {
  closeDb,
  getCorpusStats,
  ingestTextsFromFolder,
} from "./knowledge-db.ts";
import {
  chatWithNihiltheismGenius,
  getNihiltheismBrainStatus,
} from "./nihiltheism-brain.ts";

const HOST = process.env.UPLOAD_UI_HOST || "127.0.0.1";
const PORT = Number(process.env.UPLOAD_UI_PORT || 8791);
const TEXTS_DIR = resolve("./texts");
const ALLOWED_EXTENSIONS = new Set([".txt", ".md", ".markdown"]);
const configuredMaxBytes = Number(process.env.UPLOAD_UI_MAX_BYTES);
const MAX_FILE_SIZE_BYTES = Number.isFinite(configuredMaxBytes) && configuredMaxBytes > 0
  ? configuredMaxBytes
  : 20 * 1024 * 1024;

ensureDirectory(TEXTS_DIR);

interface UploadResult {
  originalName: string;
  savedAs?: string;
  status: "saved" | "skipped";
  reason?: string;
}

function ensureDirectory(path: string) {
  if (existsSync(path)) {
    const stats = statSync(path);
    if (!stats.isDirectory()) {
      throw new Error(`Expected folder, found file: ${path}`);
    }
    return;
  }
  mkdirSync(path, { recursive: true });
}

function jsonResponse(payload: unknown, status: number = 200): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function sanitizeFileName(name: string): string {
  const trimmed = name.trim();
  const withoutPath = trimmed.split(/[\\/]/).pop() || "upload.txt";
  const cleaned = withoutPath.replace(/[^a-zA-Z0-9._-]/g, "-");
  const compact = cleaned.replace(/-+/g, "-").replace(/^-|-$/g, "");
  return compact || `upload-${Date.now()}.txt`;
}

function parseFolderSegments(folder: string): string[] {
  return folder
    .split(/[\\/]/)
    .map(part => part.trim())
    .filter(Boolean)
    .filter(part => part !== "." && part !== "..")
    .map(part => part.replace(/[^a-zA-Z0-9_-]/g, "-"))
    .filter(Boolean);
}

function getUniqueTargetPath(baseDir: string, fileName: string): { fullPath: string; finalName: string } {
  const extension = extname(fileName).toLowerCase();
  const stem = fileName.slice(0, fileName.length - extension.length) || "upload";

  let count = 0;
  let finalName = fileName;
  let fullPath = join(baseDir, finalName);

  while (existsSync(fullPath)) {
    count += 1;
    finalName = `${stem}-${count}${extension}`;
    fullPath = join(baseDir, finalName);
  }

  return { fullPath, finalName };
}

function listTextFiles(rootDir: string): string[] {
  if (!existsSync(rootDir)) {
    return [];
  }

  const stack = [rootDir];
  const files: string[] = [];

  while (stack.length > 0) {
    const current = stack.pop() as string;
    const entries = readdirSync(current);

    for (const entry of entries) {
      const fullPath = join(current, entry);
      const stats = statSync(fullPath);
      if (stats.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      const extension = extname(entry).toLowerCase();
      if (ALLOWED_EXTENSIONS.has(extension)) {
        files.push(fullPath.replace(process.cwd(), ".").replace(/\\/g, "/"));
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b));
}

function openFolderInExplorer(path: string): boolean {
  try {
    if (process.platform === "win32") {
      const child = spawn("explorer.exe", [path], { detached: true, stdio: "ignore" });
      child.unref();
      return true;
    }

    if (process.platform === "darwin") {
      const child = spawn("open", [path], { detached: true, stdio: "ignore" });
      child.unref();
      return true;
    }

    const child = spawn("xdg-open", [path], { detached: true, stdio: "ignore" });
    child.unref();
    return true;
  } catch {
    return false;
  }
}

function appStatusPayload() {
  return {
    ok: true,
    corpus: getCorpusStats(),
    brain: getNihiltheismBrainStatus(),
    files: {
      textsDir: TEXTS_DIR,
      textFileCount: listTextFiles(TEXTS_DIR).length,
      maxFileSizeBytes: MAX_FILE_SIZE_BYTES,
    },
  };
}

async function handleUpload(request: Request): Promise<Response> {
  let formData: FormData;
  try {
    formData = await request.formData();
  } catch {
    return jsonResponse({ ok: false, error: "Invalid multipart form data." }, 400);
  }

  const rawFolder = String(formData.get("folder") || "");
  const folderSegments = parseFolderSegments(rawFolder);
  const targetDir = join(TEXTS_DIR, ...folderSegments);

  try {
    ensureDirectory(targetDir);
  } catch (error) {
    return jsonResponse(
      {
        ok: false,
        error: "Could not prepare destination folder.",
        details: error instanceof Error ? error.message : String(error),
      },
      500
    );
  }

  const files = formData
    .getAll("files")
    .filter((item): item is File => item instanceof File);

  if (files.length === 0) {
    return jsonResponse({ ok: false, error: "No files uploaded." }, 400);
  }

  const uploaded: UploadResult[] = [];

  for (const file of files) {
    const safeName = sanitizeFileName(file.name);
    const extension = extname(safeName).toLowerCase();

    if (!ALLOWED_EXTENSIONS.has(extension)) {
      uploaded.push({
        originalName: file.name,
        status: "skipped",
        reason: `Unsupported extension: ${extension || "(none)"}`,
      });
      continue;
    }

    if (file.size <= 0) {
      uploaded.push({
        originalName: file.name,
        status: "skipped",
        reason: "File is empty.",
      });
      continue;
    }

    if (file.size > MAX_FILE_SIZE_BYTES) {
      uploaded.push({
        originalName: file.name,
        status: "skipped",
        reason: `File exceeds limit (${MAX_FILE_SIZE_BYTES} bytes).`,
      });
      continue;
    }

    try {
      const { fullPath, finalName } = getUniqueTargetPath(targetDir, safeName);
      const bytes = new Uint8Array(await file.arrayBuffer());
      writeFileSync(fullPath, bytes);
      uploaded.push({
        originalName: file.name,
        savedAs: finalName,
        status: "saved",
      });
    } catch {
      uploaded.push({
        originalName: file.name,
        status: "skipped",
        reason: "Write failed.",
      });
    }
  }

  try {
    const sync = ingestTextsFromFolder("./texts");
    return jsonResponse({ ok: true, uploaded, sync, status: appStatusPayload() });
  } catch (error) {
    return jsonResponse(
      {
        ok: false,
        uploaded,
        error: "Upload completed, but sync failed.",
        details: error instanceof Error ? error.message : String(error),
      },
      500
    );
  }
}

function handleSync(): Response {
  try {
    const sync = ingestTextsFromFolder("./texts");
    return jsonResponse({ ok: true, sync, status: appStatusPayload() });
  } catch (error) {
    return jsonResponse(
      {
        ok: false,
        error: "Sync failed.",
        details: error instanceof Error ? error.message : String(error),
      },
      500
    );
  }
}

async function handleChat(request: Request): Promise<Response> {
  let payload: { message?: unknown; history?: unknown };
  try {
    payload = await request.json() as { message?: unknown; history?: unknown };
  } catch {
    return jsonResponse({ ok: false, error: "Invalid JSON body." }, 400);
  }

  const message = typeof payload.message === "string" ? payload.message.trim() : "";
  if (!message) {
    return jsonResponse({ ok: false, error: "message is required." }, 400);
  }

  const history = Array.isArray(payload.history)
    ? payload.history
        .filter(item => item && typeof item === "object")
        .map(item => {
          const role = (item as { role?: unknown }).role;
          const content = (item as { content?: unknown }).content;
          if ((role !== "user" && role !== "assistant") || typeof content !== "string") {
            return null;
          }
          return { role, content };
        })
        .filter((item): item is { role: "user" | "assistant"; content: string } => item !== null)
    : [];

  const response = await chatWithNihiltheismGenius({ message, history });
  return jsonResponse(response);
}

function handleOpenTextsFolder(): Response {
  const ok = openFolderInExplorer(TEXTS_DIR);
  if (!ok) {
    return jsonResponse({ ok: false, error: "Could not open system file explorer." }, 500);
  }
  return jsonResponse({ ok: true, folder: TEXTS_DIR });
}

function htmlPage(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Nihiltheism Genius</title>
  <style>
    :root {
      --bg-1: #f2ece3;
      --bg-2: #e7eee8;
      --ink: #1e2a2e;
      --muted: #607279;
      --card: #ffffffdd;
      --line: #cdd8d1;
      --primary: #265f4a;
      --secondary: #e7efe9;
      --accent: #9d5738;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", Tahoma, sans-serif;
      color: var(--ink);
      min-height: 100vh;
      background:
        radial-gradient(1000px 650px at 90% -10%, #c9e2d2 0%, transparent 60%),
        radial-gradient(900px 600px at -20% 120%, #ebd8c7 0%, transparent 60%),
        linear-gradient(140deg, var(--bg-1), var(--bg-2));
      padding: 20px;
      display: grid;
      place-items: center;
    }
    .app {
      width: min(1100px, 100%);
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--card);
      box-shadow: 0 18px 44px rgba(26, 32, 30, 0.13);
      overflow: hidden;
      backdrop-filter: blur(8px);
    }
    .head {
      padding: 20px 22px 14px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(120deg, #ffffffa8, #f4faf6cf);
    }
    h1 {
      margin: 0;
      font-size: clamp(1.2rem, 2.5vw, 1.8rem);
      letter-spacing: 0.2px;
    }
    .sub {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 0.95rem;
    }
    .content {
      padding: 16px 20px 20px;
      display: grid;
      gap: 14px;
    }
    .row {
      display: grid;
      grid-template-columns: 1fr auto auto auto;
      gap: 10px;
      align-items: center;
    }
    .folderInput {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 0.93rem;
      background: #fff;
      color: var(--ink);
      width: 100%;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 0.9rem;
      font-weight: 650;
      cursor: pointer;
      transition: transform 0.12s ease, opacity 0.12s ease;
      white-space: nowrap;
    }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.6; transform: none; cursor: not-allowed; }
    .primary { background: var(--primary); color: #fff; }
    .secondary { background: var(--secondary); color: #24443a; }
    .drop {
      border: 2px dashed #86a798;
      border-radius: 12px;
      background: #f7fcf8;
      text-align: center;
      padding: 24px 14px;
      cursor: pointer;
      transition: background 0.15s ease, border-color 0.15s ease;
    }
    .drop.active { border-color: var(--primary); background: #eef8f1; }
    .drop strong {
      display: block;
      margin-bottom: 6px;
      font-size: 1rem;
    }
    .drop span { color: var(--muted); font-size: 0.9rem; }
    .status {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      padding: 12px;
      min-height: 100px;
      font-size: 0.82rem;
      line-height: 1.35;
      white-space: pre-wrap;
      font-family: ui-monospace, "Cascadia Code", Consolas, monospace;
    }
    .chatPanel {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      padding: 12px;
      display: grid;
      gap: 10px;
    }
    .chatTitle {
      margin: 0;
      font-size: 0.92rem;
      text-transform: uppercase;
      letter-spacing: 0.7px;
      color: #324d56;
      font-weight: 700;
    }
    .chatLog {
      border: 1px solid var(--line);
      border-radius: 10px;
      min-height: 180px;
      max-height: 360px;
      overflow-y: auto;
      padding: 10px;
      display: grid;
      gap: 8px;
      background: #fbfdfb;
    }
    .bubble {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      white-space: pre-wrap;
      line-height: 1.35;
      font-size: 0.9rem;
    }
    .user { background: #edf7f2; border-color: #bfd7ca; }
    .assistant { background: #fff5ee; border-color: #e7d4c5; }
    .chatRow {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: start;
    }
    .chatInput {
      width: 100%;
      min-height: 82px;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font-size: 0.94rem;
      resize: vertical;
      font-family: inherit;
    }
    @media (max-width: 900px) {
      .row { grid-template-columns: 1fr; }
      .chatRow { grid-template-columns: 1fr; }
      button { width: 100%; }
    }
  </style>
</head>
<body>
  <main class="app">
    <section class="head">
      <h1>Nihiltheism Genius</h1>
      <p class="sub">Upload notes, sync to SQLite, and chat with a nihiltheism-focused research assistant grounded in your corpus.</p>
    </section>
    <section class="content">
      <div id="dropZone" class="drop" role="button" tabindex="0" aria-label="Upload files">
        <strong>Drop files here or click to choose</strong>
        <span>Supported: .md, .markdown, .txt (max ${Math.floor(MAX_FILE_SIZE_BYTES / (1024 * 1024))} MB per file)</span>
      </div>
      <input id="fileInput" type="file" accept=".md,.markdown,.txt,text/plain,text/markdown" multiple hidden />
      <div class="row">
        <input id="folderInput" class="folderInput" type="text" placeholder="optional subfolder inside texts/ (example: journals/2026)" />
        <button id="uploadBtn" class="primary">Upload + Sync</button>
        <button id="syncBtn" class="secondary">Sync Existing</button>
        <button id="openBtn" class="secondary">Open Texts Folder</button>
      </div>
      <div id="status" class="status">Ready.</div>
      <section class="chatPanel">
        <h2 class="chatTitle">Ask The Nihiltheism Genius</h2>
        <div id="chatLog" class="chatLog"></div>
        <div class="chatRow">
          <textarea id="chatInput" class="chatInput" placeholder="Ask a deep nihiltheistic question..."></textarea>
          <button id="chatBtn" class="primary">Send</button>
        </div>
      </section>
    </section>
  </main>
  <script>
    const dropZone = document.getElementById("dropZone");
    const fileInput = document.getElementById("fileInput");
    const folderInput = document.getElementById("folderInput");
    const uploadBtn = document.getElementById("uploadBtn");
    const syncBtn = document.getElementById("syncBtn");
    const openBtn = document.getElementById("openBtn");
    const statusBox = document.getElementById("status");
    const chatLog = document.getElementById("chatLog");
    const chatInput = document.getElementById("chatInput");
    const chatBtn = document.getElementById("chatBtn");

    let selectedFiles = [];
    const chatHistory = [];

    function setStatus(value) {
      statusBox.textContent = value;
    }

    function appendChat(role, text) {
      const div = document.createElement("div");
      div.className = "bubble " + role;
      div.textContent = text;
      chatLog.appendChild(div);
      chatLog.scrollTop = chatLog.scrollHeight;
    }

    function pushHistory(role, content) {
      chatHistory.push({ role, content });
      if (chatHistory.length > 20) {
        chatHistory.splice(0, chatHistory.length - 20);
      }
    }

    async function parseResponsePayload(response) {
      const text = await response.text();
      try {
        const parsed = JSON.parse(text);
        if (response.ok) {
          return parsed;
        }
        return { ok: false, status: response.status, ...parsed };
      } catch {
        return {
          ok: false,
          status: response.status,
          error: "Server returned non-JSON response.",
          preview: text.slice(0, 600)
        };
      }
    }

    function describeSelectedFiles() {
      if (selectedFiles.length === 0) {
        setStatus("Ready. No files selected.");
        return;
      }
      const list = selectedFiles.map(f => "- " + f.name + " (" + f.size + " bytes)").join("\\n");
      setStatus("Selected files:\\n" + list);
    }

    function applyFiles(list) {
      selectedFiles = Array.from(list || []);
      const dataTransfer = new DataTransfer();
      selectedFiles.forEach(file => dataTransfer.items.add(file));
      fileInput.files = dataTransfer.files;
      describeSelectedFiles();
    }

    dropZone.addEventListener("click", () => fileInput.click());
    dropZone.addEventListener("keydown", event => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        fileInput.click();
      }
    });
    dropZone.addEventListener("dragover", event => {
      event.preventDefault();
      dropZone.classList.add("active");
    });
    dropZone.addEventListener("dragleave", () => dropZone.classList.remove("active"));
    dropZone.addEventListener("drop", event => {
      event.preventDefault();
      dropZone.classList.remove("active");
      applyFiles(event.dataTransfer.files);
    });
    fileInput.addEventListener("change", () => applyFiles(fileInput.files));

    async function refreshStatus() {
      try {
        const response = await fetch("/api/status");
        const payload = await parseResponsePayload(response);
        if (!payload.ok) {
          setStatus(JSON.stringify(payload, null, 2));
          return;
        }
        setStatus(JSON.stringify(payload, null, 2));
      } catch (error) {
        setStatus("Status load failed.\\n" + String(error));
      }
    }

    uploadBtn.addEventListener("click", async () => {
      if (!fileInput.files || fileInput.files.length === 0) {
        setStatus("No files selected.");
        return;
      }

      uploadBtn.disabled = true;
      syncBtn.disabled = true;
      openBtn.disabled = true;
      setStatus("Uploading...");

      const form = new FormData();
      for (const file of fileInput.files) {
        form.append("files", file, file.name);
      }
      form.append("folder", folderInput.value || "");

      try {
        const response = await fetch("/api/upload", { method: "POST", body: form });
        const payload = await parseResponsePayload(response);
        setStatus(JSON.stringify(payload, null, 2));
      } catch (error) {
        setStatus("Upload failed.\\n" + String(error));
      } finally {
        uploadBtn.disabled = false;
        syncBtn.disabled = false;
        openBtn.disabled = false;
      }
    });

    syncBtn.addEventListener("click", async () => {
      uploadBtn.disabled = true;
      syncBtn.disabled = true;
      openBtn.disabled = true;
      setStatus("Syncing...");

      try {
        const response = await fetch("/api/sync", { method: "POST" });
        const payload = await parseResponsePayload(response);
        setStatus(JSON.stringify(payload, null, 2));
      } catch (error) {
        setStatus("Sync failed.\\n" + String(error));
      } finally {
        uploadBtn.disabled = false;
        syncBtn.disabled = false;
        openBtn.disabled = false;
      }
    });

    openBtn.addEventListener("click", async () => {
      try {
        const response = await fetch("/api/open-texts", { method: "POST" });
        const payload = await parseResponsePayload(response);
        setStatus(JSON.stringify(payload, null, 2));
      } catch (error) {
        setStatus("Open folder failed.\\n" + String(error));
      }
    });

    async function sendChat() {
      const message = chatInput.value.trim();
      if (!message) {
        return;
      }

      chatInput.value = "";
      chatBtn.disabled = true;
      appendChat("user", message);
      pushHistory("user", message);

      try {
        const response = await fetch("/api/chat", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            message,
            history: chatHistory,
          }),
        });
        const payload = await parseResponsePayload(response);
        if (!payload.ok) {
          appendChat("assistant", "Chat failed.\\n" + JSON.stringify(payload, null, 2));
          return;
        }

        const mode = payload.mode ? " (" + payload.mode + ")" : "";
        const themes = Array.isArray(payload.themes) && payload.themes.length > 0
          ? "\\nThemes: " + payload.themes.join(", ")
          : "";
        const thinkers = Array.isArray(payload.thinkers) && payload.thinkers.length > 0
          ? "\\nThinkers: " + payload.thinkers.join(", ")
          : "";
        const warning = payload.warning ? "\\nWarning: " + payload.warning : "";
        const answer = String(payload.answer || "No answer.");
        appendChat("assistant", "Answer" + mode + ":\\n" + answer + themes + thinkers + warning);
        pushHistory("assistant", answer);
      } catch (error) {
        appendChat("assistant", "Chat request crashed.\\n" + String(error));
      } finally {
        chatBtn.disabled = false;
        chatInput.focus();
      }
    }

    chatBtn.addEventListener("click", sendChat);
    chatInput.addEventListener("keydown", event => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        sendChat();
      }
    });

    appendChat("assistant", "Upload or sync your corpus, then ask your first nihiltheistic question.");
    refreshStatus();
  </script>
</body>
</html>`;
}

const server = Bun.serve({
  hostname: HOST,
  port: PORT,
  fetch: async request => {
    try {
      const url = new URL(request.url);

      if (request.method === "GET" && url.pathname === "/") {
        return new Response(htmlPage(), {
          headers: {
            "content-type": "text/html; charset=utf-8",
            "cache-control": "no-store",
          },
        });
      }

      if (request.method === "GET" && url.pathname === "/api/status") {
        return jsonResponse(appStatusPayload());
      }

      if (request.method === "GET" && url.pathname === "/api/files") {
        return jsonResponse({ ok: true, files: listTextFiles(TEXTS_DIR) });
      }

      if (request.method === "POST" && url.pathname === "/api/upload") {
        return await handleUpload(request);
      }

      if (request.method === "POST" && url.pathname === "/api/sync") {
        return handleSync();
      }

      if (request.method === "POST" && url.pathname === "/api/chat") {
        return await handleChat(request);
      }

      if (request.method === "POST" && url.pathname === "/api/open-texts") {
        return handleOpenTextsFolder();
      }

      return jsonResponse({ ok: false, error: "Route not found." }, 404);
    } catch (error) {
      return jsonResponse(
        {
          ok: false,
          error: "Internal server error.",
          details: error instanceof Error ? error.message : String(error),
        },
        500
      );
    }
  },
});

console.log(`Nihiltheism Genius running at http://${HOST}:${server.port}`);

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    closeDb();
    process.exit(0);
  });
}
