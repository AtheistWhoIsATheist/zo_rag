import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { searchNotes, searchQuotes } from "./knowledge-db.ts";

const REPOMIX_PATH = resolve("./texts/Repomix-All_Recall_Notes.txt");
const UNIFIED_VOICE_PATH = resolve("C:/Users/adamm/OneDrive/OneDriveDocs/Journal314/_314quotes_Unified_Voice/_314quotes_unified_voice.jsonl");
const ANPES_PATH = resolve("C:/Nihilismi Experientia Sacra/Smart_Connections/smart_connect_anpes_engine.md");
const MAX_HISTORY = 10;
const MAX_SOURCES = 14;
const MAX_CONTEXT_CHARS = 22000;
const MAX_MESSAGE_LENGTH = 4000;
const SOURCE_TYPE_LIMITS: Record<ChatSource["type"], number> = {
  repomix: 5,
  "unified-voice": 4,
  note: 3,
  quote: 3,
  anpes: 2,
};

const STOP_WORDS = new Set([
  "the", "and", "for", "that", "this", "with", "from", "into", "about", "then", "than",
  "have", "has", "had", "are", "was", "were", "been", "being", "you", "your", "our",
  "not", "but", "can", "could", "should", "would", "what", "when", "where", "why",
  "how", "who", "which", "their", "there", "them", "also", "just", "like", "they",
  "does", "did", "done", "its", "it's", "only", "through", "across", "within", "between",
  "these", "those", "any", "all", "more", "most", "less", "over", "under", "after",
  "before", "because", "while", "such", "each", "very", "much", "many", "some",
]);

const NAME_CONNECTORS = new Set([
  "of", "de", "da", "del", "van", "von", "bin", "al", "the", "and", "y", "la", "le", "du", "des",
]);

const NON_THINKER_TERMS = new Set([
  "human condition",
  "existentialism",
  "nihilism",
  "divinity",
  "god",
  "buddhism",
  "christianity",
  "mysticism",
  "experience",
  "being and time",
  "nothing",
  "theism",
  "language",
  "absurdism",
  "apophatic theology",
  "christian mysticism",
  "divine presence",
  "iteration",
  "paradox",
  "existence",
  "ontology",
  "epistemology",
]);

const THEME_KEYWORDS: Record<string, string[]> = {
  "existential-abyss": [
    "anxiety", "angst", "void", "meaningless", "absurd", "despair", "abyss", "nothingness",
    "nihilism", "death", "suffering", "dread",
  ],
  "epistemic-limits": [
    "knowledge", "skepticism", "language", "truth", "certainty", "reason", "logic",
    "paradox", "unknowing", "epistemology",
  ],
  "ego-and-duality": [
    "ego", "self", "duality", "shadow", "identity", "authenticity", "transformation",
    "self-overcoming", "selflessness",
  ],
  "mystical-transcendence": [
    "mysticism", "transcendence", "silence", "contemplation", "prayer", "meditation",
    "dark night", "apophatic", "via negativa", "emptiness",
  ],
  "nihiltheistic-synthesis": [
    "nihiltheism", "sacred", "divine", "god", "theism", "transcultural", "interdisciplinary",
    "universality", "integration", "synthesis",
  ],
};

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatSource {
  type: "note" | "quote" | "repomix" | "unified-voice" | "anpes";
  title: string;
  excerpt: string;
  score: number;
}

export interface ChatResponse {
  ok: boolean;
  mode: "zo-ai" | "local-fallback";
  answer: string;
  themes: string[];
  thinkers: string[];
  sources: ChatSource[];
  warning?: string;
}

interface CorpusChunk {
  id: string;
  title: string;
  text: string;
  textLower: string;
}

interface CorpusIndex {
  loaded: boolean;
  path: string;
  text: string;
  chunks: CorpusChunk[];
  thinkers: string[];
  error?: string;
}

interface UnifiedVoiceRow {
  theme: string;
  thinker: string;
  quote: string;
  tradition: string;
  source: string;
  texture: string;
  voiceMarker: string;
  groundlessness: boolean;
  transformation: boolean;
  resonanceScore: number;
  stance: string;
  dialecticalPosition: string;
}

interface UnifiedVoiceIndex {
  loaded: boolean;
  path: string;
  rows: UnifiedVoiceRow[];
  thinkers: string[];
  themes: string[];
  error?: string;
}

interface AnpesIndex {
  loaded: boolean;
  path: string;
  directives: string[];
  error?: string;
}

const corpusIndex = loadCorpusIndex();
const unifiedVoiceIndex = loadUnifiedVoiceIndex();
const anpesIndex = loadAnpesIndex();

function sanitizeText(text: string, maxLen: number = 1200): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLen) {
    return normalized;
  }
  return `${normalized.slice(0, maxLen)}...`;
}

function tokenize(text: string): string[] {
  const tokens = text.toLowerCase().match(/[a-z0-9]{2,}/g) || [];
  return tokens.filter(token => !STOP_WORDS.has(token));
}

function isLikelyThinkerName(name: string): boolean {
  const clean = name.replace(/\s+/g, " ").trim();
  if (!clean) {
    return false;
  }

  const lower = clean.toLowerCase();
  if (NON_THINKER_TERMS.has(lower)) {
    return false;
  }
  if (clean.length < 3 || clean.length > 60) {
    return false;
  }
  if (/^[A-Z]{2,}$/.test(clean)) {
    return false;
  }

  const words = clean.split(" ").filter(Boolean);
  if (words.length >= 2) {
    let meaningfulWords = 0;
    for (const word of words) {
      const lowerWord = word.toLowerCase();
      if (NAME_CONNECTORS.has(lowerWord)) {
        continue;
      }
      meaningfulWords += 1;
      if (!/^[A-Z][a-zA-Z'.-]{1,}$/.test(word)) {
        return false;
      }
    }
    return meaningfulWords >= 1;
  }

  const only = words[0];
  if (!/^[A-Z][a-zA-Z'.-]{3,}$/.test(only)) {
    return false;
  }
  if (/(ism|ity|ness|tion|ment)$/i.test(only)) {
    return false;
  }

  return true;
}

function splitIntoChunks(text: string): CorpusChunk[] {
  const lines = text.split(/\r?\n/);
  const chunks: CorpusChunk[] = [];

  let currentTitle = "Repomix General";
  let buffer: string[] = [];
  let index = 0;

  const flush = () => {
    const raw = buffer.join("\n").trim();
    buffer = [];
    if (!raw) {
      return;
    }

    // Keep chunks compact for better ranking precision.
    const paragraphs = raw.split(/\n{2,}/).map(part => part.trim()).filter(Boolean);
    if (paragraphs.length === 0) {
      return;
    }

    let carry = "";
    for (const paragraph of paragraphs) {
      const next = carry ? `${carry}\n\n${paragraph}` : paragraph;
      if (next.length <= 1700) {
        carry = next;
        continue;
      }

      if (carry) {
        index += 1;
        chunks.push({
          id: `repomix-${index}`,
          title: currentTitle,
          text: carry,
          textLower: carry.toLowerCase(),
        });
      }
      carry = paragraph.slice(0, 1700);
    }

    if (carry) {
      index += 1;
      chunks.push({
        id: `repomix-${index}`,
        title: currentTitle,
        text: carry,
        textLower: carry.toLowerCase(),
      });
    }
  };

  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith("<file path=") || /^#{1,4}\s+/.test(trimmed)) {
      flush();
      currentTitle = trimmed.replace(/^#{1,4}\s+/, "");
      continue;
    }
    buffer.push(line);
  }

  flush();
  return chunks;
}

function extractThinkers(text: string): string[] {
  const matcher = /\[([^\]]+)\]\(https?:\/\/[^\)]+\)/g;
  const set = new Set<string>();

  for (const match of text.matchAll(matcher)) {
    const label = match[1]?.replace(/\s+/g, " ").trim();
    if (!label || !isLikelyThinkerName(label)) {
      continue;
    }
    set.add(label);
  }

  return [...set].sort((a, b) => a.localeCompare(b));
}

function loadCorpusIndex(): CorpusIndex {
  if (!existsSync(REPOMIX_PATH)) {
    return {
      loaded: false,
      path: REPOMIX_PATH,
      text: "",
      chunks: [],
      thinkers: [],
      error: `Missing corpus file: ${REPOMIX_PATH}`,
    };
  }

  try {
    const text = readFileSync(REPOMIX_PATH, "utf8");
    return {
      loaded: true,
      path: REPOMIX_PATH,
      text,
      chunks: splitIntoChunks(text),
      thinkers: extractThinkers(text),
    };
  } catch (error) {
    return {
      loaded: false,
      path: REPOMIX_PATH,
      text: "",
      chunks: [],
      thinkers: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function parseBoolean(value: string): boolean {
  return /^(true|1|yes)$/i.test(value.trim());
}

function parseNumber(value: string, fallback: number = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseCsvRows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      row.push(cell);
      if (row.some(value => value.trim().length > 0)) {
        rows.push(row);
      }
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    if (row.some(value => value.trim().length > 0)) {
      rows.push(row);
    }
  }

  return rows;
}

function normalizeHeader(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function loadUnifiedVoiceIndex(): UnifiedVoiceIndex {
  if (!existsSync(UNIFIED_VOICE_PATH)) {
    return {
      loaded: false,
      path: UNIFIED_VOICE_PATH,
      rows: [],
      thinkers: [],
      themes: [],
      error: `Missing unified voice dataset: ${UNIFIED_VOICE_PATH}`,
    };
  }

  try {
    const text = readFileSync(UNIFIED_VOICE_PATH, "utf8");
    const rows = parseCsvRows(text);
    if (rows.length <= 1) {
      return {
        loaded: false,
        path: UNIFIED_VOICE_PATH,
        rows: [],
        thinkers: [],
        themes: [],
        error: "Unified voice dataset has no rows.",
      };
    }

    const header = rows[0].map(normalizeHeader);
    const indexMap = new Map<string, number>();
    header.forEach((value, idx) => indexMap.set(value, idx));

    const pick = (record: string[], key: string): string => {
      const idx = indexMap.get(normalizeHeader(key));
      if (idx === undefined) {
        return "";
      }
      return (record[idx] || "").trim();
    };

    const parsed: UnifiedVoiceRow[] = [];
    for (const record of rows.slice(1)) {
      const quote = pick(record, "Quote");
      const thinker = pick(record, "Thinker");
      if (!quote || !thinker) {
        continue;
      }
      parsed.push({
        theme: pick(record, "Theme"),
        thinker,
        quote,
        tradition: pick(record, "Tradition"),
        source: pick(record, "Source"),
        texture: pick(record, "Texture"),
        voiceMarker: pick(record, "Voice_Marker"),
        groundlessness: parseBoolean(pick(record, "Groundlessness")),
        transformation: parseBoolean(pick(record, "Transformation")),
        resonanceScore: parseNumber(pick(record, "Resonance_Score"), 0),
        stance: pick(record, "Stance"),
        dialecticalPosition: pick(record, "Dialectical_Position"),
      });
    }

    const thinkers = [...new Set(parsed.map(row => row.thinker).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b));
    const themes = [...new Set(parsed.map(row => row.theme).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b));

    return {
      loaded: true,
      path: UNIFIED_VOICE_PATH,
      rows: parsed,
      thinkers,
      themes,
    };
  } catch (error) {
    return {
      loaded: false,
      path: UNIFIED_VOICE_PATH,
      rows: [],
      thinkers: [],
      themes: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function loadAnpesIndex(): AnpesIndex {
  if (!existsSync(ANPES_PATH)) {
    return {
      loaded: false,
      path: ANPES_PATH,
      directives: [],
      error: `Missing ANPES file: ${ANPES_PATH}`,
    };
  }

  try {
    const text = readFileSync(ANPES_PATH, "utf8");

    const patterns = [
      "CORE IDENTITY AND PURPOSE",
      "FOUNDATIONAL PHILOSOPHICAL ARCHITECTURE",
      "Universal Nihilistic Event",
      "Epistemological Framework",
      "METHODOLOGY AND APPROACH",
      "Precision-Ambiguity Dialectic Management",
      "Aporia Amplification Pattern",
      "Recursive Densification Pattern",
      "Interdisciplinary Contamination Pattern",
      "Heretical Expansion Pattern",
      "OUTPUT STANDARDS AND QUALITY ASSURANCE",
    ];

    const directives = patterns
      .filter(pattern => text.toLowerCase().includes(pattern.toLowerCase()))
      .map(pattern => `ANPES Directive: ${pattern}`);

    if (directives.length === 0) {
      const fallback = text
        .split(/\r?\n/)
        .map(line => line.trim())
        .filter(line => line.length > 0 && line.length < 160)
        .filter(line => /nihil|aporia|recursive|ontology|epistem/i.test(line))
        .slice(0, 12);

      return {
        loaded: true,
        path: ANPES_PATH,
        directives: fallback,
      };
    }

    return {
      loaded: true,
      path: ANPES_PATH,
      directives,
    };
  } catch (error) {
    return {
      loaded: false,
      path: ANPES_PATH,
      directives: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function scoreUnifiedVoiceRow(row: UnifiedVoiceRow, terms: string[]): number {
  if (terms.length === 0) {
    return 0;
  }

  const bag = `${row.theme} ${row.thinker} ${row.quote} ${row.tradition} ${row.voiceMarker}`.toLowerCase();
  let score = row.resonanceScore * 4;

  for (const term of terms) {
    if (bag.includes(term)) {
      score += 2;
    }
    if (row.thinker.toLowerCase().includes(term)) {
      score += 3;
    }
    if (row.theme.toLowerCase().includes(term)) {
      score += 2;
    }
  }

  if (row.groundlessness) {
    score += 1;
  }
  if (row.transformation) {
    score += 1;
  }

  return score;
}

function findUnifiedVoiceContext(query: string, limit: number = 6): ChatSource[] {
  if (!unifiedVoiceIndex.loaded || unifiedVoiceIndex.rows.length === 0) {
    return [];
  }

  const terms = tokenize(query).slice(0, 18);
  const ranked = unifiedVoiceIndex.rows
    .map(row => ({ row, score: scoreUnifiedVoiceRow(row, terms) }))
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  return ranked.map(item => ({
    type: "unified-voice",
    title: `${item.row.thinker} | ${item.row.theme || "Theme"}`,
    excerpt: sanitizeText(
      `"${item.row.quote}" | tradition=${item.row.tradition || "unknown"} | voice=${item.row.voiceMarker || "n/a"} | resonance=${item.row.resonanceScore}`,
      700
    ),
    score: item.score,
  }));
}

function findAnpesContext(query: string, limit: number = 3): ChatSource[] {
  if (!anpesIndex.loaded || anpesIndex.directives.length === 0) {
    return [];
  }

  const lower = query.toLowerCase();
  const selected = anpesIndex.directives
    .map(item => ({
      item,
      score: lower.split(/\s+/).reduce((acc, token) => {
        if (token && item.toLowerCase().includes(token)) {
          return acc + 1;
        }
        return acc;
      }, 0),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  return selected.map(item => ({
    type: "anpes",
    title: "ANPES",
    excerpt: sanitizeText(item.item, 220),
    score: item.score + 1,
  }));
}

function rebalanceSources(candidates: ChatSource[], maxSources: number = MAX_SOURCES): ChatSource[] {
  const sorted = [...candidates].sort((a, b) => b.score - a.score);
  const selected: ChatSource[] = [];
  const counts: Partial<Record<ChatSource["type"], number>> = {};
  const selectedTitles = new Set<string>();

  for (const source of sorted) {
    const typeCount = counts[source.type] || 0;
    const typeLimit = SOURCE_TYPE_LIMITS[source.type] ?? maxSources;
    if (typeCount >= typeLimit) {
      continue;
    }
    const titleKey = `${source.type}|${source.title}`.toLowerCase();
    if (selectedTitles.has(titleKey)) {
      continue;
    }
    selected.push(source);
    selectedTitles.add(titleKey);
    counts[source.type] = typeCount + 1;
    if (selected.length >= maxSources) {
      return selected;
    }
  }

  if (selected.length >= maxSources) {
    return selected;
  }

  const selectedKeys = new Set(selected.map(source => `${source.type}|${source.title}`.toLowerCase()));
  for (const source of sorted) {
    const key = `${source.type}|${source.title}`.toLowerCase();
    if (selectedKeys.has(key)) {
      continue;
    }
    selected.push(source);
    selectedKeys.add(key);
    if (selected.length >= maxSources) {
      break;
    }
  }

  return selected;
}

function scoreChunk(chunk: CorpusChunk, terms: string[]): number {
  if (terms.length === 0) {
    return 0;
  }

  let score = 0;
  for (const term of terms) {
    const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const regex = new RegExp(`\\b${escaped}\\b`, "g");
    const matches = chunk.textLower.match(regex);
    if (!matches) {
      continue;
    }
    score += matches.length;
    if (chunk.title.toLowerCase().includes(term)) {
      score += 3;
    }
  }

  if (chunk.title.toLowerCase().includes("nihil")) {
    score += 1;
  }

  return score;
}

function findRepomixContext(query: string, limit: number = 6): ChatSource[] {
  if (!corpusIndex.loaded || corpusIndex.chunks.length === 0) {
    return [];
  }

  const terms = tokenize(query).slice(0, 18);
  const ranked = corpusIndex.chunks
    .map(chunk => ({
      chunk,
      score: scoreChunk(chunk, terms),
    }))
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

  return ranked.map(item => ({
    type: "repomix",
    title: item.chunk.title,
    excerpt: sanitizeText(item.chunk.text, 750),
    score: item.score,
  }));
}

function detectThemes(message: string, context: string): string[] {
  const bag = `${message}\n${context}`.toLowerCase();
  const scored = Object.entries(THEME_KEYWORDS).map(([theme, words]) => {
    let score = 0;
    for (const word of words) {
      if (bag.includes(word)) {
        score += 1;
      }
    }
    return { theme, score };
  });

  const selected = scored
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map(item => item.theme);

  if (selected.length === 0) {
    const fallbackThemes = unifiedVoiceIndex.themes.slice(0, 2);
    return fallbackThemes.length > 0
      ? fallbackThemes
      : ["existential-abyss", "nihiltheistic-synthesis"];
  }

  const unifiedMatches = unifiedVoiceIndex.themes
    .filter(theme => bag.includes(theme.toLowerCase()))
    .slice(0, 2);

  return [...new Set([...selected, ...unifiedMatches])].slice(0, 4);
}

function extractRequestedThinkers(message: string, limit: number = 4): string[] {
  const cueWords = new Set(["compare", "vs", "versus", "between", "and", "with", "about", "on", "of"]);
  const tokens = message.match(/[A-Za-z][A-Za-z'.-]*/g) || [];
  const found: string[] = [];

  for (let i = 0; i < tokens.length - 1; i += 1) {
    const cue = tokens[i].toLowerCase();
    if (!cueWords.has(cue)) {
      continue;
    }

    const first = tokens[i + 1];
    if (!/^[A-Z][a-zA-Z'.-]{3,}$/.test(first)) {
      continue;
    }

    let candidate = first;
    const second = tokens[i + 2];
    if (second && /^[A-Z][a-zA-Z'.-]{2,}$/.test(second) && !cueWords.has(second.toLowerCase())) {
      candidate = `${first} ${second}`;
    }

    if (!candidate || !isLikelyThinkerName(candidate)) {
      continue;
    }
    if (found.includes(candidate)) {
      continue;
    }
    found.push(candidate);
    if (found.length >= limit) {
      break;
    }
  }

  return found;
}

function detectThinkersInText(text: string, limit: number = 10): string[] {
  const pool = [...new Set([...corpusIndex.thinkers, ...unifiedVoiceIndex.thinkers])]
    .filter(isLikelyThinkerName);
  if (pool.length === 0) {
    return [];
  }

  const lower = text.toLowerCase();
  const found = pool
    .map(name => {
      const needle = name.toLowerCase();
      const firstIndex = lower.indexOf(needle);
      if (firstIndex < 0) {
        return null;
      }
      let count = 0;
      let cursor = 0;
      while (true) {
        const idx = lower.indexOf(needle, cursor);
        if (idx < 0) {
          break;
        }
        count += 1;
        cursor = idx + needle.length;
      }
      return { name, count, firstIndex };
    })
    .filter((item): item is { name: string; count: number; firstIndex: number } => item !== null)
    .sort((a, b) => b.count - a.count || a.firstIndex - b.firstIndex)
    .slice(0, limit)
    .map(item => item.name);

  return found;
}

function buildContextBundle(message: string): {
  sources: ChatSource[];
  context: string;
  themes: string[];
  thinkers: string[];
} {
  const noteHits = searchNotes(message, 8).map(hit => ({
    type: "note" as const,
    title: hit.title,
    excerpt: hit.snippet,
    score: hit.score,
  }));

  const quoteHits = searchQuotes(message, 6).map(hit => ({
    type: "quote" as const,
    title: hit.source || "Quote",
    excerpt: sanitizeText(hit.content, 400),
    score: 1,
  }));

  const repomixHits = findRepomixContext(message, 6);
  const unifiedVoiceHits = findUnifiedVoiceContext(message, 6);
  const anpesHits = findAnpesContext(message, 3);

  const merged = rebalanceSources(
    [...repomixHits, ...unifiedVoiceHits, ...noteHits, ...quoteHits, ...anpesHits],
    MAX_SOURCES
  );

  const context = merged
    .map((source, index) => {
      return `[${index + 1}] ${source.type.toUpperCase()} | ${source.title}\n${source.excerpt}`;
    })
    .join("\n\n")
    .slice(0, MAX_CONTEXT_CHARS);

  const themes = detectThemes(message, context);
  const explicitThinkers = extractRequestedThinkers(message, 4);
  const thinkers = [...new Set([...explicitThinkers, ...detectThinkersInText(`${message}\n${context}`, 12)])].slice(0, 12);

  return { sources: merged, context, themes, thinkers };
}

function localFallbackAnswer(
  message: string,
  themes: string[],
  thinkers: string[],
  sources: ChatSource[]
): string {
  const top = sources.slice(0, 5);
  if (top.length === 0) {
    return [
      "No aligned passages were found yet.",
      "Upload more notes or ask with thinker/theme/voice keywords (for example: Kierkegaard, apophatic, negation, existential-abyss).",
    ].join(" ");
  }

  const sourceLines = top.map((source, idx) => {
    return `${idx + 1}. ${source.type.toUpperCase()} - ${source.title}: ${sanitizeText(source.excerpt, 240)}`;
  });

  const themeText = themes.join(", ");
  const thinkerText = thinkers.length > 0 ? thinkers.join(", ") : "none explicitly detected";

  return [
    `Thesis: Your query points into ${themeText}, where nihilism functions as both collapse and method.`,
    `Thinker Signals: ${thinkerText}.`,
    "",
    "Grounded Evidence:",
    ...sourceLines,
    "",
    "Synthesis Move:",
    "Treat the void as phenomenological data rather than a defect to erase. Compare where traditions diverge on response (revolt, surrender, apophatic silence, value-creation) while preserving the shared structure of existential exposure.",
    "",
    "Next Probe:",
    "Ask for a contrast between two named thinkers and one voice marker (for example: Kierkegaard vs. Eckhart under negation vs affirmation_through_negation).",
  ].join("\n");
}

async function askZoAI(
  message: string,
  history: ChatMessage[],
  context: string,
  themes: string[],
  thinkers: string[]
): Promise<string> {
  const token = process.env.ZO_CLIENT_IDENTITY_TOKEN || "";
  if (!token) {
    throw new Error("ZO_CLIENT_IDENTITY_TOKEN is not configured.");
  }

  const historyText = history
    .slice(-MAX_HISTORY)
    .map(item => `${item.role.toUpperCase()}: ${sanitizeText(item.content, 420)}`)
    .join("\n");

  const prompt = [
    "You are Professor Nihil, a rigorous nihiltheistic research assistant.",
    "Task: answer with philosophical precision, textual grounding, and cross-tradition synthesis.",
    "",
    "Hard Rules:",
    "1. Prioritize provided context over prior assumptions.",
    "2. Explicitly map the answer to at least two themes.",
    "3. Name thinker tensions (agreement/disagreement).",
    "4. Never flatten paradox into cheap certainty.",
    "5. End with one high-quality next research question.",
    "",
    `Detected Themes: ${themes.join(", ")}`,
    `Detected Thinkers: ${thinkers.join(", ") || "none detected"}`,
    "",
    "Conversation History:",
    historyText || "None",
    "",
    "Knowledge Context:",
    context || "No context found.",
    "",
    `User Question: ${message}`,
    "",
    "Output Format:",
    "Thesis:",
    "Comparative Analysis:",
    "Nihiltheistic Synthesis:",
    "Cautions:",
    "Next Research Question:",
  ].join("\n");

  const response = await fetch("https://api.zo.computer/zo/ask", {
    method: "POST",
    headers: {
      authorization: token,
      "content-type": "application/json",
    },
    signal: AbortSignal.timeout(45000),
    body: JSON.stringify({
      input: prompt,
      model_name: "openrouter:z-ai/glm-5",
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Zo API error ${response.status}: ${sanitizeText(text, 300)}`);
  }

  const payload = await response.json() as { output?: string };
  const output = payload.output?.trim();
  if (!output) {
    throw new Error("Zo API returned empty output.");
  }
  return output;
}

export async function chatWithNihiltheismGenius(input: {
  message?: string;
  prompt?: string;
  history?: ChatMessage[];
}): Promise<ChatResponse> {
  const rawMessage = typeof input.message === "string" && input.message.trim().length > 0
    ? input.message
    : typeof input.prompt === "string"
      ? input.prompt
      : "";
  const message = rawMessage.trim().slice(0, MAX_MESSAGE_LENGTH);
  if (!message) {
    return {
      ok: false,
      mode: "local-fallback",
      answer: "Please provide a question.",
      themes: [],
      thinkers: [],
      sources: [],
      warning: "empty-message",
    };
  }

  const history = Array.isArray(input.history)
    ? input.history
        .filter(item => item && (item.role === "user" || item.role === "assistant") && typeof item.content === "string")
        .map(item => ({
          role: item.role,
          content: item.content.slice(0, MAX_MESSAGE_LENGTH),
        }))
        .slice(-MAX_HISTORY)
    : [];

  const { sources, context, themes, thinkers } = buildContextBundle(message);

  try {
    const answer = await askZoAI(message, history, context, themes, thinkers);
    return {
      ok: true,
      mode: "zo-ai",
      answer,
      themes,
      thinkers,
      sources,
    };
  } catch (error) {
    const warning = error instanceof Error ? error.message : String(error);
    return {
      ok: true,
      mode: "local-fallback",
      answer: localFallbackAnswer(message, themes, thinkers, sources),
      themes,
      thinkers,
      sources,
      warning,
    };
  }
}

export function getNihiltheismBrainStatus() {
  return {
    corpusLoaded: corpusIndex.loaded,
    corpusPath: corpusIndex.path,
    corpusChunks: corpusIndex.chunks.length,
    thinkerCount: corpusIndex.thinkers.length,
    corpusError: corpusIndex.error || null,
    unifiedVoiceLoaded: unifiedVoiceIndex.loaded,
    unifiedVoicePath: unifiedVoiceIndex.path,
    unifiedVoiceRows: unifiedVoiceIndex.rows.length,
    unifiedVoiceThinkers: unifiedVoiceIndex.thinkers.length,
    unifiedVoiceThemes: unifiedVoiceIndex.themes.length,
    unifiedVoiceError: unifiedVoiceIndex.error || null,
    anpesLoaded: anpesIndex.loaded,
    anpesPath: anpesIndex.path,
    anpesDirectives: anpesIndex.directives.length,
    anpesError: anpesIndex.error || null,
    zoConfigured: Boolean(process.env.ZO_CLIENT_IDENTITY_TOKEN),
  };
}
