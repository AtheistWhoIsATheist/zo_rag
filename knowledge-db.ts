import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, extname, join, relative, resolve } from "node:path";

const DATA_DIR = resolve("./data");
const DB_PATH = resolve(DATA_DIR, "knowledge.db");
const TEXT_EXTENSIONS = new Set([".txt", ".md", ".markdown"]);

let db: Database | null = null;
let ftsEnabled = true;

export interface Note {
  id: number;
  title: string;
  slug: string;
  content: string | null;
  note_type: string;
  created_at: string;
  updated_at: string;
}

export interface SearchHit {
  id: number;
  title: string;
  slug: string;
  snippet: string;
  score: number;
  note_type: string;
}

export interface SourceFileRecord {
  id: number;
  path: string;
  content_hash: string;
  note_id: number;
  created_at: string;
  last_synced_at: string;
}

export interface IngestResult {
  created: number;
  updated: number;
  skipped: number;
  filesProcessed: number;
  errors: string[];
}

function ensureDirectory(path: string) {
  if (existsSync(path)) {
    const stats = statSync(path);
    if (!stats.isDirectory()) {
      throw new Error(`Expected directory but found file: ${path}`);
    }
    return;
  }
  mkdirSync(path, { recursive: true });
}

function slugify(value: string): string {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return normalized || `note-${Date.now()}`;
}

function sha256(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}

function safeRelativePath(absolutePath: string): string {
  return relative(process.cwd(), absolutePath).replace(/\\/g, "/");
}

function buildSlugFromPath(sourcePath: string): string {
  const withoutExt = sourcePath.replace(/\.[^.]+$/i, "");
  return slugify(withoutExt.replace(/[\\/]/g, "-"));
}

function initSchema(database: Database) {
  database.run(`
    CREATE TABLE IF NOT EXISTS notes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      slug TEXT UNIQUE NOT NULL,
      content TEXT,
      note_type TEXT DEFAULT 'note',
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  database.run(`
    CREATE TABLE IF NOT EXISTS quotes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      figure_id INTEGER,
      note_id INTEGER,
      content TEXT NOT NULL,
      source TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (note_id) REFERENCES notes(id)
    )
  `);

  database.run(`
    CREATE TABLE IF NOT EXISTS source_files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      path TEXT UNIQUE NOT NULL,
      content_hash TEXT NOT NULL,
      note_id INTEGER NOT NULL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      last_synced_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (note_id) REFERENCES notes(id)
    )
  `);

  database.run(`CREATE INDEX IF NOT EXISTS idx_notes_slug ON notes(slug)`);
  database.run(`CREATE INDEX IF NOT EXISTS idx_notes_updated ON notes(updated_at DESC)`);
  database.run(`CREATE INDEX IF NOT EXISTS idx_source_files_note ON source_files(note_id)`);

  initFts(database);
}

function initFts(database: Database) {
  try {
    database.run(`
      CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
        title,
        content,
        slug UNINDEXED,
        note_id UNINDEXED,
        tokenize = 'unicode61 porter'
      )
    `);

    database.run(`
      CREATE TRIGGER IF NOT EXISTS notes_ai AFTER INSERT ON notes BEGIN
        INSERT INTO notes_fts(rowid, title, content, slug, note_id)
        VALUES (new.id, new.title, COALESCE(new.content, ''), new.slug, new.id);
      END
    `);

    database.run(`
      CREATE TRIGGER IF NOT EXISTS notes_ad AFTER DELETE ON notes BEGIN
        INSERT INTO notes_fts(notes_fts, rowid, title, content, slug, note_id)
        VALUES('delete', old.id, old.title, COALESCE(old.content, ''), old.slug, old.id);
      END
    `);

    database.run(`
      CREATE TRIGGER IF NOT EXISTS notes_au AFTER UPDATE ON notes BEGIN
        INSERT INTO notes_fts(notes_fts, rowid, title, content, slug, note_id)
        VALUES('delete', old.id, old.title, COALESCE(old.content, ''), old.slug, old.id);
        INSERT INTO notes_fts(rowid, title, content, slug, note_id)
        VALUES (new.id, new.title, COALESCE(new.content, ''), new.slug, new.id);
      END
    `);

    database.run(`
      INSERT INTO notes_fts(rowid, title, content, slug, note_id)
      SELECT n.id, n.title, COALESCE(n.content, ''), n.slug, n.id
      FROM notes n
      WHERE NOT EXISTS (
        SELECT 1 FROM notes_fts f WHERE f.rowid = n.id
      )
    `);

    ftsEnabled = true;
  } catch {
    ftsEnabled = false;
  }
}

export function getDb(): Database {
  if (!db) {
    ensureDirectory(DATA_DIR);
    db = new Database(DB_PATH);
    db.run("PRAGMA journal_mode = WAL");
    db.run("PRAGMA foreign_keys = ON");
    initSchema(db);
  }
  return db;
}

function getSourceByPath(sourcePath: string): SourceFileRecord | null {
  const database = getDb();
  const stmt = database.prepare(`
    SELECT * FROM source_files WHERE path = ?
  `);
  return stmt.get(sourcePath) as SourceFileRecord | null;
}

function createNote(data: {
  title: string;
  slug: string;
  content: string;
  noteType: string;
}): Note {
  const database = getDb();
  const stmt = database.prepare(`
    INSERT INTO notes (title, slug, content, note_type)
    VALUES (?, ?, ?, ?)
    RETURNING *
  `);
  return stmt.get(data.title, data.slug, data.content, data.noteType) as Note;
}

function updateNote(id: number, data: {
  title: string;
  content: string;
  noteType: string;
}): Note {
  const database = getDb();
  const stmt = database.prepare(`
    UPDATE notes
    SET title = ?, content = ?, note_type = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    RETURNING *
  `);
  return stmt.get(data.title, data.content, data.noteType, id) as Note;
}

function upsertSourceLink(sourcePath: string, hash: string, noteId: number) {
  const database = getDb();
  const existing = getSourceByPath(sourcePath);

  if (existing) {
    const stmt = database.prepare(`
      UPDATE source_files
      SET content_hash = ?, note_id = ?, last_synced_at = CURRENT_TIMESTAMP
      WHERE path = ?
    `);
    stmt.run(hash, noteId, sourcePath);
    return;
  }

  const stmt = database.prepare(`
    INSERT INTO source_files (path, content_hash, note_id)
    VALUES (?, ?, ?)
  `);
  stmt.run(sourcePath, hash, noteId);
}

function collectTextFiles(rootPath: string): string[] {
  const files: string[] = [];
  const stack = [resolve(rootPath)];

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
      if (!TEXT_EXTENSIONS.has(extension)) {
        continue;
      }

      const stem = basename(entry, extension).toLowerCase();
      if (stem === "readme") {
        continue;
      }

      files.push(fullPath);
    }
  }

  files.sort((a, b) => a.localeCompare(b));
  return files;
}

export function ingestTextsFromFolder(
  folderPath: string = "./texts",
  noteType: string = "reference-text"
): IngestResult {
  const result: IngestResult = {
    created: 0,
    updated: 0,
    skipped: 0,
    filesProcessed: 0,
    errors: [],
  };

  let files: string[] = [];
  try {
    files = collectTextFiles(folderPath);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    result.errors.push(`Could not read folder ${folderPath}: ${message}`);
    return result;
  }

  for (const absolutePath of files) {
    result.filesProcessed += 1;

    try {
      const content = readFileSync(absolutePath, "utf8");
      const hash = sha256(content);
      const relPath = safeRelativePath(absolutePath);
      const sourcePath = resolve(absolutePath);
      const existingSource = getSourceByPath(sourcePath);

      if (existingSource && existingSource.content_hash === hash) {
        result.skipped += 1;
        continue;
      }

      const title = basename(relPath, extname(relPath)).replace(/[-_]+/g, " ").trim();
      const slug = buildSlugFromPath(relPath);
      let note: Note;

      if (existingSource) {
        note = updateNote(existingSource.note_id, {
          title,
          content,
          noteType,
        });
        result.updated += 1;
      } else {
        const database = getDb();
        const existingBySlug = database
          .prepare(`SELECT * FROM notes WHERE slug = ?`)
          .get(slug) as Note | null;

        if (existingBySlug) {
          note = updateNote(existingBySlug.id, {
            title,
            content,
            noteType,
          });
          result.updated += 1;
        } else {
          note = createNote({
            title,
            slug,
            content,
            noteType,
          });
          result.created += 1;
        }
      }

      upsertSourceLink(sourcePath, hash, note.id);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      result.errors.push(`Failed ${absolutePath}: ${message}`);
    }
  }

  return result;
}

function toSnippet(content: string, query: string, radius: number = 180): string {
  const normalized = content.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }

  const q = query.toLowerCase().trim();
  if (!q) {
    return normalized.slice(0, radius * 2);
  }

  const index = normalized.toLowerCase().indexOf(q);
  if (index < 0) {
    return normalized.slice(0, radius * 2);
  }

  const start = Math.max(0, index - radius);
  const end = Math.min(normalized.length, index + q.length + radius);
  const left = start > 0 ? "..." : "";
  const right = end < normalized.length ? "..." : "";
  return `${left}${normalized.slice(start, end)}${right}`;
}

function buildFtsQuery(query: string): string {
  const tokens = query
    .toLowerCase()
    .match(/[a-z0-9]{2,}/g) || [];

  if (tokens.length === 0) {
    return "";
  }

  return tokens.map(token => `${token}*`).join(" OR ");
}

export function searchNotes(query: string, limit: number = 12): SearchHit[] {
  const database = getDb();
  const trimmed = query.trim();
  if (!trimmed) {
    return [];
  }

  const safeLimit = Math.min(Math.max(limit, 1), 50);

  if (ftsEnabled) {
    try {
      const ftsQuery = buildFtsQuery(trimmed);
      if (ftsQuery) {
        const stmt = database.prepare(`
          SELECT
            n.id,
            n.title,
            n.slug,
            n.content,
            n.note_type,
            bm25(notes_fts) AS score
          FROM notes_fts
          JOIN notes n ON n.id = notes_fts.note_id
          WHERE notes_fts MATCH ?
          ORDER BY score
          LIMIT ?
        `);

        const rows = stmt.all(ftsQuery, safeLimit) as Array<{
          id: number;
          title: string;
          slug: string;
          content: string | null;
          note_type: string;
          score: number;
        }>;

        if (rows.length > 0) {
          return rows.map(row => ({
            id: row.id,
            title: row.title,
            slug: row.slug,
            note_type: row.note_type,
            score: row.score,
            snippet: toSnippet(row.content || "", trimmed),
          }));
        }
      }
    } catch {
      ftsEnabled = false;
    }
  }

  const like = `%${trimmed}%`;
  const fallback = database.prepare(`
    SELECT id, title, slug, content, note_type
    FROM notes
    WHERE title LIKE ? OR content LIKE ?
    ORDER BY updated_at DESC
    LIMIT ?
  `);

  const rows = fallback.all(like, like, safeLimit) as Array<{
    id: number;
    title: string;
    slug: string;
    content: string | null;
    note_type: string;
  }>;

  return rows.map((row, index) => ({
    id: row.id,
    title: row.title,
    slug: row.slug,
    note_type: row.note_type,
    score: index + 1,
    snippet: toSnippet(row.content || "", trimmed),
  }));
}

export function searchQuotes(query: string, limit: number = 12): Array<{
  id: number;
  content: string;
  source: string | null;
}> {
  const database = getDb();
  const trimmed = query.trim();
  if (!trimmed) {
    return [];
  }

  const safeLimit = Math.min(Math.max(limit, 1), 50);
  const like = `%${trimmed}%`;
  const stmt = database.prepare(`
    SELECT id, content, source
    FROM quotes
    WHERE content LIKE ?
    ORDER BY created_at DESC
    LIMIT ?
  `);
  return stmt.all(like, safeLimit) as Array<{
    id: number;
    content: string;
    source: string | null;
  }>;
}

export function getRecentNotes(limit: number = 20): Note[] {
  const database = getDb();
  const safeLimit = Math.min(Math.max(limit, 1), 200);
  const stmt = database.prepare(`
    SELECT *
    FROM notes
    ORDER BY updated_at DESC
    LIMIT ?
  `);
  return stmt.all(safeLimit) as Note[];
}

export function getCorpusStats() {
  const database = getDb();
  const notesCount = (database.prepare(`SELECT COUNT(*) as count FROM notes`).get() as { count: number }).count;
  const quotesCount = (database.prepare(`SELECT COUNT(*) as count FROM quotes`).get() as { count: number }).count;
  const sourceCount = (database.prepare(`SELECT COUNT(*) as count FROM source_files`).get() as { count: number }).count;

  return {
    dbPath: DB_PATH,
    notesCount,
    quotesCount,
    sourceCount,
    ftsEnabled,
  };
}

export function closeDb() {
  if (db) {
    db.close();
    db = null;
  }
}
