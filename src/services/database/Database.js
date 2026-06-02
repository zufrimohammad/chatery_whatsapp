const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

/**
 * SQLite Database Manager (Singleton)
 * Mengelola koneksi database SQLite untuk semua data store
 */
class DatabaseManager {
    constructor() {
        this.db = null;
        this._stmtCache = new Map();
    }

    /**
     * Initialize database connection and create tables
     */
    initialize() {
        if (this.db) return this.db;

        const storeDir = path.join(process.cwd(), 'store');
        if (!fs.existsSync(storeDir)) {
            fs.mkdirSync(storeDir, { recursive: true });
        }

        const dbPath = path.join(storeDir, 'chatery.db');
        this.db = new Database(dbPath);

        // Performance optimizations
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('synchronous = NORMAL');
        this.db.pragma('cache_size = -64000'); // 64MB cache
        this.db.pragma('temp_store = MEMORY');

        this._createTables();
        console.log('🗄️  SQLite database initialized:', dbPath);

        return this.db;
    }

    /**
     * Create all tables
     */
    _createTables() {
        this.db.exec(`
            -- Session configs (replaces config.json)
            CREATE TABLE IF NOT EXISTS session_configs (
                session_id TEXT PRIMARY KEY,
                metadata TEXT DEFAULT '{}',
                webhooks TEXT DEFAULT '[]',
                created_at INTEGER DEFAULT (strftime('%s','now')),
                updated_at INTEGER DEFAULT (strftime('%s','now'))
            );

            -- Chats
            CREATE TABLE IF NOT EXISTS chats (
                session_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (session_id, chat_id)
            );

            -- Contacts
            CREATE TABLE IF NOT EXISTS contacts (
                session_id TEXT NOT NULL,
                contact_id TEXT NOT NULL,
                name TEXT,
                notify TEXT,
                verified_name TEXT,
                data TEXT NOT NULL,
                PRIMARY KEY (session_id, contact_id)
            );

            -- Messages
            CREATE TABLE IF NOT EXISTS messages (
                session_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                from_me INTEGER DEFAULT 0,
                timestamp INTEGER DEFAULT 0,
                data TEXT NOT NULL,
                PRIMARY KEY (session_id, chat_id, message_id)
            );

            -- Group metadata
            CREATE TABLE IF NOT EXISTS group_metadata (
                session_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                subject TEXT,
                data TEXT NOT NULL,
                PRIMARY KEY (session_id, group_id)
            );

            -- Profile pictures
            CREATE TABLE IF NOT EXISTS profile_pictures (
                session_id TEXT NOT NULL,
                jid TEXT NOT NULL,
                url TEXT,
                PRIMARY KEY (session_id, jid)
            );

            -- Media files tracking
            CREATE TABLE IF NOT EXISTS media_files (
                session_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                PRIMARY KEY (session_id, message_id)
            );

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_messages_ts 
                ON messages(session_id, chat_id, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_contacts_name 
                ON contacts(session_id, name COLLATE NOCASE);
            CREATE INDEX IF NOT EXISTS idx_chats_session 
                ON chats(session_id);
        `);
    }

    /**
     * Get or create a prepared statement (cached)
     * @param {string} sql - SQL query string
     * @returns {Statement}
     */
    prepare(sql) {
        if (!this.db) this.initialize();
        
        let stmt = this._stmtCache.get(sql);
        if (!stmt) {
            stmt = this.db.prepare(sql);
            this._stmtCache.set(sql, stmt);
        }
        return stmt;
    }

    /**
     * Run multiple operations in a transaction
     * @param {Function} fn - Function to execute within transaction
     */
    transaction(fn) {
        if (!this.db) this.initialize();
        return this.db.transaction(fn)();
    }

    /**
     * Get the raw database instance
     */
    getDb() {
        if (!this.db) this.initialize();
        return this.db;
    }

    /**
     * Close database connection
     */
    close() {
        if (this.db) {
            this._stmtCache.clear();
            this.db.close();
            this.db = null;
            console.log('🗄️  SQLite database closed');
        }
    }
}

// Singleton instance
const dbManager = new DatabaseManager();

// Auto-initialize on first require
dbManager.initialize();

module.exports = dbManager;
