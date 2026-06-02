const fs = require('fs');
const path = require('path');
const dbManager = require('../database/Database');

/**
 * Custom store for Baileys backed by SQLite
 * Replaces the old in-memory Map + JSON file approach
 */
class BaileysStore {
  constructor(sessionId = null) {
    this.sessionId = sessionId;
    this.db = dbManager;

    // In-memory cache for chat overview (rebuilt on demand)
    this._overviewCache = null;
    this._overviewCacheTime = 0;
    this._overviewCacheTTL = 5000; // 5 seconds
  }

  // ==================== SAFE SERIALIZATION ====================

  /**
   * Safely serialize data to JSON (handles circular refs, buffers, etc.)
   */
  _safeStringify(data) {
    const seen = new WeakSet();
    return JSON.stringify(data, (key, value) => {
      if (value instanceof Uint8Array || value instanceof ArrayBuffer) return undefined;
      if (typeof Buffer !== 'undefined' && Buffer.isBuffer && Buffer.isBuffer(value)) return undefined;
      if (typeof value === 'function') return undefined;
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return undefined;
        seen.add(value);
      }
      return value;
    });
  }

  // ==================== BIND TO BAILEYS EVENTS ====================

  /**
   * Bind store to Baileys socket events
   */
  bind(ev) {
    // ----- CHATS -----
    ev.on('chats.set', ({ chats }) => {
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'INSERT OR REPLACE INTO chats (session_id, chat_id, data) VALUES (?, ?, ?)'
        );
        for (const chat of chats) {
          stmt.run(this.sessionId, chat.id, this._safeStringify(chat));
        }
      });
      this._invalidateOverviewCache();
    });

    ev.on('chats.upsert', (chats) => {
      this.db.transaction(() => {
        for (const chat of chats) {
          const existing = this._getChatRaw(chat.id);
          const merged = existing ? { ...existing, ...chat } : chat;
          this.db.prepare(
            'INSERT OR REPLACE INTO chats (session_id, chat_id, data) VALUES (?, ?, ?)'
          ).run(this.sessionId, chat.id, this._safeStringify(merged));
        }
      });
      this._invalidateOverviewCache();
    });

    ev.on('chats.update', (updates) => {
      this.db.transaction(() => {
        for (const update of updates) {
          const existing = this._getChatRaw(update.id);
          if (existing) {
            const merged = { ...existing, ...update };
            this.db.prepare(
              'INSERT OR REPLACE INTO chats (session_id, chat_id, data) VALUES (?, ?, ?)'
            ).run(this.sessionId, update.id, this._safeStringify(merged));
          }
        }
      });
      this._invalidateOverviewCache();
    });

    ev.on('chats.delete', (ids) => {
      this.db.transaction(() => {
        const stmtChat = this.db.prepare('DELETE FROM chats WHERE session_id = ? AND chat_id = ?');
        const stmtMsg = this.db.prepare('DELETE FROM messages WHERE session_id = ? AND chat_id = ?');
        for (const id of ids) {
          stmtChat.run(this.sessionId, id);
          stmtMsg.run(this.sessionId, id);
        }
      });
      this._invalidateOverviewCache();
    });

    // ----- CONTACTS -----
    ev.on('contacts.set', ({ contacts }) => {
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'INSERT OR REPLACE INTO contacts (session_id, contact_id, name, notify, verified_name, data) VALUES (?, ?, ?, ?, ?, ?)'
        );
        for (const contact of contacts) {
          stmt.run(
            this.sessionId, contact.id,
            contact.name || null, contact.notify || null, contact.verifiedName || null,
            this._safeStringify(contact)
          );
        }
      });
    });

    ev.on('contacts.upsert', (contacts) => {
      this.db.transaction(() => {
        for (const contact of contacts) {
          const existing = this._getContactRaw(contact.id);
          const merged = existing ? { ...existing, ...contact } : contact;
          this.db.prepare(
            'INSERT OR REPLACE INTO contacts (session_id, contact_id, name, notify, verified_name, data) VALUES (?, ?, ?, ?, ?, ?)'
          ).run(
            this.sessionId, merged.id,
            merged.name || null, merged.notify || null, merged.verifiedName || null,
            this._safeStringify(merged)
          );
        }
      });
    });

    ev.on('contacts.update', (updates) => {
      this.db.transaction(() => {
        for (const update of updates) {
          const existing = this._getContactRaw(update.id);
          if (existing) {
            const merged = { ...existing, ...update };
            this.db.prepare(
              'INSERT OR REPLACE INTO contacts (session_id, contact_id, name, notify, verified_name, data) VALUES (?, ?, ?, ?, ?, ?)'
            ).run(
              this.sessionId, merged.id,
              merged.name || null, merged.notify || null, merged.verifiedName || null,
              this._safeStringify(merged)
            );
          }
        }
      });
    });

    // ----- MESSAGES -----
    ev.on('messages.set', ({ messages }) => {
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'INSERT OR REPLACE INTO messages (session_id, chat_id, message_id, from_me, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)'
        );
        for (const msg of messages) {
          if (!msg || !msg.key || !msg.key.remoteJid || !msg.key.id) continue;
          const ts = this._extractTimestamp(msg);
          stmt.run(
            this.sessionId, msg.key.remoteJid, msg.key.id,
            msg.key.fromMe ? 1 : 0, ts,
            this._safeStringify(msg)
          );
        }
      });
      this._invalidateOverviewCache();
    });

    ev.on('messages.upsert', ({ messages, type }) => {
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'INSERT OR REPLACE INTO messages (session_id, chat_id, message_id, from_me, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)'
        );
        for (const msg of messages) {
          if (!msg || !msg.key || !msg.key.remoteJid || !msg.key.id) continue;
          const ts = this._extractTimestamp(msg);
          stmt.run(
            this.sessionId, msg.key.remoteJid, msg.key.id,
            msg.key.fromMe ? 1 : 0, ts,
            this._safeStringify(msg)
          );
        }
      });
      this._invalidateOverviewCache();
    });

    ev.on('messages.update', (updates) => {
      this.db.transaction(() => {
        for (const { key, update } of updates) {
          if (!key || !key.remoteJid || !key.id) continue;
          const existing = this.getMessage(key.remoteJid, key.id);
          if (existing) {
            const merged = { ...existing, ...update };
            this.db.prepare(
              'INSERT OR REPLACE INTO messages (session_id, chat_id, message_id, from_me, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)'
            ).run(
              this.sessionId, key.remoteJid, key.id,
              key.fromMe ? 1 : 0, this._extractTimestamp(merged),
              this._safeStringify(merged)
            );
          }
        }
      });
    });

    ev.on('messages.delete', (item) => {
      if ('keys' in item) {
        this.db.transaction(() => {
          const stmt = this.db.prepare(
            'DELETE FROM messages WHERE session_id = ? AND chat_id = ? AND message_id = ?'
          );
          for (const key of item.keys) {
            if (!key || !key.remoteJid) continue;
            stmt.run(this.sessionId, key.remoteJid, key.id);
            this._deleteMediaFile(key.id);
          }
        });
        this._invalidateOverviewCache();
      }
    });

    // ----- GROUPS -----
    ev.on('groups.upsert', (groups) => {
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'INSERT OR REPLACE INTO group_metadata (session_id, group_id, subject, data) VALUES (?, ?, ?, ?)'
        );
        for (const group of groups) {
          stmt.run(this.sessionId, group.id, group.subject || null, this._safeStringify(group));
        }
      });
    });

    ev.on('groups.update', (updates) => {
      this.db.transaction(() => {
        for (const update of updates) {
          const existing = this.getGroupMetadata(update.id);
          if (existing) {
            const merged = { ...existing, ...update };
            this.db.prepare(
              'INSERT OR REPLACE INTO group_metadata (session_id, group_id, subject, data) VALUES (?, ?, ?, ?)'
            ).run(this.sessionId, update.id, merged.subject || null, this._safeStringify(merged));
          }
        }
      });
    });
  }

  // ==================== HELPERS ====================

  _extractTimestamp(msg) {
    if (!msg || msg.messageTimestamp == null) return 0;
    const ts = msg.messageTimestamp;
    if (typeof ts === 'object' && ts !== null) return Number(ts) || 0;
    return Number(ts) || 0;
  }

  _invalidateOverviewCache() {
    this._overviewCache = null;
    this._overviewCacheTime = 0;
  }

  // ==================== RAW DATA ACCESS ====================

  _getChatRaw(chatId) {
    const row = this.db.prepare(
      'SELECT data FROM chats WHERE session_id = ? AND chat_id = ?'
    ).get(this.sessionId, chatId);
    return row ? JSON.parse(row.data) : null;
  }

  _getContactRaw(contactId) {
    const row = this.db.prepare(
      'SELECT data FROM contacts WHERE session_id = ? AND contact_id = ?'
    ).get(this.sessionId, contactId);
    return row ? JSON.parse(row.data) : null;
  }

  // ==================== PUBLIC API ====================

  /**
   * Get all chats
   */
  getAllChats() {
    const rows = this.db.prepare(
      'SELECT data FROM chats WHERE session_id = ?'
    ).all(this.sessionId);
    return rows.map(r => JSON.parse(r.data));
  }

  /**
   * Get a specific chat
   */
  getChat(chatId) {
    return this._getChatRaw(chatId);
  }

  /**
   * Get a specific contact
   */
  getContact(jid) {
    return this._getContactRaw(jid);
  }

  /**
   * Get group metadata
   */
  getGroupMetadata(groupId) {
    const row = this.db.prepare(
      'SELECT data FROM group_metadata WHERE session_id = ? AND group_id = ?'
    ).get(this.sessionId, groupId);
    return row ? JSON.parse(row.data) : null;
  }

  /**
   * Get a specific message
   */
  getMessage(chatId, messageId) {
    const row = this.db.prepare(
      'SELECT data FROM messages WHERE session_id = ? AND chat_id = ? AND message_id = ?'
    ).get(this.sessionId, chatId, messageId);
    return row ? JSON.parse(row.data) : null;
  }

  /**
   * Update a single message in the database
   */
  updateMessage(chatId, messageId, msgObj) {
    const ts = this._extractTimestamp(msgObj);
    this.db.prepare(
      'INSERT OR REPLACE INTO messages (session_id, chat_id, message_id, from_me, timestamp, data) VALUES (?, ?, ?, ?, ?, ?)'
    ).run(
      this.sessionId, chatId, messageId,
      msgObj.key?.fromMe ? 1 : 0, ts,
      this._safeStringify(msgObj)
    );
  }

  /**
   * Get messages for a specific chat
   */
  getMessages(chatId, options = {}) {
    const { limit = 50, before = null } = options;

    let rows;
    if (before) {
      // Get the timestamp of the cursor message
      const cursorRow = this.db.prepare(
        'SELECT timestamp FROM messages WHERE session_id = ? AND chat_id = ? AND message_id = ?'
      ).get(this.sessionId, chatId, before);

      if (cursorRow) {
        rows = this.db.prepare(
          'SELECT data FROM messages WHERE session_id = ? AND chat_id = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT ?'
        ).all(this.sessionId, chatId, cursorRow.timestamp, limit);
      } else {
        rows = this.db.prepare(
          'SELECT data FROM messages WHERE session_id = ? AND chat_id = ? ORDER BY timestamp DESC LIMIT ?'
        ).all(this.sessionId, chatId, limit);
      }
    } else {
      rows = this.db.prepare(
        'SELECT data FROM messages WHERE session_id = ? AND chat_id = ? ORDER BY timestamp DESC LIMIT ?'
      ).all(this.sessionId, chatId, limit);
    }

    return rows.map(r => JSON.parse(r.data));
  }

  // ==================== CHAT OVERVIEW (OPTIMIZED) ====================

  /**
   * Get chats overview with latest message (uses SQL for computation)
   */
  getChatsOverviewFast(options = {}) {
    const { limit = 50, offset = 0 } = options;

    // Use cache if fresh enough
    const now = Date.now();
    if (this._overviewCache && (now - this._overviewCacheTime) < this._overviewCacheTTL) {
      const cached = this._overviewCache;
      return {
        total: cached.length,
        offset,
        limit,
        data: cached.slice(offset, offset + limit)
      };
    }

    // SQL: get the latest message per chat
    const rows = this.db.prepare(`
      SELECT 
        m.chat_id,
        m.message_id,
        m.timestamp,
        m.from_me,
        m.data as message_data
      FROM messages m
      INNER JOIN (
        SELECT chat_id, MAX(timestamp) as max_ts
        FROM messages
        WHERE session_id = ?
        GROUP BY chat_id
      ) latest ON m.chat_id = latest.chat_id AND m.timestamp = latest.max_ts AND m.session_id = ?
      ORDER BY m.timestamp DESC
    `).all(this.sessionId, this.sessionId);

    const overview = rows.map(row => {
      const latestMessage = JSON.parse(row.message_data);
      const chatId = row.chat_id;
      const isGroup = chatId.endsWith('@g.us');

      // Get name from contact, group, or chat
      const contact = this._getContactRaw(chatId);
      const chat = this._getChatRaw(chatId);
      const groupMeta = isGroup ? this.getGroupMetadata(chatId) : null;

      const profilePicRow = this.db.prepare(
        'SELECT url FROM profile_pictures WHERE session_id = ? AND jid = ?'
      ).get(this.sessionId, chatId);

      return {
        id: chatId,
        name: groupMeta?.subject || contact?.name || contact?.notify || chat?.name || chatId.replace('@c.us', '').replace('@g.us', ''),
        isGroup,
        unreadCount: chat?.unreadCount || 0,
        lastMessage: {
          id: row.message_id,
          timestamp: row.timestamp,
          preview: this._extractMessagePreview(latestMessage),
          fromMe: row.from_me === 1
        },
        profilePicture: profilePicRow?.url || null,
        conversationTimestamp: chat?.conversationTimestamp || row.timestamp
      };
    });

    // Update cache
    this._overviewCache = overview;
    this._overviewCacheTime = now;

    return {
      total: overview.length,
      offset,
      limit,
      data: overview.slice(offset, offset + limit)
    };
  }

  /**
   * Extract message preview text
   */
  _extractMessagePreview(message) {
    if (!message?.message) return '';

    const msg = message.message;

    if (msg.conversation) return msg.conversation.substring(0, 100);
    if (msg.extendedTextMessage?.text) return msg.extendedTextMessage.text.substring(0, 100);
    if (msg.imageMessage) return '📷 Image';
    if (msg.videoMessage) return '🎥 Video';
    if (msg.audioMessage) return '🎵 Audio';
    if (msg.documentMessage) return `📄 ${msg.documentMessage.fileName || 'Document'}`;
    if (msg.stickerMessage) return '🎭 Sticker';
    if (msg.contactMessage) return `👤 Contact: ${msg.contactMessage.displayName}`;
    if (msg.locationMessage) return '📍 Location';
    if (msg.buttonsMessage) return msg.buttonsMessage.contentText || 'Buttons';
    if (msg.templateMessage) return 'Template Message';
    if (msg.listMessage) return msg.listMessage.title || 'List';
    if (msg.pollCreationMessage || msg.pollCreationMessageV2 || msg.pollCreationMessageV3) {
      const pollMsg = msg.pollCreationMessage || msg.pollCreationMessageV2 || msg.pollCreationMessageV3;
      return `📊 ${pollMsg.name || 'Poll'}`;
    }
    if (msg.pollUpdateMessage) return '📊 Poll vote';

    return 'Message';
  }

  // ==================== CONTACTS ====================

  /**
   * Get contacts (optimized with SQL)
   */
  getContactsFast(options = {}) {
    const { limit = 100, offset = 0, search = '' } = options;

    let rows;
    let total;

    if (search) {
      const searchPattern = `%${search}%`;
      total = this.db.prepare(
        `SELECT COUNT(*) as cnt FROM contacts 
         WHERE session_id = ? AND contact_id LIKE '%@c.us' 
         AND (name LIKE ? OR notify LIKE ? OR contact_id LIKE ?)`
      ).get(this.sessionId, searchPattern, searchPattern, searchPattern)?.cnt || 0;

      rows = this.db.prepare(
        `SELECT contact_id, name, notify, verified_name FROM contacts 
         WHERE session_id = ? AND contact_id LIKE '%@c.us' 
         AND (name LIKE ? OR notify LIKE ? OR contact_id LIKE ?)
         ORDER BY name COLLATE NOCASE ASC
         LIMIT ? OFFSET ?`
      ).all(this.sessionId, searchPattern, searchPattern, searchPattern, limit, offset);
    } else {
      total = this.db.prepare(
        `SELECT COUNT(*) as cnt FROM contacts 
         WHERE session_id = ? AND contact_id LIKE '%@c.us'`
      ).get(this.sessionId)?.cnt || 0;

      rows = this.db.prepare(
        `SELECT contact_id, name, notify, verified_name FROM contacts 
         WHERE session_id = ? AND contact_id LIKE '%@c.us'
         ORDER BY name COLLATE NOCASE ASC
         LIMIT ? OFFSET ?`
      ).all(this.sessionId, limit, offset);
    }

    const contacts = rows.map(c => {
      const profilePicRow = this.db.prepare(
        'SELECT url FROM profile_pictures WHERE session_id = ? AND jid = ?'
      ).get(this.sessionId, c.contact_id);

      return {
        id: c.contact_id,
        name: c.name || c.notify || c.contact_id.replace('@c.us', ''),
        notify: c.notify,
        verifiedName: c.verified_name,
        profilePicture: profilePicRow?.url || null
      };
    });

    return {
      total,
      offset,
      limit,
      data: contacts
    };
  }

  // ==================== PROFILE PICTURES ====================

  /**
   * Set profile picture
   */
  setProfilePicture(jid, url) {
    this.db.prepare(
      'INSERT OR REPLACE INTO profile_pictures (session_id, jid, url) VALUES (?, ?, ?)'
    ).run(this.sessionId, jid, url);
  }

  /**
   * Get cached profile picture
   */
  getProfilePicture(jid) {
    const row = this.db.prepare(
      'SELECT url FROM profile_pictures WHERE session_id = ? AND jid = ?'
    ).get(this.sessionId, jid);
    return row?.url || null;
  }

  // ==================== MEDIA FILES ====================

  /**
   * Register a media file for a message
   */
  registerMediaFile(messageId, filePath) {
    this.db.prepare(
      'INSERT OR REPLACE INTO media_files (session_id, message_id, file_path) VALUES (?, ?, ?)'
    ).run(this.sessionId, messageId, filePath);
  }

  /**
   * Delete media file for a message
   */
  _deleteMediaFile(messageId) {
    const row = this.db.prepare(
      'SELECT file_path FROM media_files WHERE session_id = ? AND message_id = ?'
    ).get(this.sessionId, messageId);

    if (row) {
      try {
        if (fs.existsSync(row.file_path)) {
          fs.unlinkSync(row.file_path);
          console.log(`🗑️ [${this.sessionId}] Media deleted: ${row.file_path}`);
        }
      } catch (e) {
        // Silent fail
      }
      this.db.prepare(
        'DELETE FROM media_files WHERE session_id = ? AND message_id = ?'
      ).run(this.sessionId, messageId);
    }
  }

  /**
   * Cleanup old media files (keep only last N messages per chat)
   */
  cleanupOldMedia(maxMessagesPerChat = 100) {
    // Get all media files that belong to messages outside the "keep" window
    const rows = this.db.prepare(`
      SELECT mf.message_id, mf.file_path
      FROM media_files mf
      WHERE mf.session_id = ?
      AND mf.message_id NOT IN (
        SELECT m.message_id FROM messages m
        WHERE m.session_id = ?
        AND m.message_id IN (SELECT message_id FROM media_files WHERE session_id = ?)
        ORDER BY m.timestamp DESC
        LIMIT ?
      )
    `).all(this.sessionId, this.sessionId, this.sessionId, maxMessagesPerChat * 100);

    for (const row of rows) {
      try {
        if (fs.existsSync(row.file_path)) {
          fs.unlinkSync(row.file_path);
          console.log(`🗑️ [${this.sessionId}] Old media cleaned: ${row.file_path}`);
        }
      } catch (e) {
        // Silent fail
      }
    }

    if (rows.length > 0) {
      const messageIds = rows.map(r => r.message_id);
      // Delete in batches
      this.db.transaction(() => {
        const stmt = this.db.prepare(
          'DELETE FROM media_files WHERE session_id = ? AND message_id = ?'
        );
        for (const id of messageIds) {
          stmt.run(this.sessionId, id);
        }
      });
    }
  }

  /**
   * Cleanup all media files for this session
   */
  _cleanupAllMedia() {
    const rows = this.db.prepare(
      'SELECT file_path FROM media_files WHERE session_id = ?'
    ).all(this.sessionId);

    for (const row of rows) {
      try {
        if (fs.existsSync(row.file_path)) {
          fs.unlinkSync(row.file_path);
        }
      } catch (e) {
        // Silent fail
      }
    }

    this.db.prepare('DELETE FROM media_files WHERE session_id = ?').run(this.sessionId);
  }

  // ==================== CLEAR & STATS ====================

  /**
   * Clear all data for this session
   */
  clear() {
    this._cleanupAllMedia();

    this.db.transaction(() => {
      this.db.prepare('DELETE FROM chats WHERE session_id = ?').run(this.sessionId);
      this.db.prepare('DELETE FROM contacts WHERE session_id = ?').run(this.sessionId);
      this.db.prepare('DELETE FROM messages WHERE session_id = ?').run(this.sessionId);
      this.db.prepare('DELETE FROM group_metadata WHERE session_id = ?').run(this.sessionId);
      this.db.prepare('DELETE FROM profile_pictures WHERE session_id = ?').run(this.sessionId);
      this.db.prepare('DELETE FROM media_files WHERE session_id = ?').run(this.sessionId);
    });

    this._invalidateOverviewCache();
  }

  /**
   * Get store statistics
   */
  getStats() {
    const chats = this.db.prepare('SELECT COUNT(*) as cnt FROM chats WHERE session_id = ?').get(this.sessionId)?.cnt || 0;
    const contacts = this.db.prepare('SELECT COUNT(*) as cnt FROM contacts WHERE session_id = ?').get(this.sessionId)?.cnt || 0;
    const messages = this.db.prepare('SELECT COUNT(*) as cnt FROM messages WHERE session_id = ?').get(this.sessionId)?.cnt || 0;
    const groups = this.db.prepare('SELECT COUNT(*) as cnt FROM group_metadata WHERE session_id = ?').get(this.sessionId)?.cnt || 0;
    const mediaFiles = this.db.prepare('SELECT COUNT(*) as cnt FROM media_files WHERE session_id = ?').get(this.sessionId)?.cnt || 0;

    return { chats, contacts, messages, groups, mediaFiles };
  }

  // ==================== LEGACY COMPATIBILITY ====================
  // writeToFile and readFromFile are no-ops now (data is already in SQLite)

  writeToFile(filePath) {
    // No-op: data is persisted in SQLite automatically
    return true;
  }

  readFromFile(filePath) {
    // No-op: data is already in SQLite
    return true;
  }
}

module.exports = BaileysStore;
