const db = require('sqlite3');

const Logger = require('../logger/logger');

const DataManager = {
  /**
   * Constructor for OLOO style behavior delgation.
   * @param {string} dbFile - data base file name
   */
  init: function (dbFile) {
    if (!dbFile || !dataDir || !dbExt) {
      Logger.error('Database or exchange is undefined');
    } else {
      this.dbFile = dbFile;
      this.dataDir = dataDir;
      this.dbExt = dbExt;
    }

    Logger.info('Data manager initialized.');
  },

    /**
   * Returns a live sqlite connection
   */
  getDb: function() {
    const dbConn = new db.Database(
      `${this.dataDir}${this.dbFile}${this.dbExt}`
    );
    dbConn.run("PRAGMA journal_mode = WAL");
    return dbConn;
  },


  /**
   * Returns largest tradeId
   * @param {string} table
   * @returns {integer}
   */
  getNewestTrade: function(table) {
    return new Promise(resolve => {
      dbConn = this.getDb();
      dbConn.each(
        `SELECT MAX(tradeId) as lastId FROM [${table}] LIMIT 1`,
        [],
        (err, row) => resolve(row && row.lastId ? row.lastId : 0)
      );
      dbConn.close();
    });
  },

   /**
   * Stores a batch of trades in the sqlite db
   * @param {array} batch - batch of trades
   */
  storeTrades: async function (batch) {
    try {
      return new Promise(resolve => {
        if (!this.dbFile) {
          throw('Database file unspecified');
        }

        if (!batch || !batch.length) {
          throw('Data must be specified and an array');
        }

        const dbConn = this.getDb();
        const table = `[${batch[0].symbol}]`;
        const query = `CREATE TABLE IF NOT EXISTS ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, tradeId INTEGER, timestamp INTEGER, price REAL, quantity REAL)`;
        dbConn.serialize(() => {
          dbConn.run(query);
          dbConn.run('BEGIN TRANSACTION');
          const insertStmt = dbConn.prepare(`INSERT INTO ${table} (tradeId, timestamp, price, quantity) VALUES (?, ?, ?, ?)`);
          batch.forEach(trade => insertStmt.run(trade.id, trade.timestamp, trade.info.p, trade.info.q));
          insertStmt.finalize();
          dbConn.run('COMMIT');
        });

        dbConn.close(resolve);
        Logger.debug(`${batch.length} rows inserted into table`);
      });
    } catch (e) {
      Logger.error(e.message);
    }
  }
};

module.exports = DataManager;