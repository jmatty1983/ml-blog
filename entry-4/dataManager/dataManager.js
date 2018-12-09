const db = require("better-sqlite3");

const Logger = require("../logger/logger");

const limit = 100000;

const DataManager = {
  /**
   * Constructor for OLOO style behavior delgation.
   * @param {string} dbFile - data base file name
   */
  init: function(dbFile) {
    if (!dbFile || !dataDir || !dbExt) {
      Logger.error("Database or exchange is undefined");
    } else {
      this.dbFile = dbFile;
      this.dataDir = dataDir;
      this.dbExt = dbExt;
    }

    Logger.info("Data manager initialized.");
  },

  /**
   * Builds candles of different lengths
   * @param {integer} length - length of candle
   * @param {array{}} data - array of trade objects
   *
   * @returns {object} object contains {candles - new candles, remaineder - unprocessed trades at tail end}
   */
  buildCandles: function({ length, rows }) {
    length = this.convertLengthToTime(length);
    if (!length) {
      throw "Invalid time duration";
    }

    const chunks = rows.reduce(
      (chunks, row) => {
        const current = chunks[chunks.length - 1];

        if (current.length) {
          if (row.timestamp - current[0].timestamp < length) {
            current.push(row);
          } else {
            chunks.push([row]);
          }
        } else {
          current.push(row);
        }
      },
      [[]]
    );
    const remainder = chunks.pop();
    const candles = chunks.map(trades => this.makeCandle(trades));
    return { candles, remainder };
  },

  /**
   * Converts a length string like 5s, 5m, 5h, 5d to milliseconds
   * @param {string} length
   * @returns {integer}
   */
  convertLengthToTime: function(length) {
    const unit = length[length.length - 1];
    const number = length.slice(0, -1);
    switch (unit) {
      case "s":
        return number * 1000;
      case "m":
        return number * 1000 * 60;
      case "h":
        return number * 1000 * 60 * 60;
      case "d":
        return number * 1000 * 60 * 60 * 24;
      default:
        return null;
    }
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
    if (this.checkDataExists(table)) {
      const ret = this.getDb()
        .prepare(`SELECT MAX(tradeId) as lastId FROM [${table}] LIMIT 1`)
        .get();

      return ret.lastId;
    } else {
      return 0;
    }
  },

  /**
   * Candle is a candle is a candle is a candle. All get built the same, only difference is
   * how we decide which trades to build them from
   * @param {array{}} - array of trade objects
   * @returns {object} - returns a candle object of type OHLCV tradeId startTime endTime
   */
  makeCandle: function(trades) {
    return trades.reduce(
      (candle, trade) => {
        candle.startTime = candle.startTime || trade.timestamp;
        candle.endTime = trade.timestamp;
        candle.open = candle.open || trade.price;
        candle.close = trade.price;
        candle.high = trade.price > candle.high ? trade.price : candle.high;
        candle.low = trade.price < candle.low ? trade.price : candle.low;
        candle.volume += trade.quantity;
        candle.tradeId = trade.tradeId;
        return candle;
      },
      { volume: 0, high: 0, low: Infinity }
    );
  },

  processCandles: function(table, lengths) {
    types.forEach(length =>
      this.getDb()
        .prepare(`DROP TABLE IF EXISTS [${table}_${length}]`)
        .run()
    );

    let remainders = {};
    let offset = 0;
    let rowLen = 0;
    do {
      const rows = this.getDb()
        .prepare(
          `SELECT * FROM [${table}] ORDER BY tradeId ASC LIMIT ${limit} OFFSET ${offset}`
        )
        .all();

      rowLen = rows.length;
      const candles = lengths.map(length => {
        const extra = remainders[`${table}_${length}`]
          ? remainders[`${table}_${length}`]
          : [];

        const built = this.buildCandles({
          length,
          rows: [...extra, ...rows]
        });
        return { length, built };
      });

      remainders = candles.reduce((remainderObj, { length, built }) => {
        this.storeCandles(`${table}_${length}`, built.candles);
        remainderObj[`${table}_${length}`] = built.remainder;
        return remainderObj;
      }, {});
      offset += limit;
    } while (rowLen === limit);
  },

  /**
   * Stores candle data
   * @param {string} table - table name
   * @param {array{}} candles - array of candle objects
   */
  storeCandles: function(table, candles) {
    try {
      if (!this.dbFile) {
        throw "Database file unspecified";
      }

      const dbConn = this.getDb();
      dbConn
        .prepare(
          `CREATE TABLE IF NOT EXISTS [${table}] (id INTEGER PRIMARY KEY AUTOINCREMENT, open REAL, close REAL, high REAL, low REAL, volume REAL, tradeId INTEGER, startTime INTEGER, endTime INTEGER)`
        )
        .run();

      const insertStmt = dbConn.prepare(
        `INSERT INTO [${table}] (open, close, high, low, volume, tradeId, startTime, endTime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      );
      dbConn.transaction(() => {
        candles.forEach(
          ({ open, close, high, low, volume, tradeId, startTime, endTime }) =>
            insertStmt.run(
              open,
              close,
              high,
              low,
              volume,
              tradeId,
              startTime,
              endTime
            )
        );
      })();
      Logger.debug(`${candles.length} added to ${table}`);
    } catch (e) {
      Logger.error(e.message);
    }
  },

  /**
   * Stores a batch of trades in the sqlite db
   * @param {array} batch - batch of trades
   */
  storeTrades: function(batch) {
    try {
      if (!this.dbFile) {
        throw "Database file unspecified";
      }

      if (!batch || !batch.length) {
        throw "Data must be specified and an array";
      }

      const dbConn = this.getDb();
      const table = `${batch[0].symbol}`;
      dbConn
        .prepare(
          `CREATE TABLE IF NOT EXISTS [${table}] (id INTEGER PRIMARY KEY AUTOINCREMENT, tradeId INTEGER, timestamp INTEGER, price REAL, quantity REAL)`
        )
        .run();

      dbConn
        .prepare(
          `CREATE UNIQUE INDEX IF NOT EXISTS [${table}_tradeId] ON [${table}] (tradeId)`
        )
        .run();

      const insertStmt = dbConn.prepare(
        `INSERT INTO [${table}] (tradeId, timestamp, price, quantity) VALUES (?, ?, ?, ?)`
      );
      dbConn.transaction(() => {
        batch.forEach(({ id, timestamp, info }) =>
          insertStmt.run(id, timestamp, info.p, info.q)
        );
      })();

      Logger.debug(`${batch.length} rows inserted into table`);
    } catch (e) {
      Logger.error(e.message);
    }
  }
};

module.exports = DataManager;
