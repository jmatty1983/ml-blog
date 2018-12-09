const ccxt = require ('ccxt');

const Logger = require('../logger/logger');

const limit = 1000;

const exchangeImporter = {
    /**
   * Constructor for OLOO style behavior delgation.
   * @param {string} exchange - exchange name
   */
  init: function (exchange) {
    this.exchange = new ccxt[exchange]({ enableRateLimit: true });

    Logger.info('Exchange import initialized.');
  },
};

module.exports = exchangeImporter;