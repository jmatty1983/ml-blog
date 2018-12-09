require("dotenv-safe").config();
const dataDir = process.env.DATA_DIR;
const dbExt = process.env.DB_EXT;

const ExchangeImport = require('./exchangeImporter/exchangeImporter');
const Logger = require('./logger/logger');

const args = process.argv.slice(2);
const actions = ['import'];
let fn = args[0];
let exchange = 'binance';

//Check if a function was specified. If not throw error and exit. If so format it for processing
if (fn) {
  fn = fn.toLowerCase();
} else {
  Logger.error(`You must specify an action. Valid options are: ${actions}`);
  process.exit();
}

switch (fn) {
  case 'import':
    //import requires a pair to be specified
    if (args[1]) {
      const exchangeImport = Object.create(ExchangeImport);
      exchangeImport.init(exchange, dataDir, dbExt);
      exchangeImporter.getPair(args[1])
    } else {
      Logger.error('No pair provided');
    }
    break;
  default:
    Logger.error(`Invalid action ${fn}. Valid options are: ${actions}`);
    break;
}