require("dotenv-safe").config();
const dataDir = process.env.DATA_DIR;
const dbExt = process.env.DB_EXT;

const ExchangeImport = require("./exchangeImporter/exchangeImporter");
const Logger = require("./logger/logger");

const args = process.argv.slice(2);
const actions = ["import"];
let fn = args[0];
let exchange = "binance";

//Check if a function was specified. If not throw error and exit. If so format it for processing
if (fn) {
  fn = fn.toLowerCase();
} else {
  Logger.error(`You must specify an action. Valid options are: ${actions}`);
  process.exit();
}

switch (fn) {
  case "import":
    //import requires a pair to be specified
    if (args[1]) {
      const exchangeImport = Object.create(ExchangeImport);
      exchangeImport.init(exchange, dataDir, dbExt);
      exchangeImporter.getPair(args[1]);
    } else {
      Logger.error("No pair provided");
    }
    break;
  case "process":
    try {
      if (!args[1]) {
        throw "No pair provided";
      }

      if (!args[2]) {
        throw "No lengths provided";
      }

      const [, pair, length] = args;
      const dataManager = Object.create(DataManager);
      dataManager.init(exchange, dataDir, dbExt);

      //Allow processing candles of multiple durations with 1 command to save some time by not having to reload the data
      const lengths = length.split(",");
      dataManager.processCandles(pair, lengths);
    } catch (e) {
      Logger.error(e.message);
    }
    break;
  default:
    Logger.error(`Invalid action ${fn}. Valid options are: ${actions}`);
    break;
}
