require('dotenv-safe').config();
const Logger = require('./logger/logger');

Logger.debug('This is a debug message');
Logger.info('This is an info message');
Logger.error('This is an error message');