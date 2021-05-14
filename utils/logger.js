const { logLevel } = require('kafkajs');
const { logger } = require('minutrade-logger');

const toWinstonLogLevel = (level) => {
  switch (level) {
    case logLevel.ERROR:
    case logLevel.NOTHING:
      return 'error';
    case logLevel.WARN:
      return 'warn';
    case logLevel.INFO:
      return 'info';
    case logLevel.DEBUG:
      return 'debug';
  }
};

const WinstonLogCreator = () => {
  return ({ level, log }) => {
    const { message, ...extra } = log;

    delete extra.logger;
    delete extra.timestamp;
    const hasExtra = Object.keys(extra).length > 0;
    const extraMessage = hasExtra ? `,\n${extra && JSON.stringify(extra, null, 2)}` : '';

    logger.log({
      level: toWinstonLogLevel(level),
      message: `${message}${extraMessage}`,
    });
  };
};

module.exports = WinstonLogCreator;
