const { Kafka } = require('kafkajs');
const { HIGH_PRIORITY_TOPIC, LOW_PRIORITY_TOPIC, RESUME_TIMEOUT, RENEW_TIMEOUT } = require('./config');
const logger = require('./utils/logger');
const { delay } = require('./utils/delay');

class Reader {
  constructor(topics, processMessage) {
    const kafka = new Kafka({
      clientId: 'priority-example',
      brokers: ['localhost:9092'],
      logCreator: logger,
    });

    this._consumer = kafka.consumer({ groupId: 'group1' });
    this._topics = topics;
    this._resumeTimeout = null;
    this._processMessage = processMessage;
    this._batchCounter = 0;

    this._registerProcessHandlers();
  }

  _registerProcessHandlers() {
    const stopSignals = ['SIGTERM', 'SIGINT', 'SIGHUP'];

    stopSignals.forEach((signal) => process.on(signal, this._shutdown.bind(this)));

    process.on('uncaughtException', (err) => {
      this._consumer.disconnect();
      console.error('uncaughtException caught the error: %o ', err);
      process.exit(1);
    });
  }

  async _shutdown() {
    this._consumer.logger().info('Gracefully Shutting Down 😴...\n');
    await this._consumer.disconnect();
    process.exit(0);
  }

  async start() {
    await this._connectConsumer();
    await this._subscribeToTopics();
    await this._runConsumer();

    this._consumer.logger().info('Consumer Started! 🚀\n');
  }

  async _connectConsumer() {
    try {
      await this._consumer.connect();
    } catch (error) {
      this._consumer.logger().error(`Erro conectando consumer: ${error.message}`);
      process.exit(1);
    }
  }

  async _subscribeToTopics() {
    for (const topic of this._topics) {
      await this._consumer.subscribe({ topic });
    }
  }

  async _runConsumer() {
    await this._consumer.run({
      autoCommit: false,
      eachBatch: this._processEachBatch.bind(this),
    });
  }

  async _processEachBatch(payload) {
    await delay(500);
    const { batch, isRunning, isStale, resolveOffset, heartbeat } = payload;
    const { topic, partition } = batch;

    this._pauseLowPriorityTopicIfNecessary(topic);

    for (const message of batch.messages) {
      if (!isRunning() || isStale()) break;
      await this._handleEachMessage(topic, partition, message, resolveOffset, heartbeat);
    }
    this._batchCounter++;
  }

  async _handleEachMessage(topic, partition, message, resolveOffset, heartbeat) {
    await this._processMessage(this._batchCounter, topic, message);
    resolveOffset(message.offset);

    const offset = {
      topic,
      partition,
      offset: (Number(message.offset) + 1).toString(),
    };

    await this._consumer.commitOffsets([offset]);
    await heartbeat();
  }

  _pauseLowPriorityTopicIfNecessary(topic) {
    if (topic === HIGH_PRIORITY_TOPIC) {
      if (!this._isPaused(LOW_PRIORITY_TOPIC)) {
        this._pauseTopic(LOW_PRIORITY_TOPIC);
        this._setResumeTopicTimeout(LOW_PRIORITY_TOPIC, RESUME_TIMEOUT);
      } else if (RENEW_TIMEOUT) {
        this._setResumeTopicTimeout(LOW_PRIORITY_TOPIC, RESUME_TIMEOUT);
      }
    }
  }

  _isPaused(topic) {
    return Boolean(this._consumer.paused().find(({ topic: _topic }) => _topic === topic));
  }

  _pauseTopic(topic) {
    this._consumer.logger().warn(`🖐️ Pausing Topic: ${topic}`);
    this._consumer.pause([{ topic }]);
  }

  _setResumeTopicTimeout = (topic, timeout) => {
    if (this._resumeTimeout && RENEW_TIMEOUT) {
      clearTimeout(this._resumeTimeout);
      this._consumer.logger().warn(`👉 [RENEWED] Will Resume Topic: ${topic} in ${timeout}ms`);
    } else {
      this._consumer.logger().warn(`👉 Will Resume Topic: ${topic} in ${timeout}ms`);
    }

    this._resumeTimeout = setTimeout(() => {
      this._consumer.logger().warn(`👍 Resuming Topic: ${topic}`);
      this._consumer.resume([{ topic }]);
      this._resumeTimeout = null;
    }, timeout);
  };
}

const processMessage = async (batchCounter, topic, { key, offset, value }) => {
  const message = [
    `[${new Date().toISOString()}] 💬 New Message - topic: ${topic}`,
    `batch: ${batchCounter}`,
    `key: ${key}`,
    `offset: ${offset}`,
    `value: ${value.toString()}`,
  ];

  console.log(`${message.join(', ')}\n`);
};

const topics = [HIGH_PRIORITY_TOPIC, LOW_PRIORITY_TOPIC];
const reader = new Reader(topics, processMessage);

reader.start();
