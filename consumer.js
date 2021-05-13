const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'priority-example',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'group1' });

const HIGH_PRIORITY_TOPIC = 'high-priority';
const LOW_PRIORITY_TOPIC = 'low-priority';

const topics = [HIGH_PRIORITY_TOPIC, LOW_PRIORITY_TOPIC];
let resumeTimeout;

async function init() {
  await consumer.connect();

  const promises = topics.map((topic) => consumer.subscribe({ topic }));
  await Promise.all(promises);

  const processMessage = ({ key, offset, value }, topic) => {
    console.log(
      `[${new Date().toISOString()}] 💬 New Message - topic: ${topic}, key: ${key}, offset: ${offset}, value: ${value.toString()}\n`
    );
  };

  const pauseTopic = (topic) => {
    console.log(`🖐️ Pausing Topic: ${topic}`);
    consumer.pause([{ topic }]);
  };

  const setResumeTopicTimeout = (topic, timeout) => {
    console.log(`👉 Will Resume Topic: ${topic} in ${timeout}ms`);

    clearTimeout(resumeTimeout);
    resumeTimeout = setTimeout(() => {
      console.log(`👍 Resuming Topic: ${topic}`);
      consumer.resume([{ topic }]);
    }, timeout);
  };

  const isPaused = (topic) => {
    return Boolean(consumer.paused().find(({ topic: _topic }) => _topic === topic));
  };

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, isRunning, isStale, resolveOffset, heartbeat }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        if (batch.topic === HIGH_PRIORITY_TOPIC && !isPaused(LOW_PRIORITY_TOPIC)) {
          pauseTopic(LOW_PRIORITY_TOPIC);
          setResumeTopicTimeout(LOW_PRIORITY_TOPIC, 5_000);
        }

        await processMessage(message, batch.topic);
        resolveOffset(message.offset);

        const { topic, partition } = batch;

        const offset = {
          topic,
          partition,
          offset: (Number(message.offset) + 1).toString(),
        };

        await consumer.commitOffsets([offset]);
        await heartbeat();
      }
    },
  });

  console.log('\nConsumer Started! 🚀\n');
}

init();

const shutdown = async () => {
  console.log('\nGracefully Shutting Down 😴...\n');
  await consumer.disconnect();
  process.exit(0);
};

process
  .on('SIGTERM', shutdown)
  .on('SIGINT', shutdown)
  .on('SIGHUP', shutdown)
  .on('uncaughtException', (err) => {
    firstReader.disconnect();
    console.error('uncaughtException caught the error: %o ', err);
    process.exit(1);
  });
