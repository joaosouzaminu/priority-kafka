const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'priority-example',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function init() {
  await producer.connect();

  const lowInterval = setInterval(() => {
    producer.send({
      topic: 'low-priority',
      messages: [{ value: 'bye world' }],
    });
  }, 500);

  const highInterval = setInterval(() => {
    producer.send({
      topic: 'high-priority',
      messages: [{ value: 'hello world' }],
    });
  }, 2000);

  await new Promise((resolve) => {
    setTimeout(() => {
      clearInterval(lowInterval);
      clearInterval(highInterval);
      resolve();
    }, 10000);
  });

  await producer.disconnect();
}

init();
