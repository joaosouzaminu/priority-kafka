const { Kafka } = require('kafkajs');
const { delay } = require('../utils/delay');

const kafka = new Kafka({
  clientId: 'priority-example',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function init() {
  await producer.connect();

  const sendLow = async () => {
    await delay(200);

    await producer.send({
      topic: 'low-priority',
      messages: [{ value: 'low' }],
    });
  };

  const sendHigh = async () => {
    await delay(100);

    await producer.send({
      topic: 'high-priority',
      messages: [{ value: 'high' }],
    });
  };

  await sendLow();
  await sendLow();

  await sendHigh();

  await sendLow();
  await sendLow();

  await sendHigh();
  await sendHigh();

  await producer.disconnect();
}

init();
