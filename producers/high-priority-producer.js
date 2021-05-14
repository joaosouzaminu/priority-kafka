const { Kafka } = require('kafkajs');
const { delay } = require('../utils/delay');

const kafka = new Kafka({
  clientId: 'priority-example',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function init() {
  await producer.connect();

  const sendHigh = async () => {
    await delay(0);

    const response = await producer.send({
      topic: 'high-priority',
      messages: [{ value: 'high' }],
    });

    const [{ baseOffset: offset }] = response;
    console.log(`[${new Date().toISOString()}] Message with offset ${offset} was sent 💬`);
  };

  const messagesToSend = process.argv[2] || 1;

  for (let i = 0; i < messagesToSend; i++) {
    await sendHigh();
  }

  await producer.disconnect();
}

init();
