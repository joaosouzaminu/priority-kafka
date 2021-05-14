const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'priority-example',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
let intervalId;

const shutdown = async () => {
  console.log('\nGracefully Shutting Down 😴...\n');
  clearInterval(intervalId);
  await producer.disconnect();
};

const stopSignals = ['SIGTERM', 'SIGINT', 'SIGHUP'];

stopSignals.forEach((signal) => process.on(signal, shutdown));

async function init() {
  await producer.connect();

  const sendLow = async () => {
    const response = await producer.send({
      topic: 'low-priority',
      messages: [{ value: 'low' }],
    });

    const [{ baseOffset: offset }] = response;

    console.log(`[${new Date().toISOString()}] Message offset ${offset} was sent 💬`);
  };

  intervalId = setInterval(sendLow, 200);
}

init();
