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
  const topic = process.argv[2];

  if (!topic) {
    console.log('Informe um tópico!');
    process.exit(1);
  }

  const sendLow = async () => {
    const response = await producer.send({
      topic: topic,
      messages: [{ value: `${topic} - message` }],
    });

    const [{ baseOffset: offset }] = response;

    console.log(`[${new Date().toISOString()}] Message offset ${offset} was sent 💬`);
  };

  intervalId = setInterval(sendLow, 200);
}

init();
