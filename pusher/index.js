const fs = require('fs');

const http = require('http');
const express = require('express');
const socketIO = require('socket.io');
const kafka = require('no-kafka');
const avro = require('avsc');

const app = express();
const server = http.Server(app);
const io = socketIO().listen(server);

const kafka_hosts = process.env.KAFKA_HOSTS;

const avroType = avro.parse({
  name: 'Image',
  type: 'record',
  fields: [
    {name: 'image', type: 'bytes'},
    {name: 'capture_timestamp', type: 'int'}
  ]
});

if (!kafka_hosts) {
  console.error('No $KAFKA_HOSTS environment variable set');
  process.exit(1);
}

server.listen(8080);
app.use(express.static('static'));

app.get('/', function (req, res) {
  res.sendfile(`${__dirname}/static/index.html`);
});

function handleKafkaMessages(messageSet, topic, partition) {
  for (const message of messageSet) {
    const imageFileName = `${partition}_${message.offset}.jpg`;
    const packedImage = avroType.fromBuffer(message.message.value);

    fs.writeFile(`static/images/${imageFileName}`, packedImage.image, (error) => {
      if (error) {
        error(`ERROR: Writing ${imageFileName}`);
        return;
      }
      const consumerLag = getConsumerLag(packedImage.capture_timestamp);
      logConsumerLag(`Got image ${imageFileName} with consumer lag: ${consumerLag}`, consumerLag);
      io.emit('image', {
        path: imageFileName,
        captureTimestamp: packedImage.capture_timestamp
      });
    });
  }
}

function getConsumerLag(captureTimestamp) {
  const captureDate = new Date(0);
  captureDate.setUTCSeconds(captureTimestamp);
  return (new Date() - captureDate) / 1000;
}

const consumer = new kafka.GroupConsumer({
  connectionString: kafka_hosts,
  groupId: 'IMAGE_PUSHER',
  logger: {
    logFunction: info
  }
});

const producer = new kafka.Producer({
  connectionString: kafka_hosts
});

consumer.init([{
  strategy: 'DEFAULT_STRATEGY',
  subscriptions: ['PROCESSED_FEED'],
  handler: handleKafkaMessages
}]);

producer.init();

function info() {
  const argumentList = Array.prototype.slice.call(arguments);
  console.log.apply(console, argumentList);
  log('INFO', argumentList.join(' '));
}

function error(string) {
  const argumentList = Array.prototype.slice.call(arguments);
  console.error.apply(console, argumentList);
  log('ERROR', argumentList.join(' '));
}

function logConsumerLag(string, consumerLag) {
  console.log(string);
  log('INFO', string, consumerLag);
}

function log(level, string, consumerLag) {
  const logPayload = {
    '@timestamp': new Date().toISOString(),
    'level': level,
    'message': string,
    'application': 'IMAGE_PUSHER'
  };
  if (consumerLag) {
    logPayload.consumerLag = consumerLag;
  }
  producer.send({
    topic: 'APPLICATION_LOGS',
    message: {
      value: JSON.stringify(logPayload)
    },
    partition: 0
  });
}

