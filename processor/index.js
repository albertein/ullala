'use strict';

const fs = require('fs');
const kafka = require('no-kafka');
const jimp = require('jimp');
const avro = require('avsc');
const kafka_hosts = process.env.KAFKA_HOSTS;

if (!kafka_hosts) {
  console.error('No $KAFKA_HOSTS environment variable set');
  process.exit(1);
}

const avroType = avro.parse({
  name: 'Image',
  type: 'record',
  fields: [
    {name: 'image', type: 'bytes'},
    {name: 'capture_timestamp', type: 'int'}
  ]
});

const workerId = Math.floor(Math.random() * 10000) + 1;

const consumer = new kafka.GroupConsumer({
  connectionString: kafka_hosts,
  groupId: 'IMAGE_PROCESSOR_I',
  clientId: `IMAGE_PROCESSOR_${workerId}`,
  logger: {
    logFunction: info
  }
});

const producer = new kafka.Producer({
  connectionString: kafka_hosts
});

function init() {
  producer.init();
  consumer.init([{
    strategy: 'DEFAULT_STRATEGY',
    subscriptions: ['CAMERA_FEED'],
    handler: handleMessages 
  }]);
}

function processImage(packedImage, callback) {
  jimp.read(packedImage.image, (error, image) => {
    if (error) {
      error(`ERROR: ${error}`);
      return;
    }
    image.scale(0.25)
    .blur(3)
    .sepia()
      .getBuffer(jimp.MIME_JPEG, (error, image) => {
        if (error) {
          callback(error);
        } else {
          packedImage.image = image;
          callback(null, packedImage);
        }
      });
  });
}

function handleMessages(messageSet, topic, partition) {
  for (const message of messageSet) {
    processImage(avroType.fromBuffer(message.message.value), (error, processedImage) => {
      if (error) {
        error(`ERROR: ${error}`);
        return;
      }
      outputImage(processedImage, partition, message.offset);
      consumer.commitOffset({topic: topic, partition: partition, offset: message.offset});
    });
  }
}

function getConsumerLag(captureTimestamp) {
  const captureDate = new Date(0);
  captureDate.setUTCSeconds(captureTimestamp);
  return (new Date() - captureDate) / 1000;
}

function outputImage(packedImage, partition, offset) {
  const consumerLag = getConsumerLag(packedImage.capture_timestamp)
  producer.send({
    topic: 'PROCESSED_FEED',
    message: {
      value: avroType.toBuffer(packedImage) 
    },
    partition: 0
  });
  logConsumerLag(`Image ${partition}_${offset} written with consumer lag: ${consumerLag}`, consumerLag);
}

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
    'application': 'IMAGE_PROCESSOR',
    'worker': workerId
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

init();
