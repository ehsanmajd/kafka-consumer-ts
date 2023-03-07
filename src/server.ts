import express from 'express';
import KafkaConsumer from "./kafkaConsumer";

const server = express();
server.use(express.json());

const app = server.listen(8080, () => {
  console.info(`Starting server on port 8080`);
  new KafkaConsumer({
    groupId: 'test-group',
    host: 'localhost:9092',
    topic: 'test-topic',
    autoCommit: true
  }, {
    onMessage: async (message, topic, partition) => {
      console.log('Message received');
      
      console.log(message.value?.toString());
    }
  });
});
