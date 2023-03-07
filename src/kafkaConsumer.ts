import { Consumer, Kafka, logLevel, KafkaMessage } from 'kafkajs';
import KafkaConfig from './kafkaConfig';

type KafkaConsumerHandlers = {
  onMessage?: SingleMessageHandler;
  // onBatch?: MessageBatchHandler;
};

export type SingleMessageHandler = (
  message: KafkaMessage,
  topic?: string,
  partition?: number,
) => Promise<void>;

// type MessageBatchHandler = (message: KafkaMessage[]) => Promise<void>;

const doNothing = () => Promise.resolve();

export class KafkaConsumer {
  private kafka: Kafka;
  private consumer: Consumer | undefined;
  private config: KafkaConfig;
  private onMessage: SingleMessageHandler;

  private joinedGroup = false;
  private crashed = false;
  private eventsBound = false;

  constructor(config: KafkaConfig, handlers: KafkaConsumerHandlers) {
    this.config = {
      fromBeginning: true,
      ...config,
    };
    const { host } = this.config;

    this.onMessage = handlers.onMessage ?? doNothing;

    const brokers = host.split(',');
    const clientId = 'clientId'; // todo: bring this to the config

    console.log(`Bootstrapping kafka consumer. topic:"${config.topic}" host:"${config.host}" consumer group:"${config.groupId}"`);

    this.kafka = new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.WARN,
      connectionTimeout: 10000,
      requestTimeout: 60000,
      retry: {
        retries: 3,
        initialRetryTime: 10000,
      },
    });

    this.bootstrapNewConsumer().then();
  }

  private async bootstrapNewConsumer() {
    const { autoCommit, topic, fromBeginning, groupId } = this.config;

    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 90000,
      heartbeatInterval: 30000,
      rebalanceTimeout: 180000,
      retry: {
        initialRetryTime: 5000,
      },
    });

    this.bindStateEvents();

    await this.consumer.connect();

    await this.consumer.subscribe({ topic, fromBeginning });

    await this.consumer.run({
      autoCommit,
      autoCommitInterval: 3000,
      partitionsConsumedConcurrently: 2,
      eachMessage: async ({ message, topic, partition }) => {
        return this.onMessage(message, topic, partition);
      },
    });
  }

  private bindStateEvents = async () => {
    if (this.consumer && !this.eventsBound) {
      this.consumer.on(this.consumer.events.GROUP_JOIN, () => (this.joinedGroup = true));
      this.consumer.on(this.consumer.events.CRASH, () => this.crashed);
      this.eventsBound = true;
    }
  };

  private stopAndDisconnect = async () => {
    if (!this.consumer) {
      return;
    }

    await this.consumer.stop();
    await this.consumer.disconnect();

    this.eventsBound = false;
    this.consumer = undefined;
  };

  public resetOffset = async () => {
    const { groupId, topic } = this.config;
    const earliest = true;
    const kafkaAdmin = this.kafka.admin();

    await this.stopAndDisconnect();

    await kafkaAdmin.resetOffsets({ topic, groupId, earliest });

    await this.bootstrapNewConsumer();
  };
}

export default KafkaConsumer;
