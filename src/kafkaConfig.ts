export interface KafkaConfig {
  host: string;
  groupId: string;
  autoCommit: boolean;
  topic: string;
  fromBeginning?: boolean;
}

export default KafkaConfig;
