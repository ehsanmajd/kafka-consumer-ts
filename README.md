# How to run locally?

First you should run the docker containers defined in docker-compose.yml by running `docker-compose up -d` in the terminal. 

After the containers get started successfully then run `npm install` and then `npm run start:dev` to run the application.


To produce some message you can run `docker exec -it kafka-ts_kafka_1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic` and then type your message (or paste a json) and finally press Ctrl + D.