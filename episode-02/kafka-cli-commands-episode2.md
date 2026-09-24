# Kafka CLI Commands — Episode 2

## Setup

```yaml
services:
  kafka:
    image: apache/kafka:4.0.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
```

```powershell
docker compose up -d
```

```powershell
docker ps
```

## Topic management

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

## Producer

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092
```

## Consumer (basic)

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092
```

## Consumer with partition + offset shown

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 --property print.partition=true --property print.offset=true
```

## Consumer from the beginning

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 --from-beginning
```

## Consumer groups

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 --group order-service
```

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service
```

## Cleanup

```powershell
docker compose down -v
```
