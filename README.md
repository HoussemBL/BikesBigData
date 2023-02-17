# BikesBigData

# prerequisite
Install spark and Docker

# Preparation step
docker-compose up -d
create a kafka topic called "infobikes" (see kafkacommands document) using the command `
docker-compose exec kafka  \
kafka-topics --create --topic infobikes --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
`







# Scala project

The main classes to execute are:

(1) src/main/scala/solution/CallAPI.scala : here we will call the bike API and send results to the Kafka topic

(2) src/main/scala/solution/CollectBikes.scala: here we will consume the Kafka topic and persist data

(3) src/main/scala/analytics/QueryBikes.scala : analyze persisted data

(4) src/main/scala/analytics/QueryBikes.scala : inspect history of transformations over data