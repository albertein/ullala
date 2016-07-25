# ullala

ullala is a demo that showcase using kafka as a data pipeline and log aggregation through the ELK stack

## Setting up

1. Download kafka command line tools: `scripts/download-kafka.sh`
2. Run Kafka, Zookeeper, Elasticsearch and Logstash: `docker-compose up`
3. Set kafka on your host file to point to the running kafka instance (docker-machine ip default on mac)
3. Setup topics `scripts/setup-topics.sh`

