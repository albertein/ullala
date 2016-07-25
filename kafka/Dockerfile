FROM ubuntu

RUN apt-get update
RUN apt-get install -y default-jre wget


RUN wget http://www.eu.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz

RUN tar -xvzf kafka_2.11-0.9.0.0.tgz

EXPOSE 9092

COPY config/ config/

WORKDIR /kafka_2.11-0.9.0.0/

RUN mkdir -p /tmp/kafka-logs

VOLUME /tmp/kafka-logs

CMD ["bin/kafka-server-start.sh", "/config/server.properties"]
