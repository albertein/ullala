FROM ubuntu

RUN apt-get update
RUN apt-get install -y default-jre wget

RUN wget https://download.elastic.co/logstash/logstash/logstash-2.1.3.tar.gz
RUN tar -xvzf logstash-2.1.3.tar.gz

COPY config/ config/

WORKDIR /logstash-2.1.3

CMD ["bin/logstash", "-f", "/config/logstash.conf"]
