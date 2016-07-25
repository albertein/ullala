FROM ubuntu

RUN apt-get update
RUN apt-get install -y default-jre wget

RUN wget http://www.eu.apache.org/dist/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz
RUN tar -xvzf zookeeper-3.4.8.tar.gz

RUN mkdir -p /var/lib/zookeeper

VOLUME /var/lib/zookeeper

WORKDIR /zookeeper-3.4.8

COPY config/zoo.cfg conf/

EXPOSE 2181

CMD ["bin/zkServer.sh", "start-foreground"]
