FROM ubuntu:18.04

# Install updates to base image

RUN \
  apt-get update -y \
  && apt-get install -y \
  && apt-get install wget -y \
  && apt-get install htop -y \
  && apt-get install nano -y

#JAVA

RUN apt install openjdk-8-jdk -y
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/" >> /etc/bash.bashrc
RUN echo "export PATH=$PATH:/usr/lib/jvm/java-8-openjdk-amd64/jre/bin" >> /etc/bash.bashrc

#SPARK

WORKDIR /root
RUN wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
RUN tar -xvf spark-3.4.0-bin-hadoop3.tgz
RUN echo "SPARK_HOME=/root/spark-3.4.0-bin-hadoop3" >> /etc/bash.bashrc
RUN echo "export PATH=$PATH:/root/spark-3.4.0-bin-hadoop3/bin:/root/spark-3.4.0-bin-hadoop3/sbin" >> /etc/bash.bashrc


#PYTHON

RUN apt-get update && apt-get install python3-pip -y

RUN echo "export PYSPARK_PYTHON=/usr/bin/python3" >> /etc/bash.bashrc
