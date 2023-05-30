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
RUN echo "export PATH=$PATH:$JAVA_HOME/bin" >> /etc/bash.bashrc

#Scala 2.11.8
RUN apt-get remove scala-library scala
RUN wget www.scala-lang.org/files/archive/scala-2.11.8.deb

#SBT
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list

RUN apt-get install -y gnupg2
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update
RUN apt-get install sbt -y
