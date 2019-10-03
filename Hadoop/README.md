# Hadoop

## Installation

### Windows 10
+ [Installationsanleitung](https://github.com/MuhammadBilalYar/Hadoop-On-Window/wiki/Step-by-step-Hadoop-2.8.0-installation-on-Window-10)
+ `set JAVA_HOME=C:\Progra~1\Java\jdk1.8.0_181\` in hadoop-env.cmd
+ Configurations: Siehe .xml files

### Docker
+ [AWS Docker](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html)
	+ Security Group: ``All Trafic Anywhere``
+ [Running Hadoop in Docker](https://amitasviper.github.io/2018/04/24/running-hadoop-in-docker-on-mac.html)
	+ ``sudo yum install``
	+ ``sudo docker pull sequenceiq/hadoop-docker:2.7.1``
	+ ``sudo docker run -it -p 50070:50070 sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash``