# Hadoop

## Installation

### Windows 10
1. Select a [version](https://archive.apache.org/dist/hadoop/core/) and download the ``hadoop-x.x.x.tar.gz`` file

2. Unzip ([7zip]( https://7-zip.de/download.html ) works with ``.tar.gz``) and move to ``C:\hadoop-x.x.x``

3. Create a new user environment variable ``HADOOP_HOME`` with the Hadoop installations root directory 

   ![user enviroment variable](https://i.imgur.com/RNmEsUs.png)

4. Add the binaries directory as a new value for the already existing ```Path`` variabel

   ![path variable](https://i.imgur.com/wIdSWq8.png)

5. Download the corresponding version of Hadoop [winutils](https://github.com/steveloughran/winutils)

6. Create a new subdirectory ``C:\hadoop-x.x.x\winutils`` and move the binaries

7. Copy the ``hadoop.dll`` file from winutils into ``C:\Windows\System32``

8. Import the necessary ```.jar`` Files from  ``C:\hadoop-x.x.x\share\hadoop`` into your IDE

9. Edit your run configurations with extra parameters to tune the Java VM

   + ``-Dhadoop.home.dir=C:\hadoop-x.x.x``
   + ``-Djava.library.path=C:\hadoop-x.x.x\winutils``

10. Run

### Docker

+ [AWS Docker](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html)
  + Security Group: ``All Trafic Anywhere``
+ [Running Hadoop in Docker](https://amitasviper.github.io/2018/04/24/running-hadoop-in-docker-on-mac.html)
  + ``sudo yum install``
  + ``sudo docker pull sequenceiq/hadoop-docker:2.7.1``
  + ``sudo docker run -it -p 50070:50070 sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash``

