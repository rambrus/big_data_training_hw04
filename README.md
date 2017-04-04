# Spark Core homework

## Setup

### Easy way

Download and unzip the package. https://drive.google.com/file/d/0B_ImgSZV-f4DUER0a2o1c1ZKOEk/view?usp=sharing 

It contains everything needed for this project.<br>
Set the SPARK_HOMEWORK environment variable to point to the directory you unzipped the package to.

SPARK_HOMEWORK=<unzipped_package_directory>

**Start IDEA with the idea.bat!**<br>
I modified the batch file and idea.properties and also the spark and mvn cmd files to set all needed environment variables. It should work if SPARK_HOMEWORK environment variable is set correctly.

### Hard way

Download and install everything manually:
* JDK 1.8.0_121 http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
* Maven 3.3.9 https://maven.apache.org/download.cgi
* Hadoop 2.7.3 http://hadoop.apache.org/releases.html
* Spark 2.1.0 Pre-built for Hadoop 2.7 http://spark.apache.org/downloads.html
* IDEA Community Edition https://www.jetbrains.com/idea/download/

Download winutils https://github.com/steveloughran/winutils/archive/master.zip<br>
And copy the contents of hadoop-2.7.1/bin to <hadoop_install_dir>/bin without overwriting existing files

Set environment variables
* JAVA_HOME=<install_dir>\jdk1.8.0_121
* IDEA_JDK=%JAVA_HOME%
* M2_HOME=<install_dir>\apache-maven-3.3.9
* M2=%M2_HOME%\bin
* MAVEN_OPTS=-Xms256m -Xmx512m
* SPARK_HOME=<install_dir>\spark-2.1.0-bin-hadoop2.7
* HADOOP_HOME=<install_dir>\hadoop-2.7.3

Add every bin folder to PATH.

## Homework

Import this project to IDEA and check the ide-scope maven profile.

Run the scala tests and fix all the issues.

There are unimplemented functions in Homework.