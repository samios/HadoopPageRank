#
#!!!!!!!!!!!!!!!!!!!! IMPORTANT  !!!!
# This program requires to set the environment variables correctly by adding the following lines to the .bashrc file  :
#alias Sethadoop='export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
#export HADOOP_HOME=/usr/local/hadoop-2.7.1
#export PATH=$HADOOP_HOME/bin:$PATH
#export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar:${PWD}/bin' 
#-------------------------------------------------------------------------------


JAVACC=javac
JAR=jar
BIN=./bin
SRC=./src/*.java


#use make clean to delete all class and jar files

clean:	
	$(RM) $(BIN)/*.class
	$(RM) $(BIN)/*.jar

#use make all to create the class files and jar


all: pagerank.jar



pagerank.jar: PageRank.class 
	
	$(JAR) cvf $(BIN)/$@  $(BIN)	

PageRank.class: $(SRC) 
	hadoop com.sun.tools.javac.Main -d $(BIN) $?

#use make run to launch the program the default warc file can be changed from here

run:
	hadoop jar bin/pagerank.jar PageRank 8583l

