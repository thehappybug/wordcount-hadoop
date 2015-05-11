all:
	javac -cp .:${HADOOP_HOME}/hadoop-core-1.2.1.jar:${HADOOP_HOME}/hadoop-tools-1.2.1.jar io/github/thehappybug/hadoop/*.java
	jar cf WordAnalyzer.jar io/github/thehappybug/hadoop/*.class