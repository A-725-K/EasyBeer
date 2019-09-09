#!/bin/bash

if [ $1 == "KMeans_Clustering" ]; then
	hdfs dfs -rm -r res_cl
fi

if [ $1 == "C" ]; then
	rm -rf *.class
elif [ $1 == "CJAR" ]; then
	rm -rf ../*.jar
else
	file="$1.java"
	cd ..
	javac -classpath $SPARK_CLASSPATH spark/$file
	jar cf $1.jar spark
	spark-submit --master yarn --class spark.$1 $1.jar
fi
