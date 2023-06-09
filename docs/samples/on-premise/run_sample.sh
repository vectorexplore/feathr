export FEATHR_SANDBOX=True
export API_BASE="api/v1"
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
#export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

# use share spark jar
# zip -j spark340-hadoop3-jars.zip  $SPARK_HOME/jars/*
# hadoop fs -put spark340-hadoop3-jars.zip /share/

# config $SPARK_HOME/conf/spark-defaults.conf with the following 
# spark.driver.host		192.168.122.1
# spark.driver.bindAddress	192.168.122.1
# deploy-mode			client
# spark.yarn.archive		hdfs://xxx:9000/share/spark340-hadoop3-jars.zip


# Set REDIS_PASSWORD in Environment(e.g. .bashrc)
# export REDIS_PASSWORD=123456

# copy jar 
# cp ~/feathr/build/libs/feathr_2.12-1.0.2-rc11.jar .

python $1
