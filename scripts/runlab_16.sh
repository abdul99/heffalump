CLASSNAME=com.experimental.cascading.WordCountCascading

export HADOOP_CLASSPATH=/usr/lib/cascading/*:/usr/lib/cascading/lib/*
# export HADOOP_USER_CLASSPATH_FIRST=true

echo Cleaning old output directory...
hadoop fs -rm -r /user/shrek/output
echo
echo Copying data file to input folder
hadoop fs -copyFromLocal -f data/rain.txt input/rain.txt 
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME input/rain.txt output"
echo $command
$command
echo ...Done
echo
echo Here is the output from HDFS...
hadoop fs -cat output
echo
