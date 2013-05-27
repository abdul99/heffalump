CLASSNAME=com.hadooptraining.lab3.FileCopy

echo Cleaning old output directory...
hadoop fs -rm -r /user/shrek/output
echo
echo Running Hadoop task...
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME data/sample1.txt /user/shrek/output/sample1_copy.txt"
echo $command
$command
echo ...Done
echo
echo Here is the output from HDFS...
hadoop fs -cat /user/shrek/output/sample1_copy.txt
echo
