CLASSNAME=com.hadooptraining.lab3.FileCopy

echo Cleaning input and output directories...
hadoop fs -rm -r output
hadoop fs -rm -r input
hadoop fs -mkdir input
echo
command="hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME data/sample1.txt output/sample1_copy.txt"
echo $command
$command
echo ...Done
echo
echo Here are the files produced in output folder...
hadoop fs -ls output
echo
echo To see your result, type:
echo hadoop fs -cat output/sample1_copy.txt
