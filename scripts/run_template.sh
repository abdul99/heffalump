CLASSNAME=com.hadooptraining.summarize.WordCount

#source $DEV_HOME/scripts/setclasspath.sh
#java $JVM_OPTIONS -cp "$CLASSPATH" $CLASSNAME $1 $2 $3 $4 $5 $6 $7

hadoop jar $DEV_HOME/target/heffalump-1.0.jar $CLASSNAME $1 $2 $3 $4 $5 $6

