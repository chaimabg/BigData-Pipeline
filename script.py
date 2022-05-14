import os

os.system('echo "Pipeline"')
os.system('echo "****************"')

os.system('echo "****** HDFS ******"')
os.system('sleep 4')
os.system('hadoop fs -put dataset.csv ')
os.system('sleep 4')

os.system('echo "****** Map Reduce ******"')
os.system('sleep 4')
os.system('hadoop jar map-reduce.jar tn.insat.project.MapReduce input output')
os.system('sleep 4')
os.system('hadoop fs -get output/part-00000 part-blank')
os.system('sleep 4')
os.system('cat part-blank | tr -d "[:blank:]" > part')
os.system('sleep 4')
os.system('hadoop fs -put part output/part-0000')
os.system('sleep 4')
os.system('hadoop fs -rm  output/part-00000')
os.system('sleep 4')

os.system('echo "****** Create Hbase Table ******"')
os.system('sleep 4')
os.system('java -cp "/usr/local/hbase/lib/*:$KAFKA_HOME/libs/*":. HbaseTable ')
os.system('sleep 4')

os.system('echo "****** Producer Kafka ******"')
os.system('sleep 4')
os.system('java -cp "$KAFKA_HOME/libs/*":. ProducerHbase BigData-Pipeline part')
os.system('sleep 4')

os.system('echo "****** Consumer Kafka ******"')
os.system('sleep 4')
os.system('java -cp "/usr/local/hbase/lib/*:$KAFKA_HOME/libs/*":. ConsumerHbase  BigData-Pipeline')
os.system('sleep 4')

os.system('echo "****** Spark ******"')
os.system('sleep 4')
os.system('spark-submit --class tn.insat.project.HbaseSpark --master --master yarn --deploy-mode client Datafiltering.jar 2> out')
os.system('sleep 4')

os.system('echo "****** End ******"')


                