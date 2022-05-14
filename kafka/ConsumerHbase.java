import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ConsumerHbase {
	
	private Table table;
    private String tableName = "covid";
    private String family = "usa-covid";
	
  public void consume(String[] args) throws Exception {
    if(args.length == 0){
       System.out.println("Enter topic name");
       return;
    }
    String topicName = args[0].toString();
    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer",
       "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
       "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer
       <String, String>(props);

    // Kafka Consumer va souscrire a la liste de topics ici
    consumer.subscribe(Arrays.asList(topicName));

    // Afficher le nom du topic
    System.out.println("Souscris au topic " + topicName);
    
    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    table = connection.getTable(TableName.valueOf(tableName));
    
    int i = 0;

    while (true) {
       ConsumerRecords<String, String> records = consumer.poll(100);
       for (ConsumerRecord<String, String> record : records) {
    	   if(record.key().equals("STOP")) {
    		   System.out.println("Number of messages " + i);
               table.close();
               connection.close();
    		   System.exit(0);
    		   }
    	   
           String s1=record.key();
           String s2=record.value();
           try {
               byte[] row = Bytes.toBytes("cases_"+s1);
               Put p = new Put(row);

               p.addColumn(family.getBytes(), "state".getBytes(), Bytes.toBytes(s1));
               p.addColumn(family.getBytes(), "cases".getBytes(), Bytes.toBytes(s2));
               table.put(p);

           } catch (Exception e) {
               e.printStackTrace();
           }
    	   
   
       // Afficher l'offset, clef et valeur des enregistrements du consommateur
       /*System.out.printf("offset = %d, key = %s, value = %s\n",
          record.offset(), record.key(), record.value());*/
       
       i++;
     } 
    }
  }
  public static void main(String[] args) throws Exception {
	  ConsumerHbase consumer = new ConsumerHbase();
      consumer.consume(args);
  }
  
}