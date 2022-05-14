import java.util.Properties;
import java.io.*;  
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

 public class ProducerHbase {

   public static void main(String[] args) throws Exception{

      if(args.length == 0){
         System.out.println("Enter the name of the topic");
         return;
      }
      if(args.length == 1){
          System.out.println("Enter the data file");
          return;
       }     

      String topicName = args[0].toString();
      String fileName = args[1].toString();

      // Creer une instance de proprietes pour acceder aux configurations du producteur
      Properties props = new Properties();

      // Assigner l'identifiant du serveur kafka
      props.put("bootstrap.servers", "localhost:9092");

      // Definir un acquittement pour les requetes du producteur
      props.put("acks", "all");

      // Si la requete echoue, le producteur peut reessayer automatiquemt
      props.put("retries", 0);

      // Specifier la taille du buffer size dans la config
      props.put("batch.size", 16384);

      // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
      props.put("buffer.memory", 33554432);

      props.put("key.serializer",
         "org.apache.kafka.common.serialization.StringSerializer");

      props.put("value.serializer",
         "org.apache.kafka.common.serialization.StringSerializer");
      
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
      
      try  
      {  
      File file=new File(fileName);    
      FileReader fr=new FileReader(file);   
      BufferedReader br=new BufferedReader(fr);   
      String line;  
      while((line=br.readLine())!=null)  
      {  
    	  String[] parts = line.split("\t");
    	  producer.send(new ProducerRecord<String, String>(topicName,
    			  parts[0], parts[1]));
      }  
      fr.close();   
      }  
      catch(IOException e)  
      {  
      e.printStackTrace();  
      }
      
      producer.send(new ProducerRecord<String, String>(topicName,
	            "STOP", "STOP"));
      
      System.out.println("Message successfully sent");
      producer.close();
   }
 }