package stormkafkahdfs.digitalfileanalytics;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

//Create java class named “SimpleProducer”
public class IngestionMetaDataProducer{
   
    public static void main(String[] args){
      
        //Assign topicName to string variable
        String topicName = "imagemetadata";
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        //Assign main node's IP to the bootstrap.servers property
	//(6667)
        props.put("bootstrap.servers", "victoria.com:6667");
        //Set acknowledgements for producer requests.      
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);
        //Assign main node's IP to the metadata.broker.list property
	//(6667)
        props.put("metadata.broker.list", "victoria.com:6667");
        //Assign the IP's of all the nodes in the zookeeper.connect property 
	//(2181)
        props.put("zookeeper.connect", "victoria.com:2181");
        //Using a StringSerializer to produce the messages
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create instance of KafkaProducer
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        //For loop produces numbers 1 to 10 and sends them to the topic 
        producer.send(new ProducerRecord<String, String>(topicName,args[0]));
        System.out.println("Message sent successfully");
        producer.close();
   }
}
