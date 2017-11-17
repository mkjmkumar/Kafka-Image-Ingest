package stormkafkahdfs.digitalfileanalytics;
//Receives a video as a ByteArray through a producer of the given topic and compiles it into a video on the machine in which the code is run
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 
import org.apache.hadoop.hbase.filter.FilterList.Operator; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.RegexStringComparator; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.util.Bytes; 

public class IngestByteSerializerAsConsumer {
	private static String hbaseZookeeperQuorum="victoria.com";
    private static String hbaseZookeeperClientPort="2181";
    private static String TheValue;
	private static HTable table;
	private static KafkaConsumer<String, byte[]> consumer;

	public static void main(String[] args) throws Exception {
        
        //Kafka consumer configuration settings
        String topicName = "imageobject";
        Properties props = new Properties();
        //Assign the IP of the master node to the metadata.broker.list property
	//(6667)
        props.put("metadata.broker.list", "victoria.com:6667");
        //Assign the IP's of all the nodes in the cluster along with the port on which zookeeper is running to the zookepper.connect property
	//(2181)
        props.put("zookeeper.connect", "victoria.com:2181");
        //Assign the IP of the master node to the bootstrap.server property
	//(6667)
        props.put("bootstrap.servers", "victoria.com:6667");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("parse.key", true);
        //Using a ByteArrayDeserializer as the producer used a ByteArraySerializer to produce the video 
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //KafkaConsumer<String, byte[]> consumer = new KafkaConsumer <String, byte[]>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer = new KafkaConsumer <String, byte[]>(props);
        consumer.subscribe(Arrays.asList(topicName));
        //print the topic name
        System.out.println("Subscribed to topic " + topicName);      
        int counter=1;
        //Infinite loop to keep the consuming ongoing
        while (true) {
    	    byte[] b= new byte[100000];
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            //Loop through all the records in the given poll
            for (ConsumerRecord<String,byte[]> record : records) {
                b=record.value();
                //Print the ByteArray in the terminal
                for(int i=0;i<b.length;i++) {
        	        System.out.println(b[i]);
                }
                ///////////////////
                Configuration config = HBaseConfiguration.create();           
                config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
                config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);        
                config.set("zookeeper.znode.parent", "/hbase-unsecure");
                table = new HTable(config, "kafkaMessages"); 
        		table.setScannerCaching(5); 
      		  long begin = System.currentTimeMillis(); 
      		  Scan scan = new Scan(); 
      		  scan.setCacheBlocks(true); 
              // Instantiating Put class. Accepts a row key.
              //TimeStamp ts = new TimeStamp(new Date());
              //Date d = ts.getDate();
    		  //SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("image_meta"), Bytes.toBytes("X"), CompareOp.EQUAL, new RegexStringComparator(d.toString().replaceAll("\\s+","").replaceAll(":", ""))); 
    		  //filter.setFilterIfMissing(true); 
    		  //List<Filter> list = new ArrayList<Filter>(); 
    		  //list.add(filter); 
    		  //FilterList fl = new FilterList(Operator.MUST_PASS_ALL, list); 
    		  //scan.setFilter(fl); 
    		  //ResultScanner rs = table.getScanner(scan); 
    		  //Result res = null; 
    		  //try { 
    		  // while ((res = rs.next()) != null) { 
    		  //  //String rowid = Bytes.toString(res.getRow()); 
    		  //  String value = Bytes.toString(res.getValue(Bytes.toBytes("image_meta"), Bytes.toBytes("ingestsource"))); 
    		  //  //System.out.println(value);
    		  //  TheValue = Bytes.toString(res.getValue(Bytes.toBytes("image_meta"), Bytes.toBytes("ingestsource"))); 
    		  //  //System.out.println(res); 
//    		  //  System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("d1")))); 
    		  // } 
    		  //} finally { 
    		  // rs.close(); 
    		  //} 
              // Instantiating HTable class
              Get g = new Get(Bytes.toBytes("dual"));

              // Reading the data
              Result result = table.get(g);

              // Reading values from Result class object
              byte [] value = result.getValue(Bytes.toBytes("image_meta"),Bytes.toBytes("X"));

              // Printing the values
              String name = Bytes.toString(value);
              
              System.out.println("name: " + name );
              
    		  long end = System.currentTimeMillis(); 
    		  //System.out.println("HBASE OPERATION is In progfress");
    		  //System.out.println(end - begin); 
    		  //System.out.println(TheValue);
                //////////////////
                //StringBuilder instance renames the consumed files to FinalVideo1, FinalVideo2, etc. with a counter
                StringBuilder sb = new StringBuilder();
                //Append the counter to the path of the file 
                sb.append("/home/victoria/Desktop/TEST/OCR/");
                //sb.append(counter);
                sb.append(name);
                //sb.append(args[0]);
                String output = sb.toString();
                //Print the output path
                System.out.println(output);
                File someFile = new File(output);
                //Create a ouput stream to the output file
                FileOutputStream out = new FileOutputStream(someFile);
                //Write the ByteArray recieved from the producer into the file
      	        out.write(b);
      	        out.close();
      	        counter++;
            } 
        }
	}

}
