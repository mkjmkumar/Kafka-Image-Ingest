package stormkafkahdfs.digitalfileanalytics;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class RetriveData{
	private static String hbaseZookeeperQuorum="victoria.com";
    private static String hbaseZookeeperClientPort="2181";
    private static TableName tableName = TableName.valueOf("kafkaMessages");
    private static final String columnFamily = "image_meta";
   public static void main(String[] args) throws IOException, Exception{
       Configuration config = HBaseConfiguration.create();           
       config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
       config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);        
       config.set("zookeeper.znode.parent", "/hbase-unsecure");
       Connection c = ConnectionFactory.createConnection(config);
       // Check if table exists
       Admin admin = c.getAdmin();

	   
      // Instantiating Configuration class

      // Instantiating HTable class
      HTable table = new HTable(config, "kafkaMessages");

      // Instantiating Get class
      Get g = new Get(Bytes.toBytes("dual"));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("image_meta"),Bytes.toBytes("X"));

      // Printing the values
      String name = Bytes.toString(value);
      
      System.out.println("name: " + name );
   }
}