import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;


public class UserTable {
	private static Configuration config = HBaseConfiguration.create();
	private static final String tableName = "user"; 
	public static void createTable() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.tableExists(tableName)) {
			System.out.println("Drop table " + tableName);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);

		HColumnDescriptor userId = new HColumnDescriptor("userId");
		userId.setMaxVersions(Integer.MAX_VALUE);
		userId.setBloomFilterType(StoreFile.BloomType.ROW);
		tableDesc.addFamily(userId);
		admin.createTable(tableDesc);
		System.out.println("table create ok!!!");
	}
	public static void insertData() throws IOException {
		HTable table = new HTable(config, tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(64*1024*1024);
		Put put = new Put();
		Jedis server = new Jedis("10.10.102.17",6379, Integer.MAX_VALUE);
		String [] ids = server.smembers("userid").toArray(new String[0]);
		for(String tmp:ids) {
			put.add(Bytes.toBytes("userid"), null, Bytes.toBytes(tmp));
		}
		table.put(put);
	}
	
	
	public static void main(String [] args) throws IOException {
		createTable();
		insertData();
	}
}
