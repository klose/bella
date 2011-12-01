import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
public class FollowingTable {
	private static Configuration config = HBaseConfiguration.create();
	private static final String tableName = "following_test"; 
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

		HColumnDescriptor followId = new HColumnDescriptor("followId");
		followId.setMaxVersions(Integer.MAX_VALUE);
		followId.setBloomFilterType(StoreFile.BloomType.ROW);
		tableDesc.addFamily(followId);

		admin.createTable(tableDesc);
		System.out.println("table create ok!!!");
	}
	public static void insertData() throws IOException {
		HTable table = new HTable(config, tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(64*1024*1024);
		//Put put = new Put();
		BufferedReader br = new BufferedReader(new FileReader("/tmp/userid"));
		String tmp = null;
		List<Integer> l = new ArrayList<Integer>(100);
		int count = 0;
		while(null != (tmp = br.readLine())) {
			l.add(Integer.parseInt(tmp));
			if (++ count == 100) {
				break;
			}
		}
		br.close();
		
		int length = l.size();
		Random rand = new Random();
		for (Integer i : l) {
			int joinCount = Math.abs(rand.nextInt() % 100) + 1;
			int start = Math.abs(rand.nextInt() %length);
			
			//Set<Integer> s = new HashSet<Integer>(joinCount);
			//Put p = new Put(Bytes.toBytes(String.valueOf(i)));
			
			for (int j = 0; j < joinCount; j++) {
				int t = l.get((start+ j)%length);
				if (t != i) {
					System.out.println(t);
					Put p = new Put(Bytes.toBytes(String.valueOf(i)));
					p.setWriteToWAL(false);
					p.add(Bytes.toBytes("followId"), Bytes.toBytes(String.valueOf(t)), Bytes.toBytes(""));
					table.put(p);
				}
			}
			
		}
		table.flushCommits();
	}
		
	public static void main(String [] args) throws IOException {
		createTable();
		insertData();
		
	}
}
