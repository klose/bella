import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
/**
 * Calculate the relations among the two users.
 * 
 * @author jiangbing
 *
 */

public class BiRelationsMapReduce {
	private final static String tableName = "following";
	private final static String relationsName = "bi_relations";
	public static class HBaseMap
			extends
			TableMapper <Text, IntWritable>{
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result,
				Context context) throws IOException, InterruptedException {
			NavigableMap<byte[], byte[]> map =  result.getNoVersionMap().get(Bytes.toBytes("followId"));
			Iterator<byte[]> iter = map.keySet().iterator();
			
			int key = Integer.parseInt(Bytes.toString(rowKey.get()));
			while (iter.hasNext()) {				
				int followId  =  Integer.parseInt(Bytes.toString(iter.next()));
				int value = 0;
				StringBuilder sb = new StringBuilder();
				if (key < followId) {
					sb.append(key);
					sb.append(":");
					sb.append(followId);
					value = 1;
				}
				else {
					sb.append(followId);
					sb.append(":");
					sb.append(key);
					value = -1;
				}
				context.write(new Text(sb.toString()), new IntWritable(value));
			}
		}		
	}
	public static class HBaseReduce extends
			TableReducer<Text, IntWritable, ImmutableBytesWritable> {	
		private static Configuration config = HBaseConfiguration.create();
		private static HTable table ;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			HBaseAdmin admin = new HBaseAdmin(config);
			table = new HTable(config, tableName);
			table.setAutoFlush(false);
			table.setWriteBufferSize(64*1024*1024);
			
			if (admin.tableExists(tableName)) {
				// add a new family to handle the relationship
				HTableDescriptor htd = admin.getTableDescriptor(Bytes
						.toBytes(tableName));
				for (HColumnDescriptor tmp : htd.getColumnFamilies()) {
					if (tmp.getNameAsString().equals(relationsName)) {
						return;
					}
				}
			}
			admin.disableTable(tableName);
			HColumnDescriptor relationship = new HColumnDescriptor(
					relationsName);
			relationship.setMaxVersions(Integer.MAX_VALUE);
			relationship.setBloomFilterType(StoreFile.BloomType.ROW);
			admin.addColumn(tableName, relationship);
			admin.enableTable(tableName);
		}
 			

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			HTable table = new HTable(config, tableName);
			table.setAutoFlush(false);
			table.setWriteBufferSize(64*1024*1024);
			String[] tmp = key.toString().split(":");
			int sum = 0;
			Iterator<IntWritable> iter = value.iterator();
			while (iter.hasNext()) {
				int t = iter.next().get();
				sum  += t;
			}
			ImmutableBytesWritable k0 = new ImmutableBytesWritable(tmp[0].getBytes());
			Put p0 = new Put(Bytes.toBytes(tmp[0]));
			p0.add(Bytes.toBytes(relationsName), Bytes.toBytes(tmp[1]), Bytes.toBytes(String.valueOf(sum)));
			context.write(k0, p0);
			ImmutableBytesWritable k1 = new ImmutableBytesWritable(tmp[1].getBytes());
			Put p1 = new Put(Bytes.toBytes(tmp[1]));
			p1.add(Bytes.toBytes(relationsName), Bytes.toBytes(tmp[0]), Bytes.toBytes(String.valueOf(sum)));
			context.write(k1, p1);
		}
				
	}
	public static void main(String [] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		Job job = new Job(conf, "BiRelationsMapReduce");	 
		job.setJarByClass(BiRelationsMapReduce.class);
		Scan scan = new Scan(); 
		scan.setMaxVersions(1);
		scan.addFamily(Bytes.toBytes("followId"));
		TableMapReduceUtil.initTableMapperJob(tableName, scan,BiRelationsMapReduce.HBaseMap.class,
		 Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName,BiRelationsMapReduce.HBaseReduce.class, job);	    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

