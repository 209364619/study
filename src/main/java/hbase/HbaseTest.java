package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import com.google.common.collect.Table;

public class HbaseTest {
	public Connection connection;
	public static Configuration configuration = HBaseConfiguration.create();
	public Table table;
	public Admin admin;
	public HbaseTest() throws IOException {
		//configuration.addResource("hbase-site.xml");
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
//	    configuration.addResource("core-site.xml");
//		configuration.set("hbase.zookeeper.quorum", "192.168.1.231:2181,192.168.1.232:2181,192.168.1.233:2181");
		connection = ConnectionFactory.createConnection(configuration);
		//configuration.addResource("hbase-site.xml");
		admin = connection.getAdmin();
	}
	
	public void createTable(String tableName,String... cf1) throws Exception {
		System.out.println("input tableName is:"+tableName);
		TableName tname = TableName.valueOf(tableName);
		HTableDescriptor tableDescriptor = new HTableDescriptor(tname);
		//判断表是否存在
		if(admin.tableExists(tname)) {
			System.out.println("表"+tableName+"已经存在");
			return ;
		}
		//添加表列簇信息
		for(String cf:cf1) {
			HColumnDescriptor famliy = new HColumnDescriptor(cf);
			tableDescriptor.addFamily(famliy);
		}
		//调用admin CreateTable方法创建表
		admin.createTable(tableDescriptor);
		System.out.println(tableName+"创建成功！");
	}
	
}
