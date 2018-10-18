package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseUtils {
	//会使用config下面的hbase-site.xml
	public Configuration configuration;
	public Connection connection;
	public Admin admin;
	public HbaseUtils() throws IOException {
		configuration=HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(configuration);
		admin = connection.getAdmin();
	}
	/**
	 * 通过表名，列族名称创建新表
	 * @param tableName
	 * @param family
	 * @throws IOException
	 */
	public void createTable(String tableName,String []family) throws IOException {
		TableName tname = TableName.valueOf(tableName);
		System.out.println(tableName);
		if(admin.tableExists(tname)) {
			System.err.println(tableName+"表已存在！！！");
		}else {
			HTableDescriptor desc = new HTableDescriptor(tname);
			for(int i=0;i<family.length;i++) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family[i]);
				desc.addFamily(hColumnDescriptor);
			}
			admin.createTable(desc);
			System.out.println(tableName+"创建成功！");
		}
	}
	public void deleteTable(String tableName) throws IOException {
		TableName tName = TableName.valueOf(tableName); 
		if(admin.tableExists(tName)) {
			admin.disableTable(tName);
			admin.deleteTable(tName);
			System.err.println(tableName+":删除成功！");
		}else {
			System.out.println(tableName+":不存在！");
		}
	}
	/**
	 * 列出所有的表名和列簇名
	 * @throws IOException
	 */
	public void listAllTable() throws IOException {
		TableName []tables = admin.listTableNames();
		for(TableName tab:tables) {
			System.out.println("表名：==》"+tab.getNameAsString());
			HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tab);
			HColumnDescriptor []hColumnDescriptor = hTableDescriptor.getColumnFamilies();
			for(HColumnDescriptor hcol:hColumnDescriptor) {
				System.out.println(hcol.getNameAsString()+"==="+hcol.toString());
			}
		}
	}
	public void getRecordByRowKey(String tableName,String rowkey) throws IOException {
		TableName tName = TableName.valueOf(tableName);
		Table table = connection.getTable(tName);
		Get get = new Get(rowkey.getBytes());
		Result result = table.get(get);
		for(Cell cell:result.listCells()) {
			 System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
	            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
	            System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
	            System.out.println("Timestamp:" + cell.getTimestamp());
	            System.out.println("-------------------------------------------");
		}
	}
	/**
	 * 通过表名获取所有列信息
	 * @param tableName
	 * @throws IOException
	 */
	public void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("row:" + Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()));
                    System.out.println("family:"
                            + Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getValueLength()));
                    System.out.println("qualifier:"
                            + Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
                    System.out
                            .println("value:" + Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                    System.out.println("timestamp:" + cell.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }
	/**
	 * 通过表名，起始和结束行号进行查询
	 * @param tableName
	 * @param startRowKey
	 * @param endRowKey
	 * @throws IOException 
	 */
	public void getResultScanner(String tableName,String startRowKey,String endRowKey) throws IOException {
		Scan scan  = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowKey));
		scan.setStopRow(Bytes.toBytes(endRowKey));
		ResultScanner resultScanner  = null;
		Table table = connection.getTable(TableName.valueOf(tableName));
		resultScanner = table.getScanner(scan);
		for(Result rs:resultScanner) {
			for(Cell cell:rs.listCells()) {
				System.out.println("row:" + Bytes.toString(cell.getRowArray(),
						cell.getRowOffset(),cell.getRowLength()));
	            System.out.println("family:" + Bytes.toString(cell.getFamilyArray(),
	                    		cell.getFamilyOffset(),cell.getValueLength()));
	            System.out.println("qualifier:"+ Bytes.toString(cell.getQualifierArray(),
	                    		cell.getQualifierOffset(),cell.getQualifierLength()));
	            System.out.println("value:" + Bytes.toString(cell.getValueArray(),
	                    		cell.getValueOffset(),cell.getValueLength()));
	            System.out.println("timestamp:" + cell.getTimestamp());
	            System.out.println("-------------------------------------------");
			}
		}
		
	}
	/**
	 * 向已有的表中添加数据
	 * @param rowkey
	 * @param tableName
	 * @param columns1
	 * @param value1
	 * @param columns2
	 * @param value2
	 * @throws IOException
	 */
	public void addData(String rowkey,String tableName,String[]columns1,String[]value1,String[]columns2,String[]value2) throws IOException {
		Put put = new Put(Bytes.toBytes(rowkey));
		Table table = connection.getTable(TableName.valueOf(tableName));
		for(int i=0;i<columns1.length;i++) {
			put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes(columns1[i]) ,Bytes.toBytes(value1[i]));
		}
		for(int i=0;i<columns2.length;i++) {
			put.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes(columns2[i]) ,Bytes.toBytes(value2[i]));
		}
		table.put(put);
		for(int i=0;i<columns1.length;i++) {
			System.out.print(columns1[i]+'\t');
		}
		for(int i=0;i<columns2.length;i++) {
			System.out.print(columns2[i]+'\t');
		}
		System.out.println();
		for(int i=0;i<value1.length;i++) {
			System.out.print(value1[i]+'\t');
		}
		for(int i=0;i<value2.length;i++) {
			System.out.print(value2[i]+'\t');
		}
		System.out.println("插入成功！");
		
	}
	/**
	 * 为表添加列簇
	 * @param tableName
	 * @param familyName
	 * @throws IOException
	 */
	public void addFamily(String tableName,String familyName) throws IOException {
		TableName tName = TableName.valueOf(tableName);
		admin.disableTables(tableName);
		HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tName);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
		hTableDescriptor.addFamily(hColumnDescriptor);
	    admin.modifyTable(tName, hTableDescriptor);
	    admin.enableTable(tName);
	    System.out.println(familyName+"已添加！");
	}
	/**
	 * 删除列簇
	 * @param tableName
	 * @param familyName
	 * @throws IOException
	 */
	public void delFamily(String tableName,String familyName) throws IOException {
		TableName tName = TableName.valueOf(tableName);
		admin.disableTables(tableName);
		admin.deleteColumn(tName,Bytes.toBytes(familyName));
		admin.enableTable(tName);
		System.out.println(familyName+":已删除！");
	}
	
}
