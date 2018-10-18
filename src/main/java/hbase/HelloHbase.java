package hbase;

import java.io.IOException;

public class HelloHbase {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		HbaseUtils hbaseUtils = new HbaseUtils();
//		HbaseTest hbaseTest = new HbaseTest();
//		String[] family = {"name","age"};
//		hbaseUtils.createTable("users", family);
//		hbaseUtils.listAllTable();
//		hbaseUtils.getRecordByRowKey("hbase_1102", "001");
//		hbaseUtils.getResultScanner("hbase_1102", "0", "100");
//		String[]columns1 = {"name","gender"};
//		String[]value1= {"xiaohong","man"};
//		String[]columns2 = {"chinese","math"};
//		String []value2 = {"59","59"};
//		hbaseUtils.addData("004", "hbase_1102", columns1, value1, columns2, value2);
//		hbaseUtils.getRecordByRowKey("hbase_1102", "004");
//		hbaseUtils.addFamily("hbase_test1", "insert_test");
		hbaseUtils.delFamily("hbase_test1", "age");
	}

}
