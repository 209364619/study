package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	
	public void mkdir() throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.209.110:8020/");
		//conf.addResource("core-site.xml");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/demo1/java_test");
		if(! fs.exists(path)){
		   fs.mkdirs(path);
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		new HdfsTest().mkdir();
	}
}
