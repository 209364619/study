package hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class HelloHDFS {
	public static void main(String[] args) throws IllegalArgumentException, IOException, Exception {
		HdfsUtils hdfs = new HdfsUtils();
		// System.out.println(hdfs.createDir("/hello"));
		// hdfs.removeFileInRemote("/hello");
		hdfs.GetAllDir(new Path("/"));
	}
}
