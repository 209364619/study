package hdfs;



import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;

import com.sun.jersey.server.impl.uri.PathTemplate;

import io.netty.channel.local.LocalAddress;

public class HdfsUtils {
	public Configuration configuration = new Configuration();
	public FileSystem fs;
	/**
	 * 无参构造函数，初始化hdfs配置，获取文件系统管理器
	 */
	public HdfsUtils() {
		configuration.set("fs.defaultFS","hdfs://192.168.1.231:8020");
		try {
			fs = FileSystem.get(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 将本地文件上传到hdfs上
	 * @param local_path
	 * @param remote_path
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public boolean UploadToRemoteServer(String local_path,String remote_path) throws IllegalArgumentException, IOException {
		fs.copyFromLocalFile(new Path(local_path), new Path(remote_path));
		return true;
	}
	/**
	 * 下载远端文件
	 * @param loacal_path
	 * @param remote_path
	 * @return
	 */
	public boolean download(String loacal_path,String remote_path) {
		try {
			fs.copyToLocalFile(new Path(remote_path), new Path(loacal_path));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	/**
	 * 删除远端文件
	 * @param remote_path
	 * @return
	 */
	public boolean removeFileInRemote(String remote_path) {
		Path remote_file_path = new Path(remote_path);
		try {
			if(fs.exists(remote_file_path)) {
				fs.delete(new Path(remote_path));
			}else {
				System.out.println("不存在文件："+remote_file_path.toString());
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
			
		}
		return true;
	}
	/**
	 * 在远端服务器创建目录
	 * @param dir_name
	 * @return
	 */
	public String createDir(String dir_name) {
		Path dir_path = new Path(dir_name);
		try {
			if(fs.exists(dir_path)) {
				System.out.println("目标目录已存在！");
				return "目标目录已存在！";
			}else {
				fs.mkdirs(dir_path);
				return dir_name+" create success!";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "error";
		}
	}
	/**
	 * 重命名
	 * @param oldName
	 * @param newname
	 * @return
	 */
	public boolean rename_dir(String oldName,String newname) {
		Path old_path = new Path(oldName);
		Path new_path = new Path(newname);
		try {
			if(fs.exists(old_path)) {
				if(fs.exists(new_path)) {
					System.out.println("新目录已存在,请修改新文件名。");
					return false;
				}else {
					fs.rename(old_path, new_path);
					return true;
				}
			}else {
				System.out.println("原始目录不存在！");
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	/**
	 * 读取hdfs目录下的文件
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public FileStatus[] list(String path) throws FileNotFoundException, IOException {
		Path dir_path = new Path(path);
		FileStatus[] files = fs.listStatus(dir_path);
		for(FileStatus file:files) {
			System.out.println(file.getPath().toString());
		}
		return files;
	}
	/**
	 * 递归列出path下面所有的文件夹和文件
	 * @param path
	 * @throws IOException
	 * @throws Exception
	 */
	public void GetAllDir(Path path) throws IOException, Exception {
		if(fs.isDirectory(path)) {
			System.out.println("Directory:"+path.toString());
			FileStatus[]son_files = fs.listStatus(path);
			for(FileStatus file:son_files) {
				//System.out.println("for:"+file.getPath().toString());
				Path file_path = file.getPath();
				GetAllDir(file_path);
			}
		}else {
			System.out.println(path.toString());
		}
	}
}