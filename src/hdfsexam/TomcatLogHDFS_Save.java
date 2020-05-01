package hdfsexam;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TomcatLogHDFS_Save {
	public static void main(String[] args) {
		try {
			// 이 2줄운 하둡 환경 설정 같은 것이다.
			// 즉, 하둡이 꺼져 있다면 여기에서 에러가 뜬다.
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
			
			FileSystem hdfs = FileSystem.get(conf);
			
			File path = new File("C:/iotest/logs");
			File[] fileList = path.listFiles();
			Path hdfspath = new Path("/edudata/tomcat_access_log.txt");
			FSDataOutputStream out = hdfs.create(hdfspath);
			if(fileList.length > 0){
			    for(int i=0; i < fileList.length; i++){
			    	InputStream in = new BufferedInputStream(new FileInputStream(fileList[i]));
			    	
			    	byte[] b = new byte[1024];
			        int numBytes = 0;
			        
			        while ((numBytes = in.read(b)) > 0) {
			          out.write(b, 0, numBytes);
			        }
			        in.close();
			    }
			}
			out.close();
		    FileStatus fStatus =hdfs.getFileStatus(hdfspath);
		    
		    if (fStatus.isFile()) {
		    	System.out.println( fStatus.getBlockSize());
		    	System.out.println( fStatus.getLen());
		    }
		    hdfs.close();

		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
