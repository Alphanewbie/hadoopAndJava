package hdfsexam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class FileSystemCat {
   public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	
	// ip를 준 이유. 클라이언트트는 마스터가 누구인지 모르기 때문.
	conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
	
	// 하둡 파일 시스템에 대한 객체를 리턴한다.
	FileSystem hdfs = FileSystem.get(conf);
	FSDataInputStream in = null;
	try {
		
		// 업데이트는 지원하지 않는다. 데이터가 많아서.
		// 빅데이터는 업데이트를 지원하지 않는다.
		// 최근에 append까지는 허용됨.
		// 빅데이터는 집계된 데이터에 대한 통계만 내는 역활.
	    in = hdfs.open(new Path("/edudata/president_moon.txt"));
	    IOUtils.copyBytes(in, System.out, 4096, false);
	} finally {
   	    IOUtils.closeStream(in);
	}
    }
}
