package hdfsexam;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileWrite {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
			FileSystem hdfs = FileSystem.get(conf);
			String fname = "product_click.log";
			File f = new File("c:/unico/" + fname);
			if (!f.exists()) {
				System.out.println("파일이 없음!!");
				return;
			}
			Path path = new Path("/edudata/" + fname);
			if (hdfs.exists(path)) {
				hdfs.delete(path, true);
			}
			long size = f.length();
			FileReader fr = new FileReader(f);
			char[] content = new char[(int) size];
			fr.read(content);
			FSDataOutputStream outStream = hdfs.create(path);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outStream));
			writer.write(content);
			writer.close();
			outStream.close();
			fr.close();
			System.out.println(size + " 크기의 데이터 출력 완료!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
