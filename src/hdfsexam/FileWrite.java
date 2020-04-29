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
			// 이 2줄운 하둡 환경 설정 같은 것이다.
			// 즉, 하둡이 꺼져 있다면 여기에서 에러가 뜬다.
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
			
			FileSystem hdfs = FileSystem.get(conf);
			String fname = "dessert-menu.csv";
			File f = new File("C:/testcontent/" + fname);
			if (!f.exists()) {
				System.out.println("파일이 없음!!");
				return;
			}
			Path path = new Path("/edudata/" + fname);
			// 만약 이미 존재하고 있다면 삭제한다.
			if (hdfs.exists(path)) {
				hdfs.delete(path, true);
			}
			// 파일의 길이 읽어옴
			long size = f.length();
			// 이 객체를 생성한 이유는 file객체의 크기를 알아내기 위해서
			FileReader fr = new FileReader(f);
			char[] content = new char[(int) size];
			fr.read(content);
			// outStream 객체를을 새로 생성하겠다. 
			FSDataOutputStream outStream = hdfs.create(path);
			// bufferedwrite 객체로 집어 넣는다.
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
