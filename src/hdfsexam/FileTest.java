package hdfsexam;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.rmi.Remote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class FileTest {

	private static final String srcDir = "/edudata/";

	public static void main(String[] args) throws Exception {
		String fileName = "president_moon.txt";
		Path path = new Path(srcDir + fileName);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
		FileSystem fs = FileSystem.get(URI.create(srcDir + fileName), conf);

		if (fs.exists(path)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

			// 당연히 쓰면 안되는 형식이다. 왜나면 커질수록 오래 걸리는데
			// 하둡은 행 단위로 읽으면 엄청 걸린다.
			while (br.ready()) {
				String line = br.readLine();
				System.out.println(line);
			}

			// 파일 스테이터스 객체를 리턴 받아서 저장한다.
			FileStatus fStatus = fs.getFileStatus(path);
			if (fStatus.isFile()) {
				System.out.println("");
				System.out.println("===========================================");
				System.out.println("File Block Size : " + fStatus.getBlockSize());
				System.out.println("Group of File   : " + fStatus.getGroup());
				System.out.println("Owner of File   : " + fStatus.getOwner());
				System.out.println("File Length     : " + fStatus.getLen());
			} else {
				System.out.println("파일이 아닙니다.");
			}
			// "/edudata/"에 대핸 패스를 생성한다.
			path = new Path(srcDir);
			// RemoteIterator<LocatedFileStatus>에 들어가 있는 데이터를 recursive해서 가져와라
			// 만약 next가 가능한 동안 계쏙 가져와라.
			RemoteIterator<LocatedFileStatus> list = fs.listFiles(path, false);
			while (list.hasNext()) {
				LocatedFileStatus is = list.next();
				Path p = is.getPath();
				System.out.println(p.getName());
			}
		} else {
			System.out.println("파일을 찾을 수 없습니다.");
		}
	}

}
