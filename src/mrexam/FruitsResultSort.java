package mrexam;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FruitsResultSort {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
//		conf.set("fs.default.name", "hdfs://192.168.111.120:9000");
		FileSystem hdfs = FileSystem.get(conf);
		// 바나나 1, 바나나 1 같이 각각 떨어져 있는 것을 묶는다.
		// Value 기준으로 sort를 한다.
		// 키값으로 sort는 자동적으로 되지만 value를 기준으로 sort가 없어서 밑에 코드가 존재한다.
		FSDataInputStream in = hdfs.open(new Path("/result/fruit1/part-r-00000"));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// 키를 가지고 sort는 API를 사용하면 된다.
		Map<String, Integer> map = new HashMap<String, Integer>();

		while (br.ready()) {
			String line = br.readLine();
			String data[] = line.split("\\s+");
			map.put(data[0], Integer.parseInt(data[1]));
		}


		List<String> keySetList = new ArrayList<>(map.keySet());

		// 오름차순 System.out.println("------value 오름차순------");
		//비교하고자 하는 value를 가지고 정렬한다.
		// 람다 식을 사용하였다.
		// (o1, o2) -> (map.get(o1).compareTo(map.get(o2)) 람다식을 사용하여 정렬했다.
		// 전달해야될 객체가 인터페이스가 생성해야 될때 , 인터페이스가 오버라이딩 해야될 함수가 1개일때 , 이름 생략하고 바로 오버라이딩 함수를 호출한다.
		// o1과 o2의 value를 비교해서 값을 넣는다.
		// 자바의 annomynous inner클래스를 사용한다.
		Collections.sort(keySetList, (o1, o2) -> (map.get(o1).compareTo(map.get(o2))));

		for (String key : keySetList) {
			System.out.println("key : " + key + " / " + "value : " + map.get(key));
		}

		System.out.println();

		// 내림차순 System.out.println("------value 내림차순------");
		Collections.sort(keySetList, (o1, o2) -> (map.get(o2).compareTo(map.get(o1))));
		for (String key : keySetList) {
			System.out.println("key : " + key + " / " + "value : " + map.get(key));
		}

	}

}
