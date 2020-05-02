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

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream in = hdfs.open(new Path("/result/fruit1/part-r-00000"));
		
		BufferedReader br =new BufferedReader(new InputStreamReader(in));
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		while(br.ready()){ 	  	
			String line = br.readLine();
			String data[] = line.split("\\s+");
			map.put(data[0], Integer.parseInt(data[1]));
        }	
		

		
		  List<String> keySetList = new ArrayList<>(map.keySet());
		  
		  // 오름차순 System.out.println("------value 오름차순------");
		  Collections.sort(keySetList, (o1, o2) ->
		  (map.get(o1).compareTo(map.get(o2))));
		  
		  for (String key : keySetList) { System.out.println("key : " + key + " / " +
		  "value : " + map.get(key)); }
		  
		  System.out.println();
		  
		  // 내림차순 System.out.println("------value 내림차순------");
		  Collections.sort(keySetList, (o1, o2) ->
		  (map.get(o2).compareTo(map.get(o1)))); for (String key : keySetList) {
		  System.out.println("key : " + key + " / " + "value : " + map.get(key)); }
		 
	}

}
