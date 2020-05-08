package sparkexam;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDOperate1 {

	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = SparkUtils.getSparkContext("test1");
		JavaRDD<String> rdd = sc.textFile("hdfs://192.168.111.120:9000/edudata/fruits.txt");
		
		System.out.println(rdd.first());
		
		System.out.println(rdd.take(3));
		
		System.out.println(rdd.collect());
		
		List<String> list = rdd.collect();
		for (String v : list) {
			System.out.println(v);
		}
		System.out.println(rdd.count());
		
		sc.stop();
	}
}