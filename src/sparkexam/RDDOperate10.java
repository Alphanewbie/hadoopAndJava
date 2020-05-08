package sparkexam;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDOperate10 {
	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test10");

		doSortByKey(sc);

		sc.stop();
	}

	public static void doSortByKey(JavaSparkContext sc) {
		// value 기준으로 오름차순으로 정렬힌디.
		List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<>("q",3), new Tuple2<>("z",8), new Tuple2<>("a", 2));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);
		JavaPairRDD<String, Integer> result = rdd.sortByKey();
		System.out.println(result.collect());
		
		// 정렬을 위래 키와 밸루의 위치를 바꾼다. swap이 키와 밸류를 바꾼다.
		JavaPairRDD<Integer, String> rdd1 = rdd.mapToPair(x -> x.swap());
		JavaPairRDD<Integer, String> rdd2 = rdd1.sortByKey(false);
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(x -> x.swap());
		
		rdd3.foreach(item -> System.out.println(item));		
	}
}