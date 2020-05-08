package sparkexam;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDOperate12 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test12");

		doJoin(sc);

		sc.stop();
	}

	public static void doJoin(JavaSparkContext sc) {
		List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 1), new Tuple2<>("c", 1),
				new Tuple2<>("d", 1), new Tuple2<>("e", 1));
		List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2<>("b", 2), new Tuple2<>("c", 2));

		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
		JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

		JavaPairRDD<String, Tuple2<Integer, Integer>> result = rdd1.<Integer>join(rdd2);
		System.out.println(result.collect());

	}

}