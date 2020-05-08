package sparkexam;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RDDOperate13 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test13");

		doGroupBy(sc);

		sc.stop();
	}

	public static void doGroupBy(JavaSparkContext sc) {
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

		// Java7
		JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(new Function<Integer, String>() {
			@Override
			public String call(Integer v1) throws Exception {
				return (v1 % 2 == 0) ? "even" : "odd";
			}
		});

		// Java8 Lambda
		JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd1.groupBy((Integer v1) -> (v1 % 2 == 0) ? "even" : "odd");

		System.out.println(rdd2.collect());
		System.out.println(rdd3.collect());
	}
}