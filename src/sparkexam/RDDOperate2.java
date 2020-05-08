package sparkexam;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RDDOperate2 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test2");
		doMap(sc);
		sc.stop();
	}

	public static void doMap(JavaSparkContext sc) {
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

		// Java7
		JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 + 10;
			}
		});

		// Java8 Lambda
		JavaRDD<Integer> rdd3 = rdd1.map(v1 -> v1 + 10);

		System.out.println("java 7 : " + rdd2.collect());
		System.out.println("java 8 : " + rdd3.collect());
	}	
	
}