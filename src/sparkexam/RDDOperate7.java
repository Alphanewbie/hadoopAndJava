package sparkexam;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class RDDOperate7 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test7");

		doReduce(sc);

		sc.stop();
	}

	// 리듀스 기능
	public static void doReduce(JavaSparkContext sc) {
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		// 이 Rdd는 3개의 파티션으로 나눠어진다는 의미
		JavaRDD<Integer> rdd = sc.parallelize(data, 3);
		// Java7
		// 첫번째 두번째 제네릭은 call의 매개변수 세번째는 리턴 값의 제네릭
		int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println(result);
		// Java8
		// 리듀스는 집계 내는 기능. 1과 2를 전달받아 3을 리턴한다. 리듀스는 이미 있는 것까지 전달하므로 1~15까지 더하는것
		// 즉 리듀스는 액션 객체이다.
		int result2 = rdd.reduce((v1, v2) -> v1 + v2);
		System.out.println(result2);
	}

	
}