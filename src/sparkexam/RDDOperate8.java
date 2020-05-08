package sparkexam;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class RDDOperate8 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test8");
		doReduceByKey(sc);
		sc.stop();
	}

	public static void doReduceByKey(JavaSparkContext sc) {
		// Rdd객체를 3개 만든다.
		List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 1), new Tuple2<>("b", 1));

		// 키 단위로 모아서 결과값을 구성한다.
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);

		// Java7
		// 리듀스 바이키는 워드 카운팅 할때 쓰인다.
		// 키와 value로 묶어서 사용한다.
		JavaPairRDD<String, Integer> result = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println(result.collect());
		// Java8 Lambda
		JavaPairRDD<String, Integer> result2 = rdd.reduceByKey((v1,  v2) -> v1 + v2);
		System.out.println(result2.collect());
		
		// 첫번째 키와 두번째 키를 꺼낸다. 즉, 가장 언급된 단어와 키를 꺼낸다.
		result2.foreach(tupledata ->
			System.out.println(tupledata_1 + "===" + tupledata_2));
	}

}