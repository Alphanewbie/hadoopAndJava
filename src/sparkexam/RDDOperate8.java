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
		// Rdd��ü�� 3�� �����.
		List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 1), new Tuple2<>("b", 1));

		// Ű ������ ��Ƽ� ������� �����Ѵ�.
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);

		// Java7
		// ���ེ ����Ű�� ���� ī���� �Ҷ� ���δ�.
		// Ű�� value�� ��� ����Ѵ�.
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
		
		// ù��° Ű�� �ι�° Ű�� ������. ��, ���� ��޵� �ܾ�� Ű�� ������.
		result2.foreach(tupledata ->
			System.out.println(tupledata_1 + "===" + tupledata_2));
	}

}