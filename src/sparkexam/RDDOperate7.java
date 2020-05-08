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

	// ���ེ ���
	public static void doReduce(JavaSparkContext sc) {
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		// �� Rdd�� 3���� ��Ƽ������ ���������ٴ� �ǹ�
		JavaRDD<Integer> rdd = sc.parallelize(data, 3);
		// Java7
		// ù��° �ι�° ���׸��� call�� �Ű����� ����°�� ���� ���� ���׸�
		int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println(result);
		// Java8
		// ���ེ�� ���� ���� ���. 1�� 2�� ���޹޾� 3�� �����Ѵ�. ���ེ�� �̹� �ִ� �ͱ��� �����ϹǷ� 1~15���� ���ϴ°�
		// �� ���ེ�� �׼� ��ü�̴�.
		int result2 = rdd.reduce((v1, v2) -> v1 + v2);
		System.out.println(result2);
	}

	
}