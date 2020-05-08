package sparkexam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDOperate5 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test5");
		
		// Map�� FlatMap�� ���غ���� ����
		doMapAndFlatMap(sc);

		sc.stop();
	}

	public static void doMapAndFlatMap(JavaSparkContext sc) {
		List<String> data = new ArrayList<>();
		data.add("apple,orange");
		data.add("grape,apple,mango");
		data.add("blueberry,tomato,orange");
		JavaRDD<String> rdd1 = sc.parallelize(data);

		// ���� ȣ�� �Ǵ� ������ 3��
		JavaRDD<String[]> rdd2 = rdd1.map((String t) -> t.split(","));
		
		
		// �÷� ���� ȣ��Ǵ� ������ 3��
														// ��Ʈ�� �迭���� �����Ǿ� �ִ� ���� ��Ʈ���� �ٲٰڴ�.
		JavaRDD<String> rdd3 = rdd1.flatMap((String t) -> Arrays.asList(t.split(",")).iterator());
		
		// ���� element�� ���ο� RDD�� �����. �� 1�� element�� ��Ʈ�� 2��°�� ��Ʈ�� 3��°�� ��Ʈ�� ��, ��Ʈ�� �迭
		// �÷����� �������� �ͼ� �����. �׷��� �÷����� ���� ���⹰�� string ��Ʈ�� �迭�� �ƴϴ�.
		
		// ��, �÷����� ������ �����.
		
		System.out.println(rdd2.collect());
		System.out.println(rdd3.collect());
		
		JavaRDD<Integer> rdd4 = rdd1.map((String t) -> t.length());
		System.out.println(rdd4.collect());
		
	}
	
}