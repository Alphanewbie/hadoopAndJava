package sparkexam;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDOperate4 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test4");
		List<Integer> data = fillToN(100);
		JavaRDD<Integer> rdd = sc.parallelize(data);
		

		// �޼ҵ� �̸��� �޴´�.
											// sortBy f�� ���� �Ű������� �޾Ƽ� �װ� �״�� �����ϰڴٴ� �ǹ�
											// ��, ������ ���� ���� �ʰ� �׳� ��������.
		JavaRDD<Integer> rddSort = rdd.sortBy(f->f, false, 1);
							// �տ������� 10���� ����Ѵ�.

		System.out.println(rddSort.take(10));
		
		Integer result1 = rdd.first();
		System.out.println(result1);
		
		List<Integer> result2 = rdd.take(5);
		System.out.println(result2);	
		
		sc.stop();
	}


	// ArrayList ��ü�� ���� ���� ������ �ִ� ��ü
	public static ArrayList<Integer> fillToN(int n) {
		ArrayList<Integer> rst = new ArrayList<>();
		// �װɷ� RDD ��ü�� �����.

		for (int i = 0; i < n; i++)
			rst.add(i);
		return rst;
	}
	
}