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
		

		// 메소드 이름만 받는다.
											// sortBy f의 값의 매개변수로 받아서 그걸 그대로 리턴하겠다는 의미
											// 즉, 가공을 전혀 하지 않고 그냥 내보낸다.
		JavaRDD<Integer> rddSort = rdd.sortBy(f->f, false, 1);
							// 앞에서부터 10개를 출력한다.

		System.out.println(rddSort.take(10));
		
		Integer result1 = rdd.first();
		System.out.println(result1);
		
		List<Integer> result2 = rdd.take(5);
		System.out.println(result2);	
		
		sc.stop();
	}


	// ArrayList 객체를 새로 만들어서 리턴해 주는 객체
	public static ArrayList<Integer> fillToN(int n) {
		ArrayList<Integer> rst = new ArrayList<>();
		// 그걸로 RDD 객체를 만든다.

		for (int i = 0; i < n; i++)
			rst.add(i);
		return rst;
	}
	
}