package sparkexam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDOperate5 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test5");
		
		// Map과 FlatMap과 비교해보라는 예쩨
		doMapAndFlatMap(sc);

		sc.stop();
	}

	public static void doMapAndFlatMap(JavaSparkContext sc) {
		List<String> data = new ArrayList<>();
		data.add("apple,orange");
		data.add("grape,apple,mango");
		data.add("blueberry,tomato,orange");
		JavaRDD<String> rdd1 = sc.parallelize(data);

		// 맵이 호출 되는 단위는 3번
		JavaRDD<String[]> rdd2 = rdd1.map((String t) -> t.split(","));
		
		
		// 플랫 맵이 호출되는 단위도 3번
														// 스트링 배열으로 구성되어 있는 것을 스트링로 바꾸겠다.
		JavaRDD<String> rdd3 = rdd1.flatMap((String t) -> Arrays.asList(t.split(",")).iterator());
		
		// 맵은 element가 새로운 RDD를 만든다. 즉 1번 element로 스트링 2번째도 스트링 3번째도 스트링 즉, 스트링 배열
		// 플랫맵은 마지막에 와서 만든다. 그래서 플랫맵은 최종 산출물이 string 스트링 배열이 아니다.
		
		// 즉, 플랫맵은 차원을 낮춘다.
		
		System.out.println(rdd2.collect());
		System.out.println(rdd3.collect());
		
		JavaRDD<Integer> rdd4 = rdd1.map((String t) -> t.length());
		System.out.println(rdd4.collect());
		
	}
	
}