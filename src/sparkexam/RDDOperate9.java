package sparkexam;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDOperate9 {

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkUtils.getSparkContext("test9");

		doMap(sc);

		sc.stop();
	}

	public static void doMap(JavaSparkContext sc) {
		
		JavaRDD<String> rdd1 = sc.textFile("hdfs://192.168.111.120:9000/edudata/fruits.txt");
		
		JavaRDD<String> rdd2 = rdd1.flatMap((String line) -> Arrays.asList(line.split("[\\s]+")).iterator());
		
		// 단어 길이가 4개 이하인것을 전부 빼낸다.
		JavaRDD<String> rdd3 = rdd2.filter(word -> word.length() > 4);
		
		// 키가 스트링이고 value가 인테져여야한다.
		JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		
		// 이전에 수행한 결과에다가 새로운 데이터를 넣는다.만약 apple:1 appple:1이런 상황이면 전부 더한다.
		JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey((a, b) -> a + b);		
		
		// 콜렉트를 리스트 형태로 출력한다.
		System.out.println(rdd5.collect());
	}	
}