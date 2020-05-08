package sparkexam;


import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkLab3 {
	
	public static void main(String[] args) throws Exception
	{
		JavaSparkContext sc = SparkUtils.getSparkContext("sparklab3");
		doRequest(sc);
		sc.stop();
		
		
	}

	private static void doRequest(JavaSparkContext sc) {
		//[03/Jan/2020:09:26:17 +0900]
		JavaRDD<String> rdd1 = sc.textFile("hdfs://192.168.111.120:9000/edudata/tomcat_access_log.txt");
		JavaRDD<String> rdd2 = rdd1.flatMap(line -> Arrays.asList(line.split(" ")[3]).iterator());
		JavaRDD<String> rdd3 = rdd2.flatMap(line -> Arrays.asList(line.split(":")[1]).iterator());
		JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(word -> new Tuple2<String, Integer>(word,1));
		JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey((a, b)->a + b);
		JavaPairRDD<Integer, String> rdd6 = rdd5.mapToPair(x -> x.swap());
		JavaPairRDD<Integer, String> rdd7 = rdd6.sortByKey(false);
		JavaPairRDD<String, Integer> rdd8 = rdd7.mapToPair(x -> x.swap());

		System.out.println("제일 많이 요청된 시간은 "+rdd8.first()._1+" 시간대입니다.");
	}

}

