package sparkexam;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount7 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("UNICO").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		JavaRDD<String> rdd1 = sc.textFile("hdfs://192.168.111.120:9000/edudata/fruits.txt");

		JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String v) throws Exception {
				return Arrays.asList(v.split("[\\s]+")).iterator();
			}
		});

		JavaPairRDD<String, Long> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Long>() {
			@Override
			public Tuple2<String, Long> call(String s) throws Exception {
				return new Tuple2<String, Long>(s, 1L);
			}
		});

		JavaPairRDD<String, Long> rdd4 = rdd3.reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		
		System.out.println(rdd4.collect());
		rdd4.saveAsTextFile("hdfs://192.168.111.120:9000/result/sparkoutput10");
		sc.close();
		sc.stop();

	}

}
