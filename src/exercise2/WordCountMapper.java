package exercise2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 매퍼라는 클래스를 상속하고 있다. 제너릭스로 4가지 타입 상속
// 1과 2는 순서대로 1 매개변수 2매개변수타입  3,4는 리턴하는 타입
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// 매개변수 : 키 , 값, context -> servletcontext 처럼 이건 하둡의 맵 리듀스 프레임워크를 가르킨다.
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 받은 것을 토큰으로 쪼갠다.
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			// 하둡의 분산환경에 특화 되어 있다ㅓ.
			// 객체를 또 생성 안하고 그냥 갖다 쓴다.
			String data = itr.nextToken();
			if (3 <= data.length() && data.length() <= 5) {
				word.set(data);
				// 카운트를 1 증가 시킨다.
				context.write(word, one);
			}
		}
	}
}