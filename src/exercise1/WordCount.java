package exercise1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://192.168.111.120:9000");
		conf.set("fs.defaultFS", "hdfs://192.168.111.120:9000");

		// 맵 리듀스를 요청하는 것을 job이라고 한다.
		Job job = Job.getInstance(conf, "WordCount");

		// 맵, 리듀스를 요청하는 것을 드라이버 클래스라고 함. 드라이버 클래스의 클래스 객체를 준다.
		// 클래스 이름에 .class를 붙이면 그 이름의 클래스 객체가 된다.
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 입력 되는 것과 출력 되는 것의 타입을 지정한다. 즉, 둘 text타입으로 받는다./
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 출력되는 키는 text타입이고, 값도 IntWritable타입이다.
		// string이나 int가 아닌 이유, 다른 머신의 아규먼트를 리턴 받은 remote프로시저 콜이기 때문에
		// 객체 직렬화를 해야 하는데, 자바가 내장하고 있는 객체 직렬화(시리얼 통신)는 속도가 느리다. 시리얼라이즈를 자체 내장하고 있다.
		// 느린 자바 직렬화 대신 만든게 이것들이다.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 어떤 파일을 읽어서 맵 리듀스를 설정할 것인가.
		// 읽고자하는 파일의 패스 정보를 준다.
		// 해당 위치에 디렉토리가 없으면 생성한다. 오히려 있으면 에러난다.
		// 중요한건 디렉토리까지만 적는다. 파일 이름은 알아서 생성한다.
		FileInputFormat.addInputPath(job, new Path("/edudata/president_moon.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/result/exerout1"));

		//리소스 매니저가 잡을 받은 다음에 처리한다.
		// 일단 슬레이브부터 찾아본다.
		// 하나에 애플리케이션 마스터를 기동 시킨다면,
		job.waitForCompletion(true);
		System.out.println("처리가 완료되었습니다.");
	}
}