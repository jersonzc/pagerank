import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Phase1 extends Configured implements Tool {
	/*
	Input:

	A:D
	B:C
	C:A
	D:B
	C:D

	Output:

	(A, D)
	(B, C)
	(C, A)
	(D, B)
	(C, D)
	*/
	public static class Phase1Mapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}


	/*
	Input:

	(A, D)
	(B, C)
	(C, A)
	(D, B)
	(C, D)

	Output:

	(A, (1.0:D))
	(B, (1.0:C))
	(C, (1.0:A,D))
	(D, (1.0:B))

	*/
	public static class Phase1Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		/*
			Example:

			(C, A)
			(C, D)

			values: A,D
		*/
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String pageRank = "1.0:"; // Initialization of PageRank (PR)
			boolean first = true;
			while (values.hasNext()) {
				if (!first) {
					pageRank += ","; // "1.0:A,"
				}
				pageRank += values.next().toString(); // "1.0:A"   "1.0:A,D"
				first = false;
			}
			output.collect(key, new Text(pageRank)); // (C, "1.0:A,D")
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, Phase1.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("PageRankPhase1");
		job.setMapperClass(Phase1Mapper.class);
		job.setReducerClass(Phase1Reducer.class);

		job.setInputFormat(KeyValueTextInputFormat.class);
		job.set("key.value.separator.in.input.line", ":");

		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Phase1(), args);
		System.exit(res);
	}
}
