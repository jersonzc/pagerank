import java.io.IOException;
import java.util.ArrayList;
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

public class Phase2 extends Configured implements Tool {

	public static class Phase2Mapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		/*
		Input:

		(A, (1.0:D))
		(B, (1.0:C))
		(C, (1.0:A,D))
		(D, (1.0:B))

		*/
		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Example: (C, (1.0:A,D))

			// SEPARA VALUE Y NODOS
			String[] parts = value.toString().split(":"); // ["1.0", "A,D"]

			if (parts.length > 1) {
				// ALMACENA SOLO EL VALUE
				String PRStr = parts[0]; // "1.0"

				// ALMACENA SOLO NODOS EN UNA LISTA
				String nodesStr = parts[1]; // "A,D"
				String[] nodes = nodesStr.split(","); // ["A", "D"]

				// ALMACENA PAGERANK VALUE Y NÚMERO DE NODOS
				int count = nodes.length; // numero de nodos = 2
				for (int i = 0; i < nodes.length; i++) {
					String tmp = PRStr; // "1.0"
					tmp += ":"; // "1.0:"
					tmp += Integer.toString(count); // "1.0:2"
					output.collect(new Text(nodes[i]), new Text(tmp)); // ("A", "1.0:2")
				}																										// ("D", "1.0:2")
				output.collect(key, new Text(nodesStr)); // ("C", "A,D")
			}
		}
		/*

		Example: (C, (1.0:A,D))

		("A", "1.0:2")
		("D", "1.0:2")
		("C", "A,D")

		Input:

		(A, (1.0:D))
		(B, (1.0:C))
		(C, (1.0:A,D))
		(D, (1.0:B))

		Output:

		(D, 1:1)
		(C, 1:1)
		(A, 1:2)
		(D, 1:2)
		(B, 1:1)
		(A, D)
		(B, C)
		(C, AD)
		(D, B)

		*/
	}

	/*
	 * Calculating PageRank
	 */
	public static class Phase2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		/*
		Input:
		(D, 1:1)
		(C, 1:1)
		(A, 1:2)
		(D, 1:2)
		(B, 1:1)
		(A, D)
		(B, C)
		(C, AD)
		(D, B)

		Output:
		(A, (0.5:D))
		(B, (1.0:C))
		(C, (1.0:A,D))
		(D, (1.425:B))
		*/
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
		/*
		key, values:
		(D, 1:1)
		(D, 1:2)
		(D, B)
		*/
				throws IOException {
			// make a copy of values
			// values2 = <"1.0:1", "1.0:2", "B">
			ArrayList<String> values2 = new ArrayList<String>();
			while (values.hasNext()) {
				String value = values.next().toString();
				values2.add(value);
			}

			String nodesStr = "";

			// 0.85
			float damping = 0.85f;

			float newPR = 0.0f;
			float sum = 0.0f;
			for (int i = 0; i < values2.size(); i++) {
				String value = values2.get(i); // "1.0:1", // "B"
				String[] parts = value.split(":"); // ["1.0", "1"] // ["B"]
				if (parts.length > 1) {
					float PR = Float.parseFloat(parts[0]); // 1.0
					int links = Integer.parseInt(parts[1]); // 1
					sum += (PR / links);
				} else if (parts.length == 1) { // "B"
					nodesStr = value; // "B"
				}
			}
			newPR = (sum * damping + (1 - damping)); // (1.5*0.85 + (1-0.85)) = 1.425
			String tmp = Float.toString(newPR); // "1.425"
			tmp += ":"; // "1.425:"
			tmp += nodesStr; // "1.425:B"
			output.collect(key, new Text(tmp)); // (D, "1.425:B")
			/*
			Output:
			(A, (0.5:D))
			(B, (1.0:C))
			(C, (1.0:A,D))
			(D, (1.425:B))
			*/
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, Phase2.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJobName("PageRankPhase2");
		job.setMapperClass(Phase2Mapper.class);
		job.setReducerClass(Phase2Reducer.class);

		job.setInputFormat(KeyValueTextInputFormat.class);
		job.set("key.value.separator.in.input.line", "\t");

		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Phase2(), args);
		System.exit(res);
	}
}
