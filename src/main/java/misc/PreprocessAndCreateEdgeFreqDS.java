package misc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PreprocessAndCreateEdgeFreqDS extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PreprocessAndCreateEdgeFreqDS.class);

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

                String eachLine = value.toString();
                String[] edgeAndTime = eachLine.split(" ");
                context.write(new Text(edgeAndTime[0]+" "+edgeAndTime[1]), new DoubleWritable(Math.log(Double.parseDouble(edgeAndTime[2]))));

        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
//            double sum = 0;
//            for (final DoubleWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Preprocess Edge Data Set");
        job.setJarByClass(PreprocessAndCreateEdgeFreqDS.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================
        job.setMapperClass(PreprocessAndCreateEdgeFreqDS.TokenizerMapper.class);
        job.setCombinerClass(PreprocessAndCreateEdgeFreqDS.IntSumReducer.class);
        job.setReducerClass(PreprocessAndCreateEdgeFreqDS.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new PreprocessAndCreateEdgeFreqDS(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}