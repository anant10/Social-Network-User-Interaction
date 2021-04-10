package allpairshortestpath;

import model.NodeAllPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import preprocessing.PreProcessingAllPairReducer;
import preprocessing.PreProcessingMapper;

/**
 * This is a runner class which runs one pre-processing job for All Pair shortest path.
 * And it runs another job until convergence is reached.
 */
public class AllPairShortestPathRunner extends Configured implements Tool {
    //Global counter
    public enum AllPairCounter {
        ITERATION
    }

    private static final Logger logger = LogManager.getLogger(AllPairShortestPathRunner.class);

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "All Pair Shortest Path Preprocessing Data");
        job1.setJarByClass(AllPairShortestPathRunner.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", " ");

        job1.setMapperClass(PreProcessingMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(PreProcessingAllPairReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/folder0"));

        boolean res = job1.waitForCompletion(true);

        long iterCount = 7;
        int i = 0;
        while (iterCount > 0) {
            final Configuration conf2 = getConf();
            final Job job2 = Job.getInstance(conf2, "All Pair");
            job2.setJarByClass(AllPairShortestPathRunner.class);
            final Configuration jobConf2 = job2.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", " ");

            job2.setMapperClass(AllPairShortestPathMapper.class);
            job2.setReducerClass(AllPairShortestPathReducer.class);
//            job2.setNumReduceTasks(0);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(NodeAllPair.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.addInputPath(job2, new Path(args[0] + "/folder" + (i)));
            FileOutputFormat.setOutputPath(job2, new Path(args[0] + "/folder" + (i + 1)));
            res = job2.waitForCompletion(true);
            i++;
            iterCount = job2.getCounters().findCounter(AllPairCounter.ITERATION).getValue();
            logger.info("iterCount " + iterCount);
            job2.getCounters().findCounter(AllPairCounter.ITERATION).setValue(0);
            logger.info("Iteration: " + i);
            logger.info("------------------------------------------------------------------------------------------");
        }


        return res ? 1 : 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new AllPairShortestPathRunner(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
