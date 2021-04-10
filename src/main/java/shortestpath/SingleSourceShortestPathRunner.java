package shortestpath;

import model.NodeSSSP;
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
import postprocessing.PostProcessingSSSPMapper;
import preprocessing.PreProcessingMapper;
import preprocessing.PreProcessingSSSPReducer;

/**
 * This is a runner class which runs one pre-processing job for Single Source shortest path.
 * And it runs another job until convergence is reached. And a PostProcessing step to get the node and
 * its distance from source node
 */
public class SingleSourceShortestPathRunner extends Configured implements Tool {
    //Counter to keep track of changes in reduce phase.
    public enum SSSPCounter {
        ITERATION
    }

    private static final Logger logger = LogManager.getLogger(SingleSourceShortestPathRunner.class);

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "Single Source Shortest Path Preprocessing Data");
        job1.setJarByClass(SingleSourceShortestPathRunner.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", " ");
        jobConf.set("mapreduce.job.split.metainfo.maxsize", "-1");
        job1.getConfiguration().set("sourceVertex", String.valueOf(1));

        job1.setMapperClass(PreProcessingMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(PreProcessingSSSPReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/folder0"));

        boolean res = job1.waitForCompletion(true);

        long iterCount = 1;
        int i = 0;
        while (iterCount > 0) {
            final Configuration conf2 = getConf();
            final Job job2 = Job.getInstance(conf2, "Single Source Shortest Path");

            job2.setJarByClass(shortestpath.SingleSourceShortestPathRunner.class);
            final Configuration jobConf2 = job2.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", " ");
            jobConf2.set("mapreduce.job.split.metainfo.maxsize", "-1");
            job2.setMapperClass(shortestpath.SingleSourceShortestPathMapper.class);
            job2.setReducerClass(shortestpath.SingleSourceShortestPathReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(NodeSSSP.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.addInputPath(job2, new Path(args[0] + "/folder" + (i)));
            FileOutputFormat.setOutputPath(job2, new Path(args[0] + "/folder" + (i + 1)));
            res = job2.waitForCompletion(true);
            i++;
            iterCount = job2.getCounters().findCounter(SSSPCounter.ITERATION).getValue();
            logger.info("iterCount " + iterCount);
            job2.getCounters().findCounter(SSSPCounter.ITERATION).setValue(0);
            logger.info("Iteration: " + i);
            logger.info("------------------------------------------------------------------------------------------");
        }

        final Job job3 = Job.getInstance(conf1, "Single Source Shortest Path Postprocessing Data");
        job3.setJarByClass(SingleSourceShortestPathRunner.class);
        final Configuration jobConf3 = job3.getConfiguration();
        jobConf3.set("mapreduce.output.textoutputformat.separator", " ");
        jobConf3.set("mapreduce.job.split.metainfo.maxsize", "-1");

        job3.setMapperClass(PostProcessingSSSPMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job3, new Path(args[0] + "/folder" + i));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        res = job3.waitForCompletion(true);

        return res ? 1 : 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new SingleSourceShortestPathRunner(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
