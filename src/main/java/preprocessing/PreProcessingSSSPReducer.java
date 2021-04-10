package preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * PreProcessingSSSPReducer is preprocessing reducer for Single Source Shortest Path.
 * Here we create adjacency list for the node, and if the node is source set the distance as 0 else set the
 * distance to infinity. We set the source node as active, all the other nodes are inactive.
 */
public class PreProcessingSSSPReducer extends Reducer<Text, Text, Text, Text> {
    private String sourceVertex;

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        sourceVertex = configuration.get("sourceVertex");
    }

    //pre-processing: setting the distance to either 0 or infinity
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        double distance;
        boolean isActive = false;
        boolean isVertex = true;
        if (key.toString().equals(sourceVertex)) {
            distance = 0.0;
            isActive = true;
        } else {
            distance = Double.MAX_VALUE;
        }
        StringBuilder builder = new StringBuilder();
        for (Text v : values) {
            builder.append(v.toString()).append(",");
        }
        context.write(key, new Text(distance + " " + isVertex + " " + isActive + " " + builder.substring(0, builder.length() - 1)));
    }
}
