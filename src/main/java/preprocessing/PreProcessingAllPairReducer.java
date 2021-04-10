package preprocessing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * PreProcessingAllPairReducer is preprocessing reduce task for All pair shortest path.
 * Here we create a adjacency list for the node and set is Vertex is true for all the nodes.
 */
public class PreProcessingAllPairReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        boolean isVertex = true;
        StringBuilder builder = new StringBuilder();
        for (Text v : values) {
            if (v.toString().split(":")[0].equals(key.toString())) {
                continue;
            }
            builder.append(v.toString()).append(",");
        }
        // add weight as zero for the same node i.e. from (u ->u, set weight as 0)
        builder.append(key.toString()).append(":").append("0.0");
        context.write(key, new Text(builder.toString() + " " + isVertex));
    }
}
