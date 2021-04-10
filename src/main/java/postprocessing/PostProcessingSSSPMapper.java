package postprocessing;

import model.NodeSSSP;
import model.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * PreProcessingMapper is a preprocessing map step which takes a input of (fromNode, toNode, weight) and emits (fromNode, (toNode:weight)).
 * The weights of timestamp are normalised using log.
 */
public class PostProcessingSSSPMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        NodeSSSP node = NodeSSSP.createNode(split);
        context.write(new Text(String.valueOf(node.getNodeId())), new Text(String.valueOf(node.getDistance())));
    }
}