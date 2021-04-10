package preprocessing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * PreProcessingMapper is a preprocessing map step which takes a input of (fromNode, toNode, weight) and emits (fromNode, (toNode:weight)).
 * The weights of timestamp are normalised using log.
 */
public class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {

    //pre-processing data
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String eachLine = value.toString();
        String[] edgeAndTime = eachLine.split(" ");
        DecimalFormat df = new DecimalFormat("#####.#####");
        // avoid -inf value
        if (Double.parseDouble(edgeAndTime[2]) == 0.0) {
            context.write(new Text(edgeAndTime[0]), new Text(edgeAndTime[1] + ":" + df.format(0)));
        } else {
            context.write(new Text(edgeAndTime[0]), new Text(edgeAndTime[1] + ":" + df.format(Math.log(Double.parseDouble(edgeAndTime[2])))));
        }
//        context.write(new Text(edgeAndTime[0]), new Text(edgeAndTime[1] + ":" + Double.parseDouble(edgeAndTime[2])));
    }
}
