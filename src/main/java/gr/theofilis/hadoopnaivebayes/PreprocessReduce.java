package gr.theofilis.hadoopnaivebayes;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author George Theofilis, Konstantinos Skianis
 */
public class PreprocessReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;

        for (DoubleWritable value : values) {
            if (max < value.get()) {
                max = value.get();
            }

            if (min > value.get()) {
                min = value.get();
            }
        }

        context.write(new Text(key.toString().trim() + "_min"), new DoubleWritable(min));
        context.write(new Text(key.toString().trim() + "_max"), new DoubleWritable(max));
    }
}
