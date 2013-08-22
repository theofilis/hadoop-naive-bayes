package gr.theofilis.hadoopnaivebayes;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author George Theofilis, Konstantinos Skianis
 */
public class PreprocessMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String lineText = value.toString().trim();

        if (lineText.startsWith("@") || lineText.startsWith("%") || lineText.isEmpty()) {
            return;
        }

        String[] data = lineText.split(",");


        for (int i = 0; i < data.length; i++) {

            if (i == 0) {
                context.write(new Text("age"), new DoubleWritable(Double.parseDouble(data[i])));
            } else if (i == 2) {
                context.write(new Text("fnlwgt"), new DoubleWritable(Double.parseDouble(data[i])));
            } else if (i == 4) {
                context.write(new Text("education-num"), new DoubleWritable(Double.parseDouble(data[i])));
            } else if (i == 10) {
                context.write(new Text("capital-gain"), new DoubleWritable(Double.parseDouble(data[i])));
            } else if (i == 11) {
                context.write(new Text("capital-loss"), new DoubleWritable(Double.parseDouble(data[i])));
            } else if (i == 12) {
                context.write(new Text("hours-per-week"), new DoubleWritable(Double.parseDouble(data[i])));
            }

        }
    }
}
