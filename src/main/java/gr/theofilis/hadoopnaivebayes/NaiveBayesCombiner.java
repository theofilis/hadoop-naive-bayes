package gr.theofilis.hadoopnaivebayes;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author George Theofilis, Konstantinos Skianis
 */
public class NaiveBayesCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
    
        context.write(key, new IntWritable(sum));
    }

    
    
}
