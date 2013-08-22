package gr.theofilis.hadoopnaivebayes;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author George Theofilis, Konstantinos Skianis
 */
public class NaiveBayesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected HashMap<String, Double> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        System.err.println("setup");
        try (SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(conf.get("in")), conf)) {
            Text key = new Text();
            DoubleWritable value = new DoubleWritable();
            while (reader.next(key, value)) {
                map.put(key.toString().trim(), value.get());
            }
        } catch (IOException ex) {
            System.err.println("setup error " + ex);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineText = value.toString().trim();

        if (lineText.startsWith("@") || lineText.startsWith("%") || lineText.isEmpty()) {
            return;
        }

        String[] lineParts = lineText.split(",");

        getDiscretizationLabel("age", lineParts[0], lineParts[lineParts.length - 1], 3, context);
        getNominalLabel("workclass", lineParts[1], lineParts[lineParts.length - 1], context);
        getDiscretizationLabel("fnlwgt", lineParts[2], lineParts[lineParts.length - 1], 3, context);
        getNominalLabel("education", lineParts[3], lineParts[lineParts.length - 1], context);
        getDiscretizationLabel("education-num", lineParts[4], lineParts[lineParts.length - 1], 3, context);
        getNominalLabel("marital-status", lineParts[5], lineParts[lineParts.length - 1], context);
        getNominalLabel("occupation", lineParts[6], lineParts[lineParts.length - 1], context);
        getNominalLabel("relationship", lineParts[7], lineParts[lineParts.length - 1], context);
        getNominalLabel("race", lineParts[8], lineParts[lineParts.length - 1], context);
        getNominalLabel("sex", lineParts[9], lineParts[lineParts.length - 1], context);
        getDiscretizationLabel("capital-gain", lineParts[10], lineParts[lineParts.length - 1], 3, context);
        getDiscretizationLabel("capital-loss", lineParts[11], lineParts[lineParts.length - 1], 3, context);
        getDiscretizationLabel("hours-per-week", lineParts[12], lineParts[lineParts.length - 1], 3, context);
        getNominalLabel("native-country", lineParts[13], lineParts[lineParts.length - 1], context);
        getNominalLabel("", "", lineParts[lineParts.length - 1], context);
    }

    private void getDiscretizationLabel(String attributes, String value, String classname, int bin, Context context) throws IOException, InterruptedException {
        String dvalue = "";
        double min = 0, max = 0;
        min = map.get(attributes + "_min");
        max = map.get(attributes + "_max");

        double range = (max - min) / bin;

        double binvalue = ((int) Math.floor((Double.parseDouble(value) - min) / range));

        dvalue = dvalue + binvalue;

        context.write(new Text(attributes.trim() + "_" + dvalue.trim() + "_" + classname.trim()), new IntWritable(1));

    }

    private void getNominalLabel(String attributes, String value, String classname, Context context) throws IOException, InterruptedException {
        context.write(new Text(attributes.trim() + "_" + value.trim() + "_" + classname.trim()), new IntWritable(1));
    }
}
