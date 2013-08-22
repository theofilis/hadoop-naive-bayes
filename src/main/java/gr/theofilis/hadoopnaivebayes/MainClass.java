package gr.theofilis.hadoopnaivebayes;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Theofilis George
 */
public class MainClass {

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new NaiveBayesDriver(), args);
        } catch (Exception ex) {
            Logger.getLogger(NaiveBayesDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
