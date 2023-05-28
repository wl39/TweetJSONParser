
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class will write the frequency of the words in ascending order.
 */
public class CleanFrequencyReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    // The output of the reducer is a map from unique words to their total counts.

    /**
     * Override method from Reducer.reduce(key, value, output).
     * Receiving the mapping result and shuffling, sorting and reducing that result
     * @param key   The Text from mapping result
     * @param values    values from the mapping result
     * @param output    Output
     * @throws IOException  IOException which can occur during writing the result
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted
     */
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
        //In this reduce method, unlike the other reduce methods, save just exact one line (just reversed the order key and value to value and key)
        for (Text value : values) {
            output.write(new Text(value.toString()), key);
        }
    }
}
