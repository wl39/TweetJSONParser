
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CountWordsReducer.java.
 * Extending Reducer<key input, value input, key output, value output> class
 * For overriding the method reduce(key input, value input output)
 */
public class CountWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

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
    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

        // The key is the word.
        // The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).

        int sum = 0;
        for (LongWritable value : values) {
            long l = value.get();
            sum += l;
        }
        output.write(key, new LongWritable(sum));
    }
}
