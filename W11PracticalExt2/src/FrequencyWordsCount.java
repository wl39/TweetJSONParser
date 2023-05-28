
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * In this class, based on the output from mapping,
 * Getting the frequency of the word.
 * Output will saved in ascending order.
 */
public class FrequencyWordsCount extends Reducer<LongWritable, Text, Text, LongWritable> {

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

        //When adding the String, StringBuilder is more efficient than using + operator
        StringBuilder sum = new StringBuilder();
        int count = 0;
        for (Text value : values) {
            //If the value is the first one do not append ","
            if (count == 0) {
                sum.append(value);
            } else {
                sum.append(", ").append(value);
            }
            count++;
        }
        output.write(new Text(sum.toString()), key);
    }
}
