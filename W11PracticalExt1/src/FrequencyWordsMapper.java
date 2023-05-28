
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Scanner;

/**
 * FrequencyWordsMapper.java.
 * Extending Mapper<key input, value input, key output, value output> class
 * For overriding the method map(key input, value input output)
 *
 * This class will mapping the result from the words count.
 */
public class FrequencyWordsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final int KEY = 0;
    private static final int VALUE = 1;

    /**
     * Override method from Mapper.map(key, value, output)
     * In this case, method will change the order (Text LongWritable) to (LongWritable Text)
     * And then mapping the result.
     * @param key   is the character offset within the file of the start of the line
     * @param value One sentence from the file
     * @param output    Output
     * @throws IOException  IOException which can occur during writing the result
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String line = value.toString();
        //Read the line
        Scanner scanner = new Scanner(line);

        while (scanner.hasNext()) {
            //Hadoop saved the result KEY tab VALUE hence, should be split by tab
            String[] keyAndValue = scanner.nextLine().split("\t");
            //Change the order
            output.write(new LongWritable(Long.parseLong(keyAndValue[VALUE])), new Text(keyAndValue[KEY]));
        }
        scanner.close();
    }
}
