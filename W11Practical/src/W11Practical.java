
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *  W11Practical.java
 *  The programme will read the JSON file and then using the MapReduce.
 *  The result is that how many same words(url) are in the JSON file.
 *  The result will be saved as part-r-00000 file.
 */
public class W11Practical {

    private static final int INPUT_PATH = 0;
    private static final int OUTPUT_PATH = 1;
    private static final int MAXIMUM_ARGUMENT = 2;

    /**
     * In this main method, programme will do MapReduce the JSON file.
     * @param args  Command line arguments
     * @throws IOException  IOException from Reducer and Mapper class
     */
    public static void main(String[] args) throws IOException {

        if (args.length < MAXIMUM_ARGUMENT) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_directory>");
            System.exit(1);
        }

        String input_path = args[INPUT_PATH];
        String output_path = args[OUTPUT_PATH];

        // Setup new Job and Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        // Specify input and output paths
        FileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        // Set our own ScanWordsMapper as the mapper
        job.setMapperClass(ScanWordsMapper.class);

        // Specify output types produced by mapper (words with count of 1)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // The output of the reducer is a map from unique words to their total counts.
        job.setReducerClass(CountWordsReducer.class);

        // Specify the output types produced by reducer (words with total counts)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
