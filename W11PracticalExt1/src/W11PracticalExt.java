
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *  W11PracticalExt.java
 *  The programme will read the JSON file and then using the MapReduce.
 *  The result is that how many same words(url) are in the JSON file.
 *  The result will be saved as part-r-00000 file.
 *  Also saved the file for frequency words.
 */
public class W11PracticalExt {

    private static final int INPUT_PATH = 0;
    private static final int OUTPUT_PATH = 1;
    private static final int CLEAN_OPTION = 2;
    private static final int MAXIMUM_ARGUMENT = 3;
    /**
     * In this main method, the programme will do MapReduce the JSON file.
     * After that, the programme will MapReduce the file which is mapreduced JSON file.
     * @param args  Commandline argument <Input path> <Output Directory> <true or false>
     * @throws IOException  IOException from Reducer and Mapper class
     */
    public static void main(String[] args) throws IOException {

        if (args.length < MAXIMUM_ARGUMENT) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11PracticalExt <input_path> <output_directory> <true_or_false>");
            System.exit(1);
        }

        String input_path = args[INPUT_PATH];
        String output_path = args[OUTPUT_PATH];
        Boolean clean = Boolean.parseBoolean(args[CLEAN_OPTION]);

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
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //This job is for getting the popular words (frequency words)
        Job job2 = Job.getInstance(conf, "Popular Words");

        FileInputFormat.setInputPaths(job2, new Path(output_path + "/part-r-00000"));
        //Ouput will save into output2 directory
        FileOutputFormat.setOutputPath(job2, new Path("output2"));

        job2.setMapperClass(FrequencyWordsMapper.class);

        //Map output is LongWritable, Text.
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        //If user want to clean form
        if (clean) {
            //Use CleanFrequencyReducer.class to make clean form.
            job2.setReducerClass(CleanFrequencyReducer.class);
            //And then sorting the result as descending order.
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);
        } else {
            job2.setReducerClass(FrequencyWordsCount.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);
        }

        try {
            job.waitForCompletion(true);
            job2.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
