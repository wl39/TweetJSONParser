
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *  W11PracticalExt2.java
 *  The programme will read the JSON file and then using the MapReduce.
 *  The result is that how many same words(url) are in the JSON file.
 *  The result will be saved as part-r-00000 file.
 *  Also saved the file for frequency words.
 *
 *  User can put 4 arguments input path, output path, query and clean option.
 *  Query options are text, user, hashtags and location.
 */
public class W11PracticalExt2 {

    private static final int INPUT_PATH = 0;
    private static final int OUTPUT_PATH = 1;
    private static final int QUERY = 2;
    private static final int CLEAN_OPTION = 3;
    private static final int MAXIMUM_ARGUMENT = 4;

    /**
     * In this main method, the programme will do MapReduce the JSON file.
     * After that, the programme will MapReduce the file which is mapreduced JSON file.
     * @param args  Commandline argument <Input path> <Output Directory> <true or false>
     * @throws IOException  IOException from Reducer and Mapper class
     */
    public static void main(String[] args) throws IOException {

        if (args.length < MAXIMUM_ARGUMENT) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11PracticalExt2 <input_path> <output_directory> <query> <true_or_false>");
            System.exit(1);
        }
        //clean descending order.. choose
        String input_path = args[INPUT_PATH];
        String output_path = args[OUTPUT_PATH];
        String query = args[QUERY];
        boolean clean = Boolean.parseBoolean(args[CLEAN_OPTION]);

        // Setup new Job and Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        Job job2 = Job.getInstance(conf, "Popular Words");

        // Specify input and output paths
        FileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        //If user want to clean form
        if (clean) {
            //Use CleanFrequencyReducer.class to make clean form.
            job2.setReducerClass(CleanFrequencyReducer.class);
            //And then sorting the result as descending order.
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        } else {
            job2.setReducerClass(FrequencyWordsCount.class);
        }

        switch (query) {
            case "text":
                job.setMapperClass(TextMapper.class);
                job.setReducerClass(CountWordsReducer.class);
                break;
            case "user":
                job.setMapperClass(UserRetweetedMapper.class);
                job.setReducerClass(CountWordsReducer.class);
                break;

            case "hashtag":
                job.setMapperClass(HashTagsMapper.class);
                job.setReducerClass(CountWordsReducer.class);
                break;
            case "location":
                job.setMapperClass(LocationMapper.class);
                job.setReducerClass(CountWordsReducer.class);
                break;
            default:
                System.out.println("Options: text, user, hashtag and location");
                break;
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        FileInputFormat.setInputPaths(job2, new Path(output_path + "/part-r-00000"));
        //Ouput will save into output_sorted directory
        FileOutputFormat.setOutputPath(job2, new Path(output_path + "_sorted"));

        job2.setMapperClass(FrequencyWordsMapper.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);


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
