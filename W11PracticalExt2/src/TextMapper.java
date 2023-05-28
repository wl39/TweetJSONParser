
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.StringReader;

import static javax.json.JsonValue.ValueType.NUMBER;
import static javax.json.JsonValue.ValueType.STRING;

/**
 * TextMapper.java.
 * Extending Mapper<key input, value input, key output, value output> class
 * For overriding the method map(key input, value input output)
 */
public class TextMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * Override method from Mapper.map(key, value, output)
     * In this case, method will parsing the JSON file and pick the JsonValue ("text") and ("retweet_count") from retweeted_status object.
     * And then mapping the result.
     * @param key   is the character offset within the file of the start of the line
     * @param value One sentence from the file
     * @param output    Output
     * @throws IOException  IOException which can occur during writing the result
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {


        String line = value.toString();
        JsonReader reader = Json.createReader(new StringReader(line));
        JsonObject tweetObject = reader.readObject();
        JsonObject entitiesObject = tweetObject.getJsonObject("retweeted_status");
        if (entitiesObject != null) {
            JsonValue count = entitiesObject.get("retweet_count");
            JsonValue text = entitiesObject.get("text");
            if (text != null && text.getValueType() == STRING && count != null && count.getValueType() == NUMBER) {
                //Saving text and retweet_count
                output.write(new Text(text.toString()), new LongWritable(Long.parseLong(count.toString())));
            }
        }
    }
}
