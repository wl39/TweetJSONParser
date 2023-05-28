
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
 * LocationMapper.java.
 * Extending Mapper<key input, value input, key output, value output> class
 * For overriding the method map(key input, value input output)
 */
public class LocationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * Override method from Mapper.map(key, value, output)
     * In this case, method will parsing the JSON file and pick the JsonValue ("location") from user object.
     * And then mapping the result.
     * @param key   is the character offset within the file of the start of the line
     * @param value One sentence from the file
     * @param output    Output
     * @throws IOException  IOException which can occur during writing the result
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        //Logic is exactly same as basic practical
        String line = value.toString();
        JsonReader reader = Json.createReader(new StringReader(line));
        JsonObject tweetObject = reader.readObject();
        JsonObject userObject = tweetObject.getJsonObject("user");
        if (userObject != null) {
            JsonValue location = userObject.get("location");
            if (location != null && location.getValueType() == STRING) {
                output.write(new Text(location.toString()), new LongWritable(1));
            }
        }
    }
}
