import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.io.IOException;
import java.io.StringReader;

import static javax.json.JsonValue.ValueType.STRING;

/**
 * ScanWordsMapper.java.
 * Extending Mapper<key input, value input, key output, value output> class
 * For overriding the method map(key input, value input output)
 */
public class ScanWordsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    // The output of the mapper is a map from words (including duplicates) to the value 1.

    /**
     * Override method from Mapper.map(key, value, output)
     * In this case, method will parsing the JSON file and pick the JsonValue ("expanded_url").
     * And then mapping the result.
     * @param key   is the character offset within the file of the start of the line
     * @param value One sentence from the file
     * @param output    Output
     * @throws IOException  IOException which can occur during writing the result
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        // The key is the character offset within the file of the start of the line, ignored.
        // The value is a line from the file.

        String line = value.toString();

        //Reading line
        JsonReader reader = Json.createReader(new StringReader(line));
        //Change String to Json object
        JsonObject tweetObject = reader.readObject();
        //Extracting the object named "entities" from the tweet object
        JsonObject entitiesObject = tweetObject.getJsonObject("entities");
        //Entities Object is nullalbe
        if (entitiesObject != null) {
            //Extracting the array named "urls" from the entities object
            JsonArray urlsArray = entitiesObject.getJsonArray("urls");
            //Urls array is also nullable
            if (urlsArray != null) {
                //Urls Array can have more then one expanded url object.
                for (int i = 0; i < urlsArray.size(); i++) {
                    //Get the Json Object from the array.
                    JsonObject items = urlsArray.getJsonObject(i);
                    //Extracting the object named "expanded_url"
                    JsonValue expanded_urls = items.get("expanded_url");
                    //expanded_urls is also nullable and it could not be the string.
                    if (expanded_urls != null && expanded_urls.getValueType() == STRING) {
                        //Changing the json value to string
                        String result = expanded_urls.toString();
                        //write the output and count 1
                        output.write(new Text(result), new LongWritable(1));
                    }
                }
            }
        }
    }
}
