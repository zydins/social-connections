package ru.zudin.social.mr.matcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author sergey
 * @since 06.04.17
 */
public class UserCollectReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    public void setup(Context context){
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException{
        multipleOutputs.close();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> texts = StreamSupport.stream(values.spliterator(), false)
                .map(Text::toString)
                .distinct()
                .collect(Collectors.toList());
        for (String text : texts) {
            String[] split = text.split("\t");
            String otherKey = split[0].equals(key.toString()) ? split[1] : split[0];
            multipleOutputs.write(new Text(key.toString() + "\t" + otherKey + "\t" + split[2]), new Text(""), key.toString());
            multipleOutputs.write(new Text(key.toString() + "\t" + otherKey + "\t" + split[2]), new Text(""), "total_res");
        }
    }
}
