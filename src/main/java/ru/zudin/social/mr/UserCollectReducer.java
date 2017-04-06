package ru.zudin.social.mr;

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
            multipleOutputs.write(new Text(text), new Text(""), key.toString());
        }
    }
}
