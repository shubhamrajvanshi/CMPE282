package hadoopdemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		StringBuilder valueBuilder = new StringBuilder();

        for (Text val : values) {
            valueBuilder.append(val);
            valueBuilder.append(",");
        }
       // removing the extra comma appended.
        
        context.write(key, new Text(valueBuilder.substring(0, valueBuilder.length() - 1)));
        valueBuilder.setLength(0);
	}
}
