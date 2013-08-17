package learn.sample;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

public class ExpenseReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            BigDecimal sum = BigDecimal.ZERO;

            while (values.hasNext()) {
                double v = values.next().get();
                BigDecimal bigDecimal = BigDecimal.valueOf(v);
                sum = sum.add(bigDecimal);
            }

            output.collect(key, new DoubleWritable(sum.doubleValue()));
        }
}