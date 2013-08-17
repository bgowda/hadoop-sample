package learn.sample;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class ExpenseMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable lineNum, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            Text text = new Text();
            if (lineNum.get() > 0) {
                CSVParser csvParser = new CSVParser(new BufferedReader(new StringReader(value.toString())));
                List<CSVRecord> records = csvParser.getRecords();
                for (CSVRecord record : records) {

                    text.set(record.get(2).concat(","));
                    DoubleWritable doubleWritable = new DoubleWritable(Double.parseDouble(record.get(3)));
                    output.collect(text, doubleWritable);
                }

            }
        }
    }
