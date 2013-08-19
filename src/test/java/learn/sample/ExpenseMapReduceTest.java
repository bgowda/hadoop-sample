package learn.sample;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class ExpenseMapReduceTest {

    MapDriver<LongWritable,Text, Text,DoubleWritable> mapDriver;
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;


    @Before
    public void setup(){
        ExpenseMap expenseMap = new ExpenseMap();
        ExpenseReduce expenseReduce = new ExpenseReduce();

        mapDriver = MapDriver.newMapDriver(expenseMap);
        reduceDriver = ReduceDriver.newReduceDriver(expenseReduce);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(expenseMap,expenseReduce);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(100),new Text("24/03/2013,5411,\"TESCO STORE 2934       NEW MALDEN\",-43.23"));
        mapDriver.withOutput(new Text("\"TESCO STORE 2934       NEW MALDEN\","),new DoubleWritable(-43.23));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        ArrayList<DoubleWritable> doubleWritables = Lists.newArrayList(new DoubleWritable(-43.23), new DoubleWritable(-10));
        reduceDriver.withInput(new Text("\"TESCO STORE 2934       NEW MALDEN\","), doubleWritables);
        reduceDriver.withOutput(new Text("\"TESCO STORE 2934       NEW MALDEN\","),new DoubleWritable(-53.23));
        reduceDriver.runTest();
    }
}

