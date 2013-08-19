package learn.sample;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateExpenseHistogram extends Configured implements Tool {

    private void printUsage() {
        System.out.println("Usage : AggregateExpenseHistogram <input_dir> <output>");
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
        }
        JobConf job = new JobConf(getConf(), AggregateExpenseHistogram.class);
        job.setJobName("Aggregate Expense");

        job.setJarByClass(AggregateExpenseHistogram.class);
        job.setMapperClass(ExpenseMap.class);
        job.setCombinerClass(ExpenseReduce.class);
        job.setReducerClass(ExpenseReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new AggregateExpenseHistogram(), args);
        System.exit(run);
    }
}
