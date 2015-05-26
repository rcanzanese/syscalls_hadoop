/**
 * For binary system call traces output by the SCS, counts the number of 
 * occurrences of each system call.
 */
 
// TODO:
// - Do n-gram analysis
// - Output system call names instead of numbers
// - Do signature generation 

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.commons.io.FileUtils;
import java.util.List;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;


/**
 * Count system halls 
 */
public class SyscallCounter {

    /**
     * Input is the filename-contents map from the sequence file and output is 
     * a map of system call indices and counts.
     */ 
    public static class SyscallMapper extends Mapper<Text, BytesWritable, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        /** 
         * Load the data from the binary files and count the system calls
         */ 
        public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {

            byte[] data = value.getBytes();
            int lsb;
            int msb;
            int syscall;

            for ( int i = 0 ; i < value.getLength() ; i+=2)
            {
                // Get the system call index
                byte[] arr = { data[i], data[i+1] };
                ByteBuffer wrapped = ByteBuffer.wrap(arr);
                wrapped.order(ByteOrder.LITTLE_ENDIAN);
                short num = wrapped.getShort();
                syscall = convertToUnsigned(num);
                
                // Count the occurance of the systemcall
                context.write(new IntWritable(syscall), one);
            }
        }


        /**
         * Converts a signed short to an unsigned short in a int container
         */
        private int convertToUnsigned(short byteVal)
        {
            int intVal = byteVal >= 0 ? byteVal : 0x10000 + byteVal ;
            return intVal;
        }
    }

    /**
     * Inputs and outputs are system call indices and counts.
     */    
    public static class SyscallReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        private IntWritable result = new IntWritable();
        
        /**
         * Sum the calls to each system call.
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);		
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: SyscallCounter <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "system call counter");

        job.setJarByClass(SyscallCounter.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(SyscallMapper.class);
        job.setReducerClass(SyscallReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
