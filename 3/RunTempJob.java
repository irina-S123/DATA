import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
public class RunTempJob {
    
    public static SimpleDateFormat SDF=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    static class TempMapper extends Mapper<LongWritable,Text,KeyPair,Text>{
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
            String line=value.toString();
            //1949-10-01 14:21:02    34℃
            
            String[] ss=line.split("\t");
//            System.err.println(ss.length);
            if(ss.length==2){
                try{
                    
                    Date date=SDF.parse(ss[0]);
                    Calendar c=Calendar.getInstance();
                    c.setTime(date);
                    int year=c.get(1);
                    
                    String temp = ss[1].substring(0,ss[1].indexOf("℃"));
                    
                    KeyPair kp=new KeyPair();
                    kp.setYear(year);
                    kp.setTemp(Integer.parseInt(temp));
                    
                    context.write(kp,value);
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            }
        }
    }
    
    static class TempReducer extends Reducer<KeyPair,Text,KeyPair,Text> {
        @Override
        protected void reduce(KeyPair kp, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:values){
                context.write(kp,value);
            }
        }
    }

        public static void main(String args[]) throws IOException, InterruptedException{
        
        Configuration conf=new Configuration();

        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: temp <in> <out>");
            System.exit(2);
        }
        
        Job job=new Job(conf,"temp");
        
        job.setJarByClass(RunTempJob.class);
        
        job.setMapperClass(RunTempJob.TempMapper.class);
        job.setReducerClass(RunTempJob.TempReducer.class);
       
        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
        
        job.setNumReduceTasks(3);//3个年份
        
        job.setPartitionerClass(FirstPartition.class);
        job.setSortComparatorClass(SortTemp.class);
        job.setGroupingComparatorClass(GroupTemp.class);
        
        boolean isSuccess= false;
        try {
            isSuccess = job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        
        System.exit(isSuccess ?0:1);
    }
}