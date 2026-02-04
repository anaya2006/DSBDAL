import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
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
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
/* OUTPUT 
admin1@Your:~$ javac -classpath "$(hadoop classpath)" WordCount.java
admin1@Your:~$ jar cf wc.jar WordCount *.class
admin1@Your:~$ hadoop jar wc.jar WordCount /input /output
2026-02-04 12:13:07,162 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://localhost:9000/output already exists
	at org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:164)
	at org.apache.hadoop.mapreduce.JobSubmitter.checkSpecs(JobSubmitter.java:277)
	at org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal(JobSubmitter.java:143)
	at org.apache.hadoop.mapreduce.Job$11.run(Job.java:1565)
	at org.apache.hadoop.mapreduce.Job$11.run(Job.java:1562)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1762)
	at org.apache.hadoop.mapreduce.Job.submit(Job.java:1562)
	at org.apache.hadoop.mapreduce.Job.waitForCompletion(Job.java:1583)
	at WordCount.main(WordCount.java:59)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.util.RunJar.run(RunJar.java:323)
	at org.apache.hadoop.util.RunJar.main(RunJar.java:236)
admin1@Your:~$ hdfs dfs -rmdir /output
rmdir: `/output': Directory is not empty
admin1@Your:~$ hdfs dfs -rm -r /output
Deleted /output
admin1@Your:~$ hadoop jar wc.jar WordCount /input /output
2026-02-04 12:13:54,208 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
2026-02-04 12:13:54,472 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2026-02-04 12:13:54,501 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/admin1/.staging/job_1770185496525_0003
2026-02-04 12:13:54,703 INFO input.FileInputFormat: Total input files to process : 1
2026-02-04 12:13:54,791 INFO mapreduce.JobSubmitter: number of splits:1
2026-02-04 12:13:54,899 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1770185496525_0003
2026-02-04 12:13:54,900 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-02-04 12:13:55,024 INFO conf.Configuration: resource-types.xml not found
2026-02-04 12:13:55,024 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-02-04 12:13:55,085 INFO impl.YarnClientImpl: Submitted application application_1770185496525_0003
2026-02-04 12:13:55,118 INFO mapreduce.Job: The url to track the job: http://Your.Domain.tld:8088/proxy/application_1770185496525_0003/
2026-02-04 12:13:55,119 INFO mapreduce.Job: Running job: job_1770185496525_0003
2026-02-04 12:14:00,208 INFO mapreduce.Job: Job job_1770185496525_0003 running in uber mode : false
2026-02-04 12:14:00,209 INFO mapreduce.Job:  map 0% reduce 0%
2026-02-04 12:14:04,243 INFO mapreduce.Job:  map 100% reduce 0%
2026-02-04 12:14:08,262 INFO mapreduce.Job:  map 100% reduce 100%
2026-02-04 12:14:09,273 INFO mapreduce.Job: Job job_1770185496525_0003 completed successfully
2026-02-04 12:14:09,333 INFO mapreduce.Job: Counters: 54
	File System Counters
		FILE: Number of bytes read=183
		FILE: Number of bytes written=473707
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=218
		HDFS: Number of bytes written=113
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
		HDFS: Number of bytes read erasure-coded=0
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=1632
		Total time spent by all reduces in occupied slots (ms)=1724
		Total time spent by all map tasks (ms)=1632
		Total time spent by all reduce tasks (ms)=1724
		Total vcore-milliseconds taken by all map tasks=1632
		Total vcore-milliseconds taken by all reduce tasks=1724
		Total megabyte-milliseconds taken by all map tasks=1671168
		Total megabyte-milliseconds taken by all reduce tasks=1765376
	Map-Reduce Framework
		Map input records=4
		Map output records=24
		Map output bytes=212
		Map output materialized bytes=183
		Input split bytes=99
		Combine input records=24
		Combine output records=16
		Reduce input groups=16
		Reduce shuffle bytes=183
		Reduce input records=16
		Reduce output records=16
		Spilled Records=32
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=73
		CPU time spent (ms)=830
		Physical memory (bytes) snapshot=587153408
		Virtual memory (bytes) snapshot=5125922816
		Total committed heap usage (bytes)=476053504
		Peak Map Physical memory (bytes)=348278784
		Peak Map Virtual memory (bytes)=2557128704
		Peak Reduce Physical memory (bytes)=238874624
		Peak Reduce Virtual memory (bytes)=2568794112
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=119
	File Output Format Counters 
		Bytes Written=113
admin1@Your:~$ hdfs dfs -ls /output
Found 2 items
-rw-r--r--   1 admin1 supergroup          0 2026-02-04 12:14 /output/_SUCCESS
-rw-r--r--   1 admin1 supergroup        113 2026-02-04 12:14 /output/part-r-00000
admin1@Your:~$ hdfs dfs -cat /output/part-r-00000
Hello	1
This	1
a	1
as	1
file	3
file.	1
for	1
hdfs	3
input	1
is	2
sample	1
text	1
the	4
this	1
used	1
wordcount	1
admin1@Your:~$ 

*/
