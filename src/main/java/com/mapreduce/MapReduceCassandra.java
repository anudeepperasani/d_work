/**
 Anudeep P
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *  */
public class MapReduceCassandra extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(MapReduceCassandra.class);

    static final String KEYSPACE = "Turbulence";
    static final String COLUMN_FAMILY = "InstanceData";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";
    private static final String OUTPUT_PATH_PREFIX = "/root/workspace/output";
    
    private static final String CONF_COLUMN_NAME = "columnname";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new MapReduceCassandra(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, Text>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;
        String key_sample;
	//int c = 0;
        
        HashMap<String, Integer> cache1 = new HashMap<String, Integer>();
        ArrayList<String> list = new ArrayList<String>();

        @SuppressWarnings("resource")
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
            sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
            Path[] uris = DistributedCache.getLocalCacheFiles(context
            	    .getConfiguration());
            BufferedReader fis;
            
            fis = new BufferedReader(new FileReader(uris[0].toString()));
            
            String chunk = null;
            logger.info("parsing file");
            String[] records = null;
            while ((chunk = fis.readLine()) != null) {
            	records = null;
            	String temp = chunk.toString();
            	records =  temp.split("\\s+");
            	for(String s : records)
            	cache1.put(s,1);
		//c = c+1;
            }
            
            // do whatever you like with xml using parser
            //System.out.println("Records :" + records);
           }
            
        

        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
        	
       int count = 0;
           
            	 key_sample = ByteBufferUtil.string(key);
            		// if (cache1.containsKey(key_sample) && (cache1.get(key_sample)).equals(1))
            		 //{
            			 IColumn column = columns.get(sourceColumn);
            			 if (column == null)
            				 return;
                    
                    String value = ByteBufferUtil.string(column.value());
                    //count++;
                    logger.debug("read " + key + ":" + value + " from " + context.getInputSplit());
                    context.write(new Text(ByteBufferUtil.string(key)), new Text(value));
                    //if(count == c)
                    //;
			 // No need to look further.
               // } 
                
       
       //}
    }
    }

    public static class ReducerToFilesystem extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
           
            for (Text val : values)
               
            context.write(key, val);
        }
    }

    /*public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
    {
        private ByteBuffer outputKey;

        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException
        {
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        }

        private static Mutation getMutation(Text word, int sum)
        {
            Column c = new Column();
            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
            c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);
            return m;
        }
    }*/
    

    public int run(String[] args) throws Exception
    {
        String outputReducerType = "filesystem";
        if (args != null && args[0].startsWith(OUTPUT_REDUCER_VAR))
        {
            String[] s = args[0].split("=");
            if (s != null && s.length == 2)
                outputReducerType = s[1];
        }
        logger.info("output reducer type: " + outputReducerType);

        
            String columnName = "data";
            getConf().set(CONF_COLUMN_NAME, columnName);
            Path file_path = new Path("/root/workspace/keyset1.txt");
            

            Job job = new Job(getConf(), "wordcount");
            job.setJarByClass(MapReduceCassandra.class);
            job.setMapperClass(TokenizerMapper.class);
            DistributedCache.addCacheFile(file_path.toUri(),
            	    job.getConfiguration());

            if (outputReducerType.equalsIgnoreCase("filesystem"))
            {
                job.setCombinerClass(ReducerToFilesystem.class);
                job.setReducerClass(ReducerToFilesystem.class);
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(IntWritable.class);
                job.setNumReduceTasks(0);
                FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
            }
            /*else
            {
                job.setReducerClass(ReducerToCassandra.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                job.setOutputKeyClass(ByteBuffer.class);
                job.setOutputValueClass(List.class);

                job.setOutputFormatClass(ColumnFamilyOutputFormat.class);

                ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
            }*/

            job.setInputFormatClass(ColumnFamilyInputFormat.class);


            ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
            ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
            ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.Murmur3Partitioner");
            ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
            ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

            job.waitForCompletion(true);
        
        return 0;
    }
}

    

	
