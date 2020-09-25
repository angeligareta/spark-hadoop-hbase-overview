/**
 * <h1>TopTen Hadoop and HBase Assignment</h1>
 * In this exercise the aim is to read a record of users from an xml stored in HDFS, execute a MapReduce job that calculates
 * the top ten users in the list with more reputation and stores the output in a HBase table.
 * <h2>Arguments</h2>
 * In order to compile the program the commands needed are:
 * <ol>
 *     <li>
 *      Start necessary services
 *      $HADOOP_HOME/bin/hdfs --daemon start namenode
 *      $HADOOP_HOME/bin/hdfs --daemon start datanode
 *      $HBASE_HOME/bin/start-hbase.sh
 * </li>
 * <li>
 *      Create the HBase table topten with one column family info to store the id and reputation of users.
 *      $HBASE_HOME/bin/hbase shell
 *      [Inside hbase shell] create ’topten’, ’info’
 * </li>
 * <li>
 *      Set necessary environment variables
 *      export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
 *      export HBASE_CLASSPATH=$($HBASE_HOME/bin/hbase classpath)
 *      export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_CLASSPATH
 * </li>
 * <li>
 *      Compile the program
 *      javac -cp $HADOOP_CLASSPATH -d output src/main/java/TopTen.java
 * </li>
 * <li>
 *     Generate the jar using the files in the previous compiled folder
 *     jar -cvf topten.jar -C output .
 * </li>
 * <li>
 *     Execute the program passing as first argument the hdfs path where users.xml is and the HBase table name:
 *     $HADOOP_HOME/bin/hadoop jar topten.jar TopTen <HDFS_PATH>/users.xml topten
 * </li>
 * </ol>
 * <h2>Authors</h2>
 * <ul>
 * <li>
 * Serghei Socolovschi (serghei@kth.se)
 * </li>
 * Angel Igareta (angel@kth.se)
 * </li>
 * </ul>
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TopTen {

    static final int TOP_N = 10;
    static final String SEPARATOR = "-";
    static final String COLUMN_FAMILY = "info";

    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    // Mapper class. It receives the input xml, parses it and store into a local hashmap the values id and reputation of each valid user.
    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record.
        // We decided to use HashMap instead of the proposed TreeMap to avoid the extra computation of sorting the results by id, as in this case
        // it is not required, only the values column. If instead of id, reputation was add as the key for TreeMap, the sorting would already be done,
        // however, two users that have the same reputation would not appear in the results, as TreeMap keys must be unique.
        HashMap<Integer, Integer> repToRecordMap = new HashMap<>();

        public void map(Object key, Text value, Context context) {
            Map<String, String> userValues = transformXmlToMap(value.toString());

            // Select columns that are going to be used
            String id = userValues.get("Id");
            String reputation = userValues.get("Reputation");

            // Only add value to the local state if the user is valid (some could have a null id)
            if (id != null) {
                repToRecordMap.put(Integer.parseInt(id), Integer.parseInt(reputation));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort array of records<id, reputation> by reputation and only take TOP_N results
            Iterator<Map.Entry<Integer, Integer>> userRecordIterator = repToRecordMap
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) // Sort in reverse order
                    .limit(TOP_N)
                    .iterator();

            while (!userRecordIterator.hasNext()) {
                Map.Entry<Integer, Integer> userRecord = userRecordIterator.next();

                // Parse use record to custom text to be serializable and more efficient
                Text parsedOutput = new Text(userRecord.getKey() + SEPARATOR + userRecord.getValue());
                context.write(NullWritable.get(), parsedOutput);
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
        public void reduce(NullWritable key, Iterable<Text> userRecordsRaw, Context context) {
            try {
                // A counter of elements has been added to prevent from outputting more than TOP_N elements.
                // This is just in case multiple mappers output to this reducer (not in this case as the file < 128MB, block size)
                int index = 0;
                Iterator<Text> userRecordsRawIterator = userRecordsRaw.iterator();

                while (index < TOP_N && userRecordsRawIterator.hasNext()) {
                    Text userRecordRaw = userRecordsRawIterator.next();

                    // Parse user record following the previous format as in the mapper
                    String[] userRecordParsed = userRecordRaw.toString().split(SEPARATOR);

                    // The output column name is based on the index of the record
                    String outputTableRowName = ("row" + (TOP_N - 1 - index));

                    // Create a row and insert two columns, one with the value of the user id and other one with user reputation
                    Put outputTableRow = new Put(outputTableRowName.getBytes());
                    outputTableRow.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("id"), Bytes.toBytes(userRecordParsed[0]));
                    outputTableRow.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rep"), Bytes.toBytes(userRecordParsed[1]));
                    context.write(NullWritable.get(), outputTableRow);

                    index++;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    public static void main(String[] args) throws Exception {
        // Receive necessary configuration from program args.
        String inputDirPath = args[0];
        String outputTableName = args[1];

        // Creates a hadoop configuration with HBase resources
        Configuration conf = HBaseConfiguration.create();

        // Creates a new Job with no particular Cluster, a given configuration and a jobName.
        Job job = Job.getInstance(conf, "Top Ten");
        // Set TopTen as jar class to submit the job to the resource manager
        job.setJarByClass(TopTen.class);

        // Input format describes the input specification for a Map-Reduce job. It validates the input-specification of the job and
        // split-up input files into logical splits which would be assigned to an individual mapper.
        // With addInputPath we are indicating a path to the list of inputs for the map-reduce job
        FileInputFormat.addInputPath(job, new Path(inputDirPath));

        // Set the Mapper for the job, its output key class and value class. [They are matching the output types from TopTenMapper class.]
        job.setMapperClass(TopTenMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Here, as proposed in the assignment description, we are setting to one the number of reduce tasks for this job.
        // This is done to avoid distributing our input into multiple reducers and having different top-ten results. Instead, we do everything in
        // one cluster and one reducer.
        job.setNumReduceTasks(1);

        // Here the HBase dependency jars as well as jars for any of the configured job classes are added to the job configuration,
        // so the job can ship them to the cluster.
        TableMapReduceUtil.addDependencyJars(job);
        // This is used before sending a TableReduce job, it appropriately set up the JobConf.
        TableMapReduceUtil.initTableReducerJob(outputTableName, TopTenReducer.class, job);

        // Send the job and wait for completion before exiting the program.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}