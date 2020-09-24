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

    static final String SEPARATOR = "-";
    static final String COLUMN_FAMILY = "info";

    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
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

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        HashMap<Integer, Integer> repToRecordMap = new HashMap<Integer, Integer>();

        public void map(Object key, Text value, Context context) {
            Map<String, String> userValues = transformXmlToMap(value.toString());

            String id = userValues.get("Id");
            String reputation = userValues.get("Reputation");

            // Add values to the local state
            if (id != null) {
                repToRecordMap.put(Integer.parseInt(id), Integer.parseInt(reputation));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<Integer, Integer>> userRecordIterator = repToRecordMap
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) // Sort in reverse order
                    .iterator();

            int index = 0;
            while (index < 10 || !userRecordIterator.hasNext()) {
                Map.Entry<Integer, Integer> userRecord = userRecordIterator.next();
                Text parsedOutput = new Text(userRecord.getKey() + SEPARATOR + userRecord.getValue());
                context.write(NullWritable.get(), parsedOutput);

                index++;
            }
        }
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
        public void reduce(NullWritable key, Iterable<Text> userRecordsRaw, Context context) {
            try {
                int index = 0;
                Iterator<Text> userRecordsRawIterator = userRecordsRaw.iterator();

                // Just in case multiple mappers output to this reducer (not in this case)
                while (index < 10 && userRecordsRawIterator.hasNext()) {
                    Text userRecordRaw = userRecordsRawIterator.next();
                    String[] userRecordParsed = userRecordRaw.toString().split(SEPARATOR);

                    String topTenTableColumnName = ("row" + (9 - index));
                    Put topTenTableColumn = new Put(topTenTableColumnName.getBytes());
                    topTenTableColumn.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("id"), Bytes.toBytes(userRecordParsed[0]));
                    topTenTableColumn.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rep"), Bytes.toBytes(userRecordParsed[1]));
                    context.write(NullWritable.get(), topTenTableColumn);

                    index++;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "Top Ten");
        job.setJarByClass(TopTen.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(TopTenMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // Only one reducer
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableReducerJob(args[1], TopTenReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}