
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 *This Hadoop Map Reduce code is to do the Jaccard Coefficient link prediction. It will calculate score of the Jaccard similarity between nodes from a dataset
 *score(x,y) = (common friends of node x and node y) / (total friends of node x and node y)
 *This code is written based on Ben Cole and Jacob Bank's Jaccard Map Reduce code and the WordCount code in the Map Reduce tutorial in the Hadoop website.
 */

public class JCMapRed extends Configured implements Tool {
    
    public static class PassOneMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

        /**
         *This map fuction is to read the file from the input folder.
         *It takes in the key and value from the input file unchanged.
         *The outputKey represents the node x in the input file, while the outputValue represents the node y in the input file
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException{
            String nodes = value.toString();
            StringTokenizer st= new StringTokenizer(nodes);
            Text outputKey= new Text(st.nextToken());
            Text outputValue= new Text(st.nextToken());
            output.collect(outputKey, outputValue);
        }
    }

    public static class PassOneReduce extends MapReduceBase implements Reducer<Text, Text, Text, PairNodes>{
        /**
         *This reduce fuction is to pair the keys and values from the output of the map fuction and count all values that linked to one key.
         *The output of this fuction will be node x, all linked nodes y and the count of these nodes y.
         *The file of the output will be in the "jc1" folder.
         */

        public void reduce(Text key, Iterator<Text> valueList, OutputCollector<Text, PairNodes> output, Reporter reporter)
        throws IOException{
            Set<Text> s = new TreeSet<Text>(); // Use a treeset to store the values
            while(valueList.hasNext()){
                Text temp = valueList.next();
                //Text diff = new Text(temp); // Return the different values that linked to one key
                s.add(new Text(temp)); // Add the values into the treeset
            }
            for (Text t : s) {
                String nodeY = t.toString();
                int neighboursY = s.size();
                PairNodes outputPair = new PairNodes(nodeY, neighboursY); // Output value will be the pair of different nodes and the count of these nodes
                output.collect (key, outputPair);
            }
        }
    }
    
    
    public static class PassTwoMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, PairNodes>{
        /**
         *This map fuction is to switch the keys and values.
         *The node y will be outputed as the key. The fuction will collect all nodes x that linked to a node y and the count of these nodes x.
         */

        public void map(LongWritable key, Text value, OutputCollector<Text, PairNodes> output, Reporter reporter)
        throws IOException{
            String nodes = value.toString();
            StringTokenizer st= new StringTokenizer(nodes);
            String x = st.nextToken();
            String y = st.nextToken();
            String count = st.nextToken();
            Text outputKey= new Text(y); // Use node y as output key
            PairNodes outputPair= new PairNodes(x, Integer.parseInt(count));// Use the pair of node x and the count as output value
            output.collect(outputKey, outputPair);
        }
    }
    
    public static class PassTwoReduce extends MapReduceBase implements Reducer<Text, PairNodes, Text,  Text>{
        
        /**
         *This reduce fuction is to split up the input into small size.
         *The output will be an empty key with the pair of nodes x and the count
         *The output file will be in the "jc2" folder
         */

        public void reduce(Text key, Iterator<PairNodes> valueList, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException{
            Text outputKey = new Text();
            Text outputValue = new Text();
            final int c = 500;
            int counter = 0;
            Vector<String> chunks = new Vector<String>();
            while(valueList.hasNext())
            {
                String temp = valueList.next().toString();
                for(String chunk: chunks){
                    outputKey.set("");
                    outputValue.set(temp + " " + chunk);
                    output.collect(outputKey, outputValue);
                }
                if(counter % c == 0)
                {
                    chunks.add(temp);
                }
                else
                {
                    chunks.set(chunks.size()-1, temp + " " + chunks.get(chunks.size()-1));
                    //counter++;
                }
                counter++;
            }
        }
    }

    public static class PassThreeMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

        /**
         *The map fuction is to create pairs of node x and node y with the first node in the input value list and each of the following nodes.
         *The output key will be the pairs of node x and node y. The output value will be the total count of neighbours of node x and node y.
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
            String values = value.toString();
            Scanner sc = new Scanner(values); // Scan the value list
            ArrayList<PairNodes> pairList = new ArrayList<PairNodes>();
            while(sc.hasNext()){
                String x = sc.next(); //Get node x
                if(sc.hasNext()){
                    int c = sc.nextInt(); // Get the count of node x
                    PairNodes newPair = new PairNodes(x,c);
                    pairList.add(newPair); // Pair node x with it's count and add it to the pair list
                }
            }
            Text newKey = new Text();
            for (int i = 1; i < pairList.size(); i++){
                String x = pairList.get(0).getX();
                String y = pairList.get(i).getX();
                if(x.compareTo(y) > 0)
                {
                    newKey.set(x + "         " + y);
                }
                else
                {
                    newKey.set(y + "         " + x);
                }
                int countX = pairList.get(0).getInfo();
                int countY = pairList.get(i).getInfo();
                IntWritable newValue = new IntWritable (countX + countY);
                output.collect(newKey, newValue);
            }
            
        }
    }
    
    public static class PassThreeReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable>{
        /**
         *The reduce function is to calculate the final result of the Jaccard Coefficient algorithm.
         */
        public void reduce(Text key, Iterator<IntWritable> valueList, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException{
            float count = 0;
            float sumCounts = 0;
            while (valueList.hasNext()){
                sumCounts = (float)valueList.next().get();
                count++;
            }
            float totalCounts = sumCounts - count;
            FloatWritable newValue = new FloatWritable(count/totalCounts);
            output.collect(key,newValue);
        }
        
    }
    
    public static class PassFourMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
            String lines = value.toString();
            StringTokenizer st= new StringTokenizer(lines);
            String x = st.nextToken();
            String y = st.nextToken();
            String score = st.nextToken();
            String newKey = x + "         " + y;
            Text outputKey= new Text(score);
            Text outputValue= new Text(newKey);
            output.collect(outputKey, outputValue);
        }
    
    }
    
    public static class PassFourReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterator<Text> valueList, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
            while (valueList.hasNext()){
                Text newKey = key;
                Text value = valueList.next();
                output.collect(newKey,value);
            }
        }
        
    }
    
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JCMapRed(), args);
        System.exit(res);
	   }
    
    public int run(String[] args) throws Exception{
        pass1();
        pass2();
        pass3();
        pass4();
        return 0;
    }
    
    public void pass1() throws Exception{
        JobConf conf= new JobConf(JCMapRed.class);
        conf.setJobName("Jaccard Coefficient Step1");
        
        conf.setMapperClass(PassOneMap.class);
        conf.setReducerClass(PassOneReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(conf, new Path("input"));
        FileOutputFormat.setOutputPath(conf, new Path("jc1"));
        
        JobClient.runJob(conf);
    }
    
    public void pass2() throws Exception{
        JobConf conf2= new JobConf(JCMapRed.class);
        conf2.setJobName("Jaccard Coefficient Step2");
        
        conf2.setMapperClass(PassTwoMap.class);
        conf2.setReducerClass(PassTwoReduce.class);
        
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(PairNodes.class);
        
        FileInputFormat.setInputPaths(conf2, new Path("jc1"));
        FileOutputFormat.setOutputPath(conf2, new Path("jc2"));
        
        JobClient.runJob(conf2);
    }
    
    public void pass3() throws Exception{
        JobConf conf3= new JobConf(JCMapRed.class);
        conf3.setJobName("Jaccard Coefficient Step3");
        
        conf3.setMapperClass(PassThreeMap.class);
        conf3.setReducerClass(PassThreeReduce.class);
        
        conf3.setOutputKeyClass(Text.class);
        conf3.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(conf3, new Path("jc2"));
        FileOutputFormat.setOutputPath(conf3, new Path("JCoutput"));

        JobClient.runJob(conf3);
    }
    
    public void pass4() throws Exception{
        JobConf conf4= new JobConf(JCMapRed.class);
        conf4.setJobName("Jaccard Coefficient Step4");
        
        conf4.setMapperClass(PassFourMap.class);
        conf4.setReducerClass(PassFourReduce.class);
        
        //conf4.setInputFormat(TextInputFormat.class);
        //conf4.setOutputFormat(TextOutputFormat.class);
        conf4.setOutputKeyClass(Text.class);
        conf4.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf4, new Path("JCoutput"));
        FileOutputFormat.setOutputPath(conf4, new Path("JCoutput2"));
        
        JobClient.runJob(conf4);
    }
    
}

