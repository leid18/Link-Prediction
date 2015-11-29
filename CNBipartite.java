
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
 *This Hadoop Map Reduce code is to do the Common Neighbours Bipartite link prediction. It will calculate score of the Jaccard similarity between nodes from a dataset
 *score(x,y) = (common friends of node x and node y) / (total friends of node x and node y)
 *This code is written based on Ben Cole and Jacob Bank's Jaccard Map Reduce code and the WordCount code in the Map Reduce tutorial in the Hadoop website.
 */

public class CNBipartite extends Configured implements Tool {
    
    private static Set<CPair> ts = new TreeSet<CPair>();
    
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
    
    public static class PassOneReduce extends MapReduceBase implements Reducer<Text, Text, CPair, Text>{
        /**
         *This reduce fuction is to pair the keys and values from the output of the map fuction and count all values that linked to one key.
         *The output of this fuction will be node x, all linked nodes y and the count of these nodes y.
         *The file of the output will be in the "cnb1" folder.
         */
        
        public void reduce(Text key, Iterator<Text> valueList, OutputCollector<CPair, Text> output, Reporter reporter)
        throws IOException{
            Set<Text> s = new TreeSet<Text>();
            while (valueList.hasNext()){
                Text temp = valueList.next();
                s.add(new Text(temp));
            }
            for (Text t : s){
                String tempValue = t.toString();
                int tempInt = Integer.parseInt(tempValue);
                Text newOutput = new Text(tempValue);
                CPair newKey = new CPair(key.toString(), tempInt);
                ts.add(newKey);
                output.collect(newKey, newOutput);
            }
           /** Set<Text> s = new TreeSet<Text>(); // Use a treeset to store the values
            while(valueList.hasNext()){
                Text temp = valueList.next();
                //Text diff = new Text(temp); // Return the different values that linked to one key
                s.add(new Text(temp)); // Add the values into the treeset
            }
            for (Text t : s) {
                String nodeY = t.toString();
                int neighboursY = s.size();
                CPair outputPair = new CPair(nodeY, neighboursY); // Output value will be the pair of different nodes and the count of these nodes
                output.collect (key, outputPair);
            }*/
        }
    }
    
    
    public static class PassTwoMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        /**
         *This map fuction is to switch the keys and values.
         *The node y will be outputed as the key. The fuction will collect all nodes x that linked to a node y and the count of these nodes x.
         */
        
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException{
            String nodes = value.toString();
            StringTokenizer st= new StringTokenizer(nodes);
            String x = st.nextToken();
            String count1 = st.nextToken();
            String count2 = st.nextToken();
            Text outputKey= new Text(count2); // Use node y as output key
            Text newValue = new Text(x);
            output.collect(outputKey, newValue);
        }
    }
    
    public static class PassTwoReduce extends MapReduceBase implements Reducer<Text, Text, Text,  Text>{
        
        /**
         *This reduce fuction is to split up the input into small size.
         *The output will be an empty key with the pair of nodes x and the count
         *The output file will be in the "cnb2" folder
         */
        
        public void reduce(Text key, Iterator<Text> valueList, OutputCollector<Text, Text> output, Reporter reporter)
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
                    outputKey.set(key);
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
    
    public static class PassThreeMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        
        /**
         *The map fuction is to create pairs of node x and node y with the first node in the input value list and each of the following nodes.
         *The output key will be the pairs of node x and node y. The output value will be the total count of neighbours of node x and node y.
         */
        public void map(LongWritable key, Text valueList, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            String values = valueList.toString();
            Scanner sc = new Scanner(values); // Scan the value list
            ArrayList<String> list = new ArrayList<String>();
            String commonNeighbour = null;
            while(sc.hasNext()){
                commonNeighbour = Integer.toString(sc.nextInt()); //Get common neighbour
                if(sc.hasNext()){
                    int temp = sc.nextInt();
                    list.add(Integer.toString(temp));
                }
            }
            Text newKey = new Text();
            for (int i = 1; i < list.size(); i++){
                String x = list.get(0);
                String y = list.get(i);
                if(x.compareTo(y) > 0)
                {
                    newKey.set(x + "         " + y);
                }
                else
                {
                    newKey.set(y + "         " + x);
                }
                Text newValue = new Text (commonNeighbour);
                output.collect(newKey, newValue);
            }
            
        }
    }
    
    public static class PassThreeReduce extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable>{
        /**
         *The reduce function is to calculate the final result of the Common Neighbours Bipartite algorithm.
         */
        public void reduce(Text key, Iterator<Text> valueList, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException{
            float count = 0;
            ArrayList<String> al = new ArrayList<String>();
            while (valueList.hasNext()){
                String n = valueList.next().toString();
                al.add(n);
                count++;
            }
            
            StringTokenizer st = new StringTokenizer(key.toString());
            String x = st.nextToken();
            String y = st.nextToken();
            Set <CPair> newPair = new TreeSet<CPair>();
            for(CPair p : ts){
                for (String c : al){
                        if(p.getX().equals(y)){
                            newPair.add(p);
                            if(p.getCount() == Integer.parseInt(c)){
                                newPair.remove(p);
                            }
                            
                        }
                }
            }
            for (CPair np : newPair){
                int newY = np.getCount();
                Text newKey = new Text(x + "   " + newY);
                
                FloatWritable newValue = new FloatWritable(count);
                output.collect(newKey,newValue);
            }
            //float totalCounts = sumCounts - count;
            //float z = (float) count;
            //FloatWritable newValue = new FloatWritable(count);
            //output.collect(key,newValue);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CNBipartite(), args);
        System.exit(res);
	   }
    
    public int run(String[] args) throws Exception{
        pass1();
        pass2();
        pass3();
        return 0;
    }
    
    public void pass1() throws Exception{
        JobConf conf= new JobConf(CNBipartite.class);
        conf.setJobName("Common Neighbours Bipartite Step1");
        
        conf.setMapperClass(PassOneMap.class);
        conf.setReducerClass(PassOneReduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf, new Path("CNBinput"));
        FileOutputFormat.setOutputPath(conf, new Path("cnb1"));
        
        JobClient.runJob(conf);
    }
    
    public void pass2() throws Exception{
        JobConf conf2= new JobConf(CNBipartite.class);
        conf2.setJobName("Common Neighbours Bipartite Step2");
        
        conf2.setMapperClass(PassTwoMap.class);
        conf2.setReducerClass(PassTwoReduce.class);
        
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf2, new Path("cnb1"));
        FileOutputFormat.setOutputPath(conf2, new Path("cnb2"));
        
        JobClient.runJob(conf2);
    }
    
    public void pass3() throws Exception{
        JobConf conf3= new JobConf(CNBipartite.class);
        conf3.setJobName("Common Neighbours Bipartite Step3");
        
        conf3.setMapperClass(PassThreeMap.class);
        conf3.setReducerClass(PassThreeReduce.class);
        
        conf3.setOutputKeyClass(Text.class);
        conf3.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(conf3, new Path("cnb2"));
        FileOutputFormat.setOutputPath(conf3, new Path("CNBoutput"));
        
        JobClient.runJob(conf3);
    }
    
}

