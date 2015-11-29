import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PairNodes implements WritableComparable {
    
    private String nodeID;
    private int info;
    
    // required methods
    public PairNodes(){
        nodeID = "";
        info = 0;
    }
    
    public static PairNodes genc(StringTokenizer st){
        return new PairNodes(st.nextToken(), Integer.parseInt(st.nextToken()));
    }
    
    public PairNodes(String a, int b){
        nodeID = a;
        info = b;
    }
    
    public String getX(){
        return nodeID; }
    
    public int getInfo(){
        return info;
    }

    public String toString(){
        return nodeID + " " + info;
    }
    
    public void write(DataOutput out) throws IOException{
        out.writeChars(nodeID + "\n");
        out.writeInt(info);
    }
    
    public void readFields(DataInput in) throws IOException{
        nodeID = in.readLine();
        info = in.readInt();
    }
    
    public int hashCode() {
        return nodeID.hashCode()*127 + new Integer(info).hashCode();
    }
    
    public int compareTo(Object q){
        PairNodes p = (PairNodes)q;
        return getX().compareTo(p.getX());
    }
    
    public boolean equals(PairNodes p){
        return compareTo(p)==0;
    }
    
    
}
