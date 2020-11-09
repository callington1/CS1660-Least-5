import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class Least5Mapper extends Mapper<Object, 
                            Text, Text, LongWritable> { 
  
    private TreeMap<Long, String> tmap; 
  
    @Override
    public void setup(Context context) throws IOException, 
                                     InterruptedException 
    { 
        tmap = new TreeMap<Long, String>(); 
    } 
  
    @Override
    public void map(Object key, Text value, 
       Context context) throws IOException,  
                      InterruptedException 
    { 
  
        // input data format => movie_name     
        // no_of_views  (tab seperated) 
        // we split the input data 
        String[] tokens = value.toString().split("\t"); 
  
        String word = tokens[0]; 
        long count = Long.parseLong(tokens[1]); 
  
        // insert data into treeMap, 
        // we want top 10  viewed movies 
        // so we pass no_of_views as key 
        tmap.put(count, word); 
  
        // we remove the last key-value 
        // if it's size increases 5
        if (tmap.size() > 5) 
        { 
            tmap.remove(tmap.lastKey()); 
        } 
    } 
  
    @Override
    public void cleanup(Context context) throws IOException, 
                                       InterruptedException 
    { 
        for (Map.Entry<Long, String> entry : tmap.entrySet())  
        { 
  
            long count = entry.getKey(); 
            String name = entry.getValue(); 
  
            context.write(new Text(name), new LongWritable(count)); 
        } 
    } 
}