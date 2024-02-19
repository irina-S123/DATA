import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class SortTemp extends WritableComparator{

    public SortTemp() {
        super(KeyPair.class,true);
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
       
        KeyPair o1=(KeyPair)a;
        KeyPair o2=(KeyPair)b;
        int result=Integer.compare(o1.getYear(),o2.getYear());
        
        if(result != 0){
            return result;
        }
        
        return -Integer.compare(o1.getTemp(),o2.getTemp());
    }
}
