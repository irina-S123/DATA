import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class GroupTemp extends WritableComparator{

    public GroupTemp() {
        super(KeyPair.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        
        KeyPair o1=(KeyPair)a;
        KeyPair o2=(KeyPair)b;
        return Integer.compare(o1.getYear(),o2.getYear());
    }
}
