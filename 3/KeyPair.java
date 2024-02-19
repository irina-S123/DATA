import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class KeyPair implements WritableComparable<KeyPair>{
        private int year;
        private int temp;

    public void setYear(int year) {
        this.year = year;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public int getYear() {
        return year;
    }

    public int getTemp() {
        return temp;
    }
    @Override
    public int compareTo(KeyPair o) {
                int result=Integer.compare(year,o.getYear());
        if(result != 0){
                        return 0;
        }
                return Integer.compare(temp,o.getTemp());
    }

    @Override
    
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeInt(year);
       dataOutput.writeInt(temp);
    }

    @Override
    
    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readInt();
        this.temp=dataInput.readInt();
    }

    @Override
    public String toString() {
        return year+"\t"+temp;
    }

    @Override
    public int hashCode() {
        return new Integer(year+temp).hashCode();
    }
}