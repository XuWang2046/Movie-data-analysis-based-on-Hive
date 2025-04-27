import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBeans implements WritableComparable<FlowBeans> {
    private long id;
    private double price;

    public FlowBeans(){

    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     *  重写比较器方法
     */
    @Override
    public int compareTo(FlowBeans o) {
        // 首先比较id 升序
        if(this.id > o.id){
            return 1;
        }else if(this.id < o.id){
            return -1;
        }else {
            // 当id 相同的时候 比较price 降序
            return this.price > o.price ? -1:1;
        }
    }
    /**
     * 序列化
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.id);
        dataOutput.writeDouble(this.price);
    }

    /**
     *  反序列化
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.id +"\t"+ this.price;
    }
}
