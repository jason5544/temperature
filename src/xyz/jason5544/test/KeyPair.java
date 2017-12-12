package xyz.jason5544.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyPair implements WritableComparable<KeyPair>{
	private int year;
	private int tp;
	
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.tp = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(tp);
	}

	public int compareTo(KeyPair obj) {
		int res = Integer.compare(year, obj.getYear());
		if (res != 0)
		{
			return res;
		}
		return Integer.compare(tp, obj.getTp());
	}
	
	public String toString()
	{
		return year+"\t"+tp;
	}
	
	public int hashCode()
	{
		return new Integer(year+tp).hashCode();
	}

	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getTp() {
		return tp;
	}
	public void setTp(int tp) {
		this.tp = tp;
	}

}
