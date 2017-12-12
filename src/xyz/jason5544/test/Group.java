package xyz.jason5544.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator {
	public Group()
	{
		super(KeyPair.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b)
	{
		KeyPair kp1 = (KeyPair) a;
		KeyPair kp2 = (KeyPair) b;
		return Integer.compare(kp1.getYear(), kp2.getYear());
	}

}
