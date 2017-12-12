package xyz.jason5544.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortTp extends WritableComparator {
	
	public SortTp()
	{
		super(KeyPair.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b)
	{
		KeyPair kp1 = (KeyPair) a;
		KeyPair kp2 = (KeyPair) b;
		
		int res = Integer.compare(kp1.getYear(), kp2.getYear());
		
		if (res != 0)
		{
			return res;
		}
		
		return -Integer.compare(kp1.getTp(), kp2.getTp());
	}


}
