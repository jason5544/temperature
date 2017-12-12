package xyz.jason5544.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartition extends Partitioner <KeyPair, Text> {

	public int getPartition(KeyPair key, Text value, int num) {
		return (key.getYear()*127) % num;
	}

}
