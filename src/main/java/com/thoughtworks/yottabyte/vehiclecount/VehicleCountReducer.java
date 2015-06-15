package com.thoughtworks.yottabyte.vehiclecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VehicleCountReducer extends Reducer<Text,IntWritable,NullWritable,Text> {
  @Override
  protected void reduce(Text vehicleType, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
	  int vehicleCount = 0;
	  
	  for (IntWritable value : values) {
		  vehicleCount += value.get();
	  }
	  
	  String resultDescription = String.format("%s was found %d times", vehicleType, vehicleCount);
	  context.write(NullWritable.get(), new Text(resultDescription));
  }
}









