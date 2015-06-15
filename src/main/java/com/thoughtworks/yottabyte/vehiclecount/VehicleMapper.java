package com.thoughtworks.yottabyte.vehiclecount;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VehicleMapper extends Mapper<Object, Text, Text, IntWritable> {

  public static final String COLUMN_SEPARATOR = "COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";
  private Configuration configuration;
  private static final IntWritable one = new IntWritable(1);

  @Override
  public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
	  String columnSeparator = ",";
	  String vehicleType = row.toString().split(columnSeparator)[0];
	  
	  context.write(new Text(vehicleType), one);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

  protected String get(String key) {
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }
}






