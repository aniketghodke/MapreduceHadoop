import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FlightData {
	public static class FlightMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String txt = value.toString();
			// Declaring the variables to be used for sending required data
			// Constats
			/*
			 * DEFINING CONSTANTS STARTS
			 */
			int monthBegin = 06;
			int yearBegin = 2007;
			int monthEnd = 05;
			int yearEnd = 2008;

			String departAirport = "ORD";
			String arrivalAirport = "JFK";
			/*
			 * DEFINING CONSTANTS ENDS
			 */
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat timeFormat = new SimpleDateFormat("HHmm");
			// Gives easy way to check if date1 > date2
			Calendar cal = Calendar.getInstance();
			// Getting the data
			String[] contents = txt.split(",");
			// Flight date and time
			Date flightDate = null;
			Date depTime = null;
			Date arrTime = null;
			float ArrDelayMins = (float) -0.1;
			try {
				// Flight date
				flightDate = dateFormat.parse(contents[5]);
				// Setting it to calander since dateFormat does not give proper
				// output.
				cal.setTime(flightDate);
				// Departure time
				contents[26] = contents[26].replace("\"", "");
				if (!contents[26].equals(""))
					depTime = timeFormat.parse(contents[26]);
				// Arrival time
				contents[37] = contents[37].replace("\"", "");
				if (!contents[37].equals(""))
					arrTime = timeFormat.parse(contents[37]);
			} catch (Exception e) {
				e.printStackTrace();
			}

			// cancelled = 1.0 indicates flight cancelled
			float cancelled = Float.parseFloat(contents[43]);

			// diverted = 1.0 indicates flight delayed
			float diverted = Float.parseFloat(contents[45]);

			// Flight departure airport
			String flightDepAirport = contents[11];
			flightDepAirport = flightDepAirport.replace("\"", "");

			// Flight arrival airport
			String flightArrAirport = contents[18];
			flightArrAirport = flightArrAirport.replace("\"", "");

			// Flight delay mins
			if (!contents[39].equals(""))
				ArrDelayMins = Float.parseFloat(contents[39]);

			// Getting the actual data
			// Checking for begin year and begin month > required time or begin
			// year and begin month < requried timeline
			if ((cal.get(Calendar.YEAR) == yearBegin && cal.get(Calendar.MONTH) + 1 >= monthBegin)
					|| (cal.get(Calendar.YEAR) == yearEnd && cal
							.get(Calendar.MONTH) + 1 <= monthEnd)) {
				
				// Flights eligible for F1 or F2
				boolean flight1 = (flightDepAirport.equals(departAirport) && (!flightArrAirport
						.equals(arrivalAirport)));
				boolean flight2 = (!flightDepAirport.equals(departAirport) && (flightArrAirport
						.equals(arrivalAirport)));

				// Check if the flight1 is diverted or cancelled
				if ((flight1) && (cancelled == 0.00 && diverted == 0.00)
						&& (arrTime != null && ArrDelayMins != -0.1)) {
					// Sending the data for join
					// key should be conditions for equi join i.e.
					// FlightArrAirport and date
					// value should be other values required for
					// computing i.e.
					// a flag indicating the flight is flight F1, arrival
					// time and arrival delay mins
					String outputKey = flightArrAirport.replace("\"", "") + ";"
							+ contents[5];
					String outputValue = "F1;" + contents[37] + ";"
							+ ArrDelayMins;
					context.write(new Text(outputKey), new Text(outputValue));
				}
				if ((flight2) && (cancelled == 0.00 && diverted == 0.00)
						&& (depTime != null && ArrDelayMins != -0.1)) { 
					// Flights eligible for F2
					// Check if the flight is diverted or cancelled
					// Sending the data for join
					// Key should be departure airport, date these will
					// be equated with values sent by F1
					// Value should be a flag F2, dep time and arrival delay mins
					String outputKey = flightDepAirport.replace("\"", "") + ";"
							+ contents[5];
					String outputValue = "F2;" + contents[26] + ";"
							+ ArrDelayMins;
					context.write(new Text(outputKey), new Text(outputValue));
				}
			}
		}
	}

	// Defing reducer1
	public static class FlightReducer extends Reducer<Text, Text, Text, Text> {

		double sum = (double) 0.0;
		double count = 0;
		String sum_str = new String();
		String count_str = new String();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrTime = new ArrayList<String>();
			ArrayList<String> depTime = new ArrayList<String>();
			ArrayList<Double> delayMins = new ArrayList<Double>();
			ArrayList<Boolean> flags = new ArrayList<Boolean>();
			// ArrayList<String> flightDate_arr = new ArrayList<String>();
			// ArrayList<String> flightDate_dep = new ArrayList<String>();
			SimpleDateFormat timeFormat = new SimpleDateFormat("HHmm");
			for (Text v : values) {
				String Value = v.toString();
				String[] contents = Value.split(";");
				// Adding the data to lists
				if (contents[0].equals("F1")) {
					arrTime.add(contents[1]);
					depTime.add("");
					delayMins.add(Double.parseDouble(contents[2]));
					flags.add(true);
				}
				if (contents[0].equals("F2")) {
					arrTime.add("");
					depTime.add(contents[1]);
					delayMins.add(Double.parseDouble(contents[2]));
					flags.add(false);
				}
			}
			Date flightArrTime = null;
			Date flightDepTime = null;
			int j;
			// Iterating over the list of all the elements i.e. flight f1
			for (int i = 0; i < arrTime.size(); i++) {
				// if the flight is F1 then only we have to calculate the
				// data
				if (flags.get(i)) {
					try {
						flightArrTime = timeFormat.parse(arrTime.get(i));
					} catch (Exception e) {
						e.printStackTrace();
					}
					// Iterating over all the list again to get the values i.e.
					// flight f2
					for (j = 0; j < arrTime.size(); j++) {
						if (!flags.get(j)) {
							try {
								flightDepTime = timeFormat
										.parse(depTime.get(j));
							} catch (Exception e) {
								e.printStackTrace();
							}
							// If the dep time of f2 is more than arrival time
							// of f1
							if (flightDepTime.after(flightArrTime)) {
								sum += delayMins.get(i) + delayMins.get(j);
								count += 1;
							}
						}
					}
				}
			}
			// Just converting to strig to get output in text format
			count_str = count + "";
			sum_str = sum + "";
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text(count_str), new Text(sum_str));
		}
	}

	public static class FlightMapper2 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text("Dummy"), value);
		}
	}

	public static class FlightReducer2 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		double sum = 0;
		double count = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				String value = val.toString();
				String[] data = value.split("\t");
				count += Double.parseDouble(data[0]);
				sum += Double.parseDouble(data[1]);
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text("Average: "),
					new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: FlightData <in> <out>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "FlightData");
		job1.setJarByClass(FlightData.class);
		job1.setNumReduceTasks(10);
		job1.setMapperClass(FlightMapper.class);
		job1.setReducerClass(FlightReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		job1.waitForCompletion(true);
		// New job waits for the old one to finish
		Job job2 = new Job(conf, "FlightData");
		job2.setJarByClass(FlightData.class);
		job2.setNumReduceTasks(1);
		job2.setMapOutputValueClass(Text.class);
		job2.setMapperClass(FlightMapper2.class);
		job2.setReducerClass(FlightReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
