
import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.hadoop.io.NullWritable;

public class LogProcess {
	
	
	private static Pattern record_pattern = Pattern.compile("\\[(.*)\\]\\[([A-Za-z0-9_]*)\\](.*)");
	private static Pattern int_pattern = Pattern.compile("^\\d+$");
	private static Pattern float_pattern = Pattern.compile("^\\d+\\.\\d+$");
	private static JSONObject TypeObject = new JSONObject(TypeDefinition.TypeFile);
	
	public static class ParsingLogMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text word = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			word.set(parse_record(value.toString()));
			context.write(word, NullWritable.get());
		}
	}

	public static class ParsingLogReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
	}

	public static String parse_record(String record_str) {
		Matcher m = record_pattern.matcher(record_str);
		if (!m.find() || m.groupCount()!=3)
			return "";
		String log_time = m.group(1);
		String log_type = m.group(2);
		JSONObject log_content = new JSONObject(m.group(3));
		Set<String> attr_list = log_content.keySet();
		JSONObject log_format = (JSONObject) TypeObject.get(log_type);
		JSONObject result = new JSONObject();
		result.put("LogTime", log_time);
		result.put("LogType", log_type);
		for (String attr: log_format.keySet()){
			String attr_type = (String) ((JSONArray) log_format.get(attr)).get(0);
			// NUMBER type attribute
			if (attr_type.equals("NUMBER")) {
				boolean isAdded = false;
				if (attr_list.contains(attr)) {
					Object val = log_content.get(attr);
					if (val instanceof Integer || val instanceof Float) {
						result.put(attr, val);
						isAdded = true;
					}
					else if (val instanceof String) {
						if (int_pattern.matcher((String) val).find()) {
							result.put(attr, Integer.parseInt((String) val));
							isAdded = true;
						}
						else if (float_pattern.matcher((String) val).find()) {
							result.put(attr, Float.parseFloat((String) val));
							isAdded = true;
						}
					}
				}
				if (!isAdded) {
					result.put(attr, -1);
				}
			}
			// String type attribute
			else if (attr_type.equals("STRING")) {
				boolean isAdded = false;
				if (attr_list.contains(attr)) {
					Object val = log_content.get(attr);
					if (val instanceof String) {
						result.put(attr, val);
						isAdded = true;
					}
					else if (val instanceof JSONArray) {
						JSONArray old_val = (JSONArray) val;
						String new_val = "";
						for (int i=0; i<old_val.length(); i++) {
							if (new_val.length() > 0)
								new_val += " ";
							new_val += old_val.get(i);
						}
						result.put(attr, new_val);
						isAdded = true;
					}
				}
				if (!isAdded) {
					result.put(attr, "");
				}
			}
			if (log_content.keySet().contains(attr)) {
				log_content.remove(attr);
			}
		}
		// Appending abundant information
		String other_info = "";
		if (log_content.length() > 0) {
			for (String attr : log_content.keySet()) {
				Object val = log_content.get(attr);
				if ( other_info.length() > 0 )
					other_info += ", ";
				if (val instanceof JSONArray) {
					other_info += attr + ":";
					JSONArray val_array = (JSONArray) val;
					for (int i=0; i<val_array.length(); i++) {
						other_info += val_array.get(i).toString() + " ";
					}
					other_info = other_info.trim();
				}
				else {
					other_info += attr +":" + val.toString();
				}
			}
		}
		result.put("LogOtherInfo", other_info);
		return result.toString();
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);		// compressing intermediate output
		Job job = Job.getInstance(conf, "log processing");
		job.setJarByClass(LogProcess.class);
	    job.setMapperClass(ParsingLogMapper.class);
	    job.setCombinerClass(ParsingLogReducer.class);
	    job.setReducerClass(ParsingLogReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileOutputFormat.setCompressOutput(job, true);			//compressing job output
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
