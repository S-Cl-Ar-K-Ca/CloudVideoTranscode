import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TranscoderMR {
	public static class TranscodeMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {
		// Create a path
		public static void makeDir(File dir) {
			if (!dir.getParentFile().exists()) {
				makeDir(dir.getParentFile());
			}
			dir.mkdir();
		}
		
		/**
		 * 删除单个文件
		 * 
		 * @param sPath
		 *            被删除文件的文件名
		 * @return 单个文件删除成功返回true，否则返回false
		 */
		public static boolean deleteFile(String sPath) {
			boolean flag = false;
			File file = new File(sPath);
			// 路径为文件且不为空则进行删除
			if (file.isFile() && file.exists()) {
				file.delete();
				flag = true;
			}
			return flag;
		}

		public static int callexec(Runtime rt, String command) {
			Process process = null;
			int result = -1;
			try {
				process = rt.exec(command);
				//启用StreamGobbler线程清理错误流和输入流 防止IO阻塞
				new StreamGobbler(process.getErrorStream(),"ERROR").start();
				new StreamGobbler(process.getInputStream(),"INPUT").start();
				result = process.waitFor();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				if(process!=null&&result!=0){
					process.destroy();
				}
			}
			
			return result;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
			String ffmpeg = "/opt/ffmpeg/ffmpeg-git-20160409-64bit-static/ffmpeg ";
			String line = value.toString();
			if (line.length() <= 0)
				return;

			String splitName = line.substring(0, line.lastIndexOf("@"));
			String parameter = line.substring(line.lastIndexOf("@") + 1, line.lastIndexOf("&"));
			String outformat = line.substring(line.lastIndexOf("&") + 1, line.lastIndexOf("$"));
			String username  = line.substring(line.lastIndexOf("$") + 1, line.length());

			System.out.println("start to transcode " + splitName);

			String fileName = splitName.substring(0, splitName.lastIndexOf(".split"));
			String splitPath = "/" + username + "/" + fileName + "/split/";
			String transPath = "/" + username + "/" + fileName + "/trans/";
			Runtime rt = Runtime.getRuntime();
			String command = null;
			String localSplitPath = "/home/" + username + "/split/";
			String localTransPath = "/home/" + username + "/trans/";
			TranscodeMapper.makeDir(new File(localSplitPath));
			TranscodeMapper.makeDir(new File(localTransPath));
			
			int exit = 0;

			try {
				// step 00: delete the possible file in split path. 
				TranscodeMapper.deleteFile(localSplitPath + splitName);
				
				// step 01: copy a split file to local datanode
				command = hadoop + "fs -copyToLocal " + splitPath + splitName + " " + localSplitPath;
				exit = callexec(rt, command);
				System.out.println(command + ": " + (exit == 0 ? "Success" : "Fail"));

				// step 02: transcode the video
				command = ffmpeg + "-y -i" + " " + localSplitPath + splitName + " " + parameter + " " + localTransPath + splitName + outformat;
				System.out.print(command);
				exit = callexec(rt, command);
				System.out.println(": " + (exit == 0 ? "Success" : "Fail"));

				// step 03: copy the local file back to hdfs
				command = hadoop + "fs -copyFromLocal -f " + localTransPath + splitName + outformat + " " + transPath;
				exit = callexec(rt, command);
				System.out.println(command + ": " + (exit == 0 ? "Success" : "Fail"));
			} finally {
				// step 04: delete the local file
				TranscodeMapper.deleteFile(localSplitPath + splitName);
				TranscodeMapper.deleteFile(localTransPath + splitName + outformat);
			}
		}
	}

	public static class TranscodeReducer extends Reducer<Text, BooleanWritable, Text, BooleanWritable> {
		private BooleanWritable result = new BooleanWritable(true);

		public void reduce(Text key, Iterable<BooleanWritable> values, Context context)
				throws IOException, InterruptedException {
			for (BooleanWritable val : values) {
				if (val.get() == false) {
					result.set(false);
					break;
				}
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "transcoder");
		job.setJarByClass(TranscoderMR.class);
		job.setMapperClass(TranscodeMapper.class);
		job.setCombinerClass(TranscodeReducer.class);
		job.setReducerClass(TranscodeReducer.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BooleanWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}