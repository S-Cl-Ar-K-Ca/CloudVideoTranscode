package cn.cntv.transcoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;
import java.io.*;

public class TranscodeClient {
	public static boolean renameFile(String path, String oldname, String newname) {
		if (!oldname.equals(newname)) {// 新的文件名和以前文件名不同时,才有必要进行重命名
			File oldfile = new File(path + oldname);
			File newfile = new File(path + newname);
			if (!oldfile.exists()) {
				return false; // 重命名文件不存在
			}
			if (newfile.exists())// 若在该目录下已经有一个文件和新文件名相同，则不允许重命名
				return false;
			else {
				oldfile.renameTo(newfile);
			}
		} else {
			return false;
		}
		return true;
	}

	public static String replaceBlank(String str) {
		String dest = "";
		if (str != null) {
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(str);
			dest = m.replaceAll("");
		}
		return dest;
	}

	public static void makeDir(File dir) {
		if (!dir.getParentFile().exists()) {
			makeDir(dir.getParentFile());
		}
		dir.mkdir();
	}

	public static FilenameFilter filter(final String regex) {
		return new FilenameFilter() {
			private Pattern pattern = Pattern.compile(regex);

			public boolean accept(File dir, String name) {
				return pattern.matcher(name).matches();
			}
		};
	}

	public static void main(String[] args) {
		// Specify the input directory that include videos to be transcoded.
		String input = args[0];
		// Specify the output directory that contains the transcoding result.
		String output = args[1];
		// Create the temp directory that contains the index txt.
		String index = output + ".temp/index/";
		makeDir(new File(index));
		// Specify the temp directory that contains the video splits.
		String splits = output + ".temp/splits/";
		makeDir(new File(splits));
		// Specify the temp directory that contains the transcode video splits.
		String trans = output + ".temp/transc/";
		makeDir(new File(trans));

		File recd = new File(output + "log.txt");
		if (recd.exists())
			recd.delete();
		
		ArrayList<Future<String>> failList = new ArrayList<Future<String>>();

		for (int loop = 0;; loop++) {
			ParaParser.parser(new File(args[2]));
			
			// Scan the input directory to find out all video files.
			File inputFilePath = new File(input);
			String[] inputList = inputFilePath.list(filter(".*\\.(mp4|ts|MP4|TS)"));
			// rename the file whose filename has blanks
			for (String fileName : inputList) {
				String newFileName = TranscodeClient.replaceBlank(fileName);
				TranscodeClient.renameFile(input, fileName, newFileName);
			}
			inputList = inputFilePath.list(filter(".*\\.(mp4|ts|MP4|TS)"));

			// Check the output directory to find out all successful transcode
			// videos in order to skip them.
			File ouputFilePath = new File(output);
			String[] outputList = ouputFilePath.list(filter(".*\\.(mp4|ts|MP4|TS)"));

			List<String> taskFileList = new ArrayList<String>();
			List<String> outputFileList = Arrays.asList(outputList);
			List<String> failFileList = new ArrayList<String>();
			for (Future<String> failFileName : failList) {
				try{
					failFileList.add(failFileName.get());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			for (int i = 0; i < outputFileList.size(); ++i) {
				outputFileList.set(i,outputFileList.get(i).replaceAll("[0-9|a-z]{32}", "{UUID}"));
			}
			
			for (String fileName : inputList) {
				String input_filename = fileName.substring(0, fileName.lastIndexOf("."));
				String output_filename = ParaParser.getOutputFilename();
				output_filename = output_filename.replaceAll("\\{original_filename\\}", input_filename);
				output_filename = output_filename + ParaParser.getFileoutFormat();
				
				if (outputFileList.contains(output_filename)) {
					if (loop == 0) {
						System.out.println(output_filename + " exists! check output path!");
					}
				} else if (failFileList.contains(fileName)) {
					System.out.println(fileName + " transcode fails!");
				} else {
					taskFileList.add(fileName);
				}
			}

			// If the taskFileList is empty, then all task is done, if not there
			// are new submitted video files. The client should re-scan the
			// input and output directory to find out new submitted files.
			if (taskFileList.isEmpty())
				break;

			// Initialize the thread pool.
			ExecutorService es = Executors.newFixedThreadPool(2);

			// Transcode videos
			for (String fileName : taskFileList) {
				String parameter = ParaParser.getParameter();
				String output_format = ParaParser.getFileoutFormat();
				String output_filename = ParaParser.getOutputFilename();
				
				String input_filename = fileName.substring(0, fileName.lastIndexOf("."));
				output_filename = output_filename.replaceAll("\\{original_filename\\}", input_filename);
				output_filename = output_filename.replaceAll("\\{UUID\\}", UUID.randomUUID().toString().replaceAll("-", ""));
				
				failList.add(es.submit(new TranscodeTask(input, fileName, output, index, splits, trans, parameter, output_filename, output_format)));
			}
			es.shutdown();
			try {
				es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
