package cn.cntv.transcoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        if(!dir.getParentFile().exists()) {  
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
		// Specify the temp directory that contains the transcoded video splits.
		String trans = output + ".temp/transc/";
		makeDir(new File(trans));

		while (true) {
			// Scan the input directory to find out all video files.
			File inputFilePath = new File(input);
			String[] inputList = inputFilePath.list(filter(".*\\.(mp4|xxx)"));
			// rename the file whose filename has blanks
			for (String fileName : inputList) {
				String newFileName = TranscodeClient.replaceBlank(fileName);
				TranscodeClient.renameFile(input,fileName,newFileName);
			}
			inputList = inputFilePath.list(filter(".*\\.(mp4|xxx)"));

			// Check the output directory to find out all successful transcode
			// videos in order to skip them.
			File ouputFilePath = new File(output);
			String[] ouputList = ouputFilePath.list(filter(".*\\.(mp4|xxx)"));

			List<String> taskFileList = new ArrayList<String>();
			List<String> outputFileList = Arrays.asList(ouputList);
			for (String fileName : inputList) {
				if (!outputFileList.contains(fileName)) {
					taskFileList.add(fileName);
				}
			}

			// If the taskFileList is empty, then all task is done, if not there
			// are new submitted video files. The client should re-scan the
			// input and output directory to find out new submitted files.
			if (taskFileList.isEmpty())
				break;

			// Initialize the thread pool.
			ExecutorService es = Executors.newFixedThreadPool(5);
					
			// Transcode videos one by one
			for (String fileName : taskFileList) {
				String parameter = ParaParser.parser(new File(args[2]));
				es.submit(new TranscodeTask(input, fileName, output, index, splits, trans, parameter));
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
