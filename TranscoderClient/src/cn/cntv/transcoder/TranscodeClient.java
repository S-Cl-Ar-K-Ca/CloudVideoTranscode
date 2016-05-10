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
import java.lang.Thread;

enum TRANSCODECLIENT_STATUS {
	START("START",1), TRANSCODING("TRANSCODING",2), WAITING("WAITING",3);
	// 成员变量
    private String name;
    private int index;

    // 构造方法
    private TRANSCODECLIENT_STATUS(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
    
    public String getName() {
        return name;
    }
}

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
		if (args.length != 4) {
			System.out.println("Not enought input parameters, 4 parameters expected!");
			System.exit(0);
		}
			
		// Specify the input directory that include videos to be transcoded.
		String input_path = args[0];
		// Specify the output directory that contains the transcoding result.
		String output_path = args[1];
		// //Read the input parameters
		ParaParser.parser(new File(args[2])); 
		// Create the temp directory that contains the index txt.
		String index = output_path + ".temp/index/";
		// Specify the username
		String username = args[3];
		
		makeDir(new File(index));
		// Specify the temp directory that contains the video splits.
		String splits = output_path + ".temp/splits/";
		makeDir(new File(splits));
		// Specify the temp directory that contains the transcode video splits.
		String trans = output_path + ".temp/transc/";
		makeDir(new File(trans));

		File recd = new File(output_path + "log.txt");
		if (recd.exists())
			recd.delete();
		
		File inputFilePath = new File(input_path);
		File ouputFilePath = new File(output_path);
		String[] inputList = null;
		String[] outputList = null;
		List<String> inputFileList = new ArrayList<String>();
		List<String> outputFileList = new ArrayList<String>();
		List<String> taskFileList = new ArrayList<String>();
		ArrayList<Future<String>> failTaskList = new ArrayList<Future<String>>();
		List<String> failFileList = new ArrayList<String>();
		String regex_video_file_filter = ".*\\.(mp4|MP4|ts|TS)";
		boolean flag = true;
		
		TRANSCODECLIENT_STATUS status = TRANSCODECLIENT_STATUS.START;
		
		while (true) {
			ParaParser.parser(new File(args[2])); //Read the input parameters
			
			switch(status) {
			case START:
				// Scan the input directory to find out all video files.
				inputList = inputFilePath.list(filter(regex_video_file_filter));
				// rename the file whose filename has blanks
				for (String fileName : inputList) {
					String newFileName = TranscodeClient.replaceBlank(fileName);
					TranscodeClient.renameFile(input_path, fileName, newFileName);
				}
				inputList = inputFilePath.list(filter(regex_video_file_filter));
				
				// Check the output directory to find out all successful transcoded videos in order to skip them.
				outputList = ouputFilePath.list(filter(regex_video_file_filter));
				outputFileList = Arrays.asList(outputList);
				
				// add the fail task's filename to the fail file list.
				for (Future<String> failFileName : failTaskList) {
					try{
						failFileList.add(failFileName.get());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				// if the output file name contains uuid, then replace them with "{UUID}"
				for (int i = 0; i < outputFileList.size(); ++i) {
					outputFileList.set(i,outputFileList.get(i).replaceAll("[0-9|a-z]{32}", "{UUID}"));
				}
				
				// check that whether the output path already contains the corresponding file.
				for (String fileName : inputList) {
					String input_filename = fileName.substring(0, fileName.lastIndexOf("."));
					String output_filename = ParaParser.getOutputFilename();
					output_filename = output_filename.replaceAll("\\{original_filename\\}", input_filename);
					output_filename = output_filename + ParaParser.getFileoutFormat();
					
					if (outputFileList.contains(output_filename)) {
						//System.out.println(output_filename + " exists! check output path!");
					} else if (failFileList.contains(fileName)) {
						//System.out.println(fileName + " transcode fails!");
					} else {
						taskFileList.add(fileName);
					}
				}
				
				status = TRANSCODECLIENT_STATUS.TRANSCODING;
				break;
				
			case TRANSCODING:
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
					
					failTaskList.add(es.submit(new TranscodeTask(input_path, fileName, output_path, index, splits, trans, parameter, output_filename, output_format)));
				}
				es.shutdown();
				try {
					es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				taskFileList.clear();
				status = TRANSCODECLIENT_STATUS.WAITING;
				flag = true;
				break;
				
			case WAITING:
				// Scan the input directory to find out all video files.
				inputList = inputFilePath.list(filter(regex_video_file_filter));
				// rename the file whose filename has blanks
				for (String fileName : inputList) {
					String newFileName = TranscodeClient.replaceBlank(fileName);
					TranscodeClient.renameFile(input_path, fileName, newFileName);
				}
				inputList = inputFilePath.list(filter(regex_video_file_filter));
				inputFileList = Arrays.asList(inputList);
				
				// Check the output directory to find out all successful transcoded videos in order to skip them.
				outputList = ouputFilePath.list(filter(regex_video_file_filter));
				outputFileList = Arrays.asList(outputList);
				
				// add the fail task's filename to the fail file list.
				for (int i = 0; i < failTaskList.size();) {
					try{
						if (inputFileList.contains(failTaskList.get(i).get())) { // if the failed video file still in the input path, do nothing.
							++i;
						} else { // if the failed video file is removed from the input path, then forget it.
							failTaskList.remove(i);
							continue;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				failFileList.clear();
				for (Future<String> failFileName : failTaskList) {
					try{
						failFileList.add(failFileName.get());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				// if the output file name contains uuid, then replace them with "{UUID}"
				for (int i = 0; i < outputFileList.size(); ++i) {
					outputFileList.set(i,outputFileList.get(i).replaceAll("[0-9|a-z]{32}", "{UUID}"));
				}
				
				// check that whether the output path already contains the corresponding file.
				for (String fileName : inputList) {
					String input_filename = fileName.substring(0, fileName.lastIndexOf("."));
					String output_filename = ParaParser.getOutputFilename();
					output_filename = output_filename.replaceAll("\\{original_filename\\}", input_filename);
					output_filename = output_filename + ParaParser.getFileoutFormat();
					
					if (outputFileList.contains(output_filename)) {
						if (flag == true) {
							System.out.println(output_filename + " exists! check output path!");
						}
					} else if (failFileList.contains(fileName)) {
						if (flag == true) {
							System.out.println(fileName + " transcode fails!");
						}
					} else {
						taskFileList.add(fileName);
					}
				}
				
				if (taskFileList.isEmpty()) {
					status = TRANSCODECLIENT_STATUS.WAITING;
					flag = false;
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					status = TRANSCODECLIENT_STATUS.TRANSCODING;
				}
				
				break;
			}
		}
	}
}
