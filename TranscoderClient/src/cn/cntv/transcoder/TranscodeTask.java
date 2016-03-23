package cn.cntv.transcoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class TranscodeTask implements Callable<Boolean> {
	private String inputPath;
	private String fileName;
	private String outputPath;
	private String indexPath;
	private String splitPath;
	private String transPath;
	private String parameter;
	private static int taskcount = 0;
	private final int taskid = taskcount++;
	private static ReentrantLock local_tx_lock = new ReentrantLock();
	private static ReentrantLock local_rx_lock = new ReentrantLock();
	private static ReentrantLock hadoop_lock = new ReentrantLock();

	synchronized private void println(String msg) {
		System.out.println(msg);
	}

	/**
	 * @param dir
	 *            The directory that need to be deleted.
	 * @return boolean Returns "true" if all deletions were successful.
	 */
	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		return dir.delete();
	}

	private static boolean clearDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return true;
	}

	public static FilenameFilter filter(final String regex) {
		return new FilenameFilter() {
			private Pattern pattern = Pattern.compile(regex);

			public boolean accept(File dir, String name) {
				return pattern.matcher(name).matches();
			}
		};
	}

	public int callexec(Runtime rt, String command) throws IOException, InterruptedException {
		Process process = rt.exec(command);
		BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String message = null;
		while ((message = br.readLine()) != null) {
			println(message);
		}
		br.close();
		return process.waitFor();
	}

	public TranscodeTask(String inputPath, String fileName, String outputPath, String indexPath, String splitPath,
			String transPath, String parameter) {
		this.inputPath = inputPath;
		this.fileName = fileName;
		this.outputPath = outputPath;
		this.indexPath = indexPath;
		this.splitPath = splitPath;
		this.transPath = transPath;
		this.parameter = parameter;
	}

	public Boolean call() {
		boolean result = false;
		try {
			result = transcode();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {

		}
		return result;
	}

	private boolean stepClearLocalPath() {
		clearDir(new File(indexPath));
		clearDir(new File(splitPath));
		return true;
	}

	private boolean stepPrepareCluster() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /transcode/" + fileName;
		exit = callexec(rt, command);
		
		command = hadoop + "fs -mkdir -p " + "/transcode/" + fileName + "/split";
		exit = callexec(rt, command);
		println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		command = hadoop + "fs -mkdir -p " + "/transcode/" + fileName + "/index";
		exit = callexec(rt, command);
		println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		return true;
	}

	private boolean stepSplitVideo() throws IOException, InterruptedException {
		String fileFullName = inputPath + fileName;
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = "mkvmerge -o " + splitPath + fileName + ".split%04d.mp4 --split 100m " + fileFullName;
		exit = callexec(rt, command);
		println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		return true;
	}

	private String[] stepGenerateIdx() throws FileNotFoundException {
		String[] splitList = null;
		File splitVideoPath = new File(splitPath);
		splitList = splitVideoPath.list(filter(".*\\.(mp4|xxx)"));
		Arrays.sort(splitList);
		for (String splitname : splitList) {
			PrintWriter outTxt = new PrintWriter(indexPath + splitname + ".idx");
			outTxt.println("file " + splitname + "@" + parameter);
			outTxt.close();
		}
		return splitList;
	}

	private boolean stepCopyToCluster(String[] splitList) throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		for (String splitname : splitList) {
			// copy the index videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + indexPath + splitname + ".idx" + " /transcode/" + fileName
					+ "/index";
			exit = callexec(rt, command);
			println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;

			// copy the splits videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + splitPath + splitname + " /transcode/" + fileName + "/split";
			exit = callexec(rt, command);
			println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;
		}

		return true;
	}

	private boolean stepTranscode() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /transcode/" + fileName + "/trans";
		exit = callexec(rt, command);
		// println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));

		// step 06: start transcode, and waiting for its completion.
		command = hadoop + "jar /home/bin/tc.jar TranscoderMR /transcode/" + fileName + "/index" + " /transcode/"
				+ fileName + "/trans";
		exit = callexec(rt, command);
		println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		return exit == 0;
	}

	/**
	 * Transcode a video file.
	 * 
	 * @return
	 */
	private boolean transcode() throws IOException, InterruptedException {
		//String fileFullName = inputPath + fileName;
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		//String command = null;
		String[] splitList = null;
		Runtime rt = Runtime.getRuntime();

		local_tx_lock.lock();
		try {
			// step 00: clear index and split directory.
			boolean step00 = stepClearLocalPath();
			if (step00 == false)
				return false;
			// step 01: prepare work directory on hadoop cluster.
			boolean step01 = stepPrepareCluster();
			if (step01 == false)
				return false;
			// step 02: use mkvmerge to split the video file.
			boolean step02 = stepSplitVideo();
			if (step02 == false)
				return false;
			// step 03: scan the splits videos and generate the index files.
			splitList = stepGenerateIdx();
			// step 04: copy the index and video files to hadoop cluster index
			// directory
			boolean step04 = stepCopyToCluster(splitList);
			if (step04 == false)
				return false;

		} finally {
			local_tx_lock.unlock();
		}

		hadoop_lock.lock();
		try {
			int retry = 3;
			boolean step05 = false;
			for (int i = 0; i <= retry; ++i) {
				step05 = stepTranscode();
				if (step05)
					break;
			}
			if (step05 == false)
				return false;
		} finally {
			hadoop_lock.unlock();
		}

		local_rx_lock.lock();
		try {
			// step 07: copy the trans videos to client machine
			boolean step07 = stepCopyToClient(hadoop, splitList, rt);
			if (step07 == false)
				return false;
			// step 08: scan the tempTransVideoPath path to generate the
			// out.ffconcat file.
			stepGenerateConcat();
			// step 09: assemble all the splits with ffmpeg and put it to the
			// output directory.
			boolean step09 = stepAssembleVideo(rt);
			if (step09 == false)
				return false;
		} finally {
			local_rx_lock.unlock();
		}
		return true;
	}

	/**
	 * @param rt
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean stepAssembleVideo(Runtime rt) throws IOException, InterruptedException {
		String command;
		int exit;
		command = "ffmpeg -f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy -bsf:a aac_adtstoasc "
				+ outputPath + fileName;
		exit = callexec(rt, command);
		println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		return true;
	}

	/**
	 * @throws FileNotFoundException
	 */
	private void stepGenerateConcat() throws FileNotFoundException {
		File transVideoPath = new File(transPath);
		PrintWriter outTxt = new PrintWriter(transPath + "out.ffconcat");
		String[] transList = transVideoPath.list(filter(".*\\.(mp4|xxx)"));
		Arrays.sort(transList);
		for (String filename : transList) {
			outTxt.println("file " + filename);
		}
		outTxt.close();
	}

	private boolean stepCopyToClient(String hadoop, String[] splitList, Runtime rt)
			throws IOException, InterruptedException {
		String command;
		int exit;
		clearDir(new File(transPath));
		for (String splitname : splitList) {
			command = hadoop + "fs -copyToLocal /transcode/" + fileName + "/trans/" + splitname + " " + transPath;
			exit = callexec(rt, command);
			println("TaskID:" + this.taskid + " " + command + ": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;
		}

		return true;
	}
}
