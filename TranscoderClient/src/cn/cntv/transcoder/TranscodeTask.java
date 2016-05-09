package cn.cntv.transcoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class TranscodeTask implements Callable<String> {
	private String inputPath;
	private String originFileName; // original file name, may contain special characters.
	private String procesfileName; // currently processing file name.
	private String outputPath;
	private String indexPath;
	private String splitPath;
	private String transPath;
	private String parameter;
	private String output_filename;
	private String outformat;
	private static int taskcount = 0;
	private final int taskid = taskcount++;
	private static ReentrantLock local_tx_lock = new ReentrantLock();
	private static ReentrantLock local_rx_lock = new ReentrantLock();
	private static ReentrantLock hadoop_lock = new ReentrantLock();

	synchronized private void print(String msg) {
		System.out.print(msg);
	}
	
	synchronized private void println(String msg) {
		System.out.println(msg);
	}

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

	public static String getLocalIP() {
		StringBuilder sb = new StringBuilder();
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while (en.hasMoreElements()) {
				NetworkInterface intf = (NetworkInterface) en.nextElement();
				Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
				while (enumIpAddr.hasMoreElements()) {
					InetAddress inetAddress = (InetAddress) enumIpAddr.nextElement();
					if (!inetAddress.isLoopbackAddress() && !inetAddress.isLinkLocalAddress()
							&& inetAddress.isSiteLocalAddress()) {
						sb.append(inetAddress.getHostAddress().toString() + "\n");
					}
				}
			}
		} catch (SocketException e) {

		}
		return sb.toString();
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

	public int callexec(Runtime rt, String command) {
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

	public TranscodeTask(String inputPath, String fileName, String outputPath, String indexPath, String splitPath,
			String transPath, String parameter, String output_filename, String outformat) {
		this.inputPath = inputPath;
		this.originFileName = fileName;
		String filetype = fileName.substring(fileName.lastIndexOf("."), fileName.length());
		this.procesfileName = "Transcoding_" + TranscodeTask.getLocalIP().trim() + "_" + taskid + filetype;
		this.outputPath = outputPath;
		this.indexPath = indexPath;
		this.splitPath = splitPath;
		this.transPath = transPath;
		this.parameter = parameter;
		this.output_filename = output_filename;
		this.outformat = outformat;
	}

	public String call() {
		int code = -1;
		boolean flag = false;
		try {
			flag = TranscodeTask.renameFile(this.inputPath, this.originFileName, this.procesfileName);
			if (flag == false)
				return this.originFileName; // fail,can not rename the original video file.
			code = transcode();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			flag = TranscodeTask.renameFile(this.inputPath, this.procesfileName, this.originFileName);
			if (flag == false)
				return this.originFileName; // fail,can not rename back the video file in input path
		}

		return code == 0?"":this.originFileName;
	}

	private boolean clearLocalPath() {
		clearDir(new File(indexPath));
		clearDir(new File(splitPath));
		return true;
	}
	
	private boolean clearCluster() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /transcode/" + procesfileName;
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Success")); //print success what ever
		return true;
	}

	private boolean prepareCluster() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /transcode/" + procesfileName;
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Success")); //print success what ever

		command = hadoop + "fs -mkdir -p " + "/transcode/" + procesfileName + "/split";
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		command = hadoop + "fs -mkdir -p " + "/transcode/" + procesfileName + "/index";
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		return true;
	}

	private boolean splitVideo() throws IOException, InterruptedException {
		String fileFullName = inputPath + procesfileName;
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = "mkvmerge -o " + splitPath + procesfileName + ".split%04d.mp4 --split " + ParaParser.getSplitSize() + fileFullName;
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
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
			outTxt.println("file " + splitname + "@" + this.parameter + "&" + this.outformat);
			outTxt.close();
		}
		return splitList;
	}

	private boolean copyToCluster(String[] splitList) throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		for (String splitname : splitList) {
			// copy the index videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + indexPath + splitname + ".idx" + " /transcode/"
					+ procesfileName + "/index";
			
			exit = callexec(rt, command);
			print("TaskID:" + this.taskid + " " + command);
			println(": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;

			// copy the splits videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + splitPath + splitname + " /transcode/" + procesfileName
					+ "/split";
			
			exit = callexec(rt, command);
			print("TaskID:" + this.taskid + " " + command);
			println(": " + (exit == 0 ? "Success" : "Fail"));
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

		command = hadoop + "fs -rm -r /transcode/" + procesfileName + "/trans";
		exit = callexec(rt, command);

		// step 06: start transcode, and waiting for its completion.
		command = hadoop + "jar /home/bin/tc.jar TranscoderMR /transcode/" + procesfileName + "/index" + " /transcode/"
				+ procesfileName + "/trans";
		
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		return exit == 0;
	}

	/**
	 * Transcode a video file.
	 * 
	 * @return
	 */
	private int transcode() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String[] splitList = null;
		Runtime rt = Runtime.getRuntime();

		local_tx_lock.lock();
		try {
			boolean flag = false;

			try {
				FileOutputStream outTxt = new FileOutputStream(this.outputPath + "log.txt", true);
				String newline = this.procesfileName + " " + this.originFileName + "\n";
				outTxt.write(newline.getBytes());
				outTxt.close();
			} catch (FileNotFoundException e) {
				return 2; // can not open and write the log.txt file.
			}

			// clear index and split directory.
			flag = clearLocalPath();
			if (flag == false)
				return 3; // can not clear the local temporary path.

			// prepare work directory on hadoop cluster.
			flag = prepareCluster();
			if (flag == false)
				return 4; // can not create working directory on hadoop

			// use mkvmerge to split the video file.
			flag = splitVideo();
			if (flag == false)
				return 5; // can not split video

			// scan the splits videos and generate the index files.
			splitList = stepGenerateIdx();

			// copy the index and video files to hadoop cluster
			flag = copyToCluster(splitList);
			if (flag == false)
				return 6; // can not copy files to hadoop

		} finally {
			local_tx_lock.unlock();
		}

		hadoop_lock.lock();
		try {
			boolean flag = false;
			flag = stepTranscode();
			if (flag == false)
				return 7; // transcoding process on hadoop fails
		} finally {
			hadoop_lock.unlock();
		}
		

		local_rx_lock.lock();
		try {
			// copy the trans videos to client machine
			boolean flag = false;
			flag = copyToClient(hadoop, splitList, rt);
			if (flag == false)
				return 8; // can not copy files to local client

			// scan the trans path to generate the out.ffconcat.
			generateConcat();
			
			// assemble all the splits with ffmpeg
			flag = stepAssembleVideo(rt);
			if (flag == false)
				return 9; // can not assemble video

			String output_filename = this.output_filename + this.outformat;
			flag = TranscodeTask.renameFile(this.outputPath, this.procesfileName + this.outformat, output_filename);
			if (flag == false)
				return 10; // can not rename the video file in output path
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			clearCluster();
			local_rx_lock.unlock();
		}
		return 0;
	}

	/**
	 * @param rt
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean stepAssembleVideo(Runtime rt) throws IOException, InterruptedException {
		String command;
		String ffmpeg = "/opt/ffmpeg/ffmpeg-git-20160409-64bit-static/ffmpeg ";
		int exit;
		if (this.outformat.intern() == ".mp4".intern()) {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy -bsf:a aac_adtstoasc "
				    + outputPath + this.procesfileName + this.outformat;
		} else if (this.outformat.intern() == ".ts".intern()) {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy "
					+ outputPath + this.procesfileName + this.outformat;
		} else {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy "
					+ outputPath + this.procesfileName + this.outformat;
		}
		
		exit = callexec(rt, command);
		print("TaskID:" + this.taskid + " " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		return true;
	}

	/**
	 * @throws FileNotFoundException
	 */
	private void generateConcat() throws FileNotFoundException {
		File transVideoPath = new File(transPath);
		PrintWriter outTxt = new PrintWriter(transPath + "out.ffconcat");
		String[] transList = transVideoPath.list(filter(".*\\.(mp4|ts)"));
		Arrays.sort(transList);
		for (String filename : transList) {
			outTxt.println("file " + filename);
		}
		outTxt.close();
	}

	private boolean copyToClient(String hadoop, String[] splitList, Runtime rt)
			throws IOException, InterruptedException {
		String command;
		int exit;
		clearDir(new File(transPath));
		for (String splitname : splitList) {
			command = hadoop + "fs -copyToLocal /transcode/" + procesfileName + "/trans/" + splitname + this.outformat + " " + transPath;
			
			exit = callexec(rt, command);
			print("TaskID:" + this.taskid + " " + command);
			println(": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;
		}

		return true;
	}
}
