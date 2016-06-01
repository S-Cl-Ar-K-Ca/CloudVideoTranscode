package cn.cntv.transcoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

enum TRANSCODE_ERROR_CODE {
	SUCCESS("SUCCESS",0),
	OPEN_LOG_FILE_FAIL("OPEN_LOG_FILE_FAIL",1), 
	CLEAR_LOCAL_TEMP_PATH_FAIL("CLEAR_LOCAL_TEMP_PATH_FAIL",2), 
	CREATE_WORK_PATH_ON_HADOOP_FAIL("CREATE_WORK_PATH_ON_HADOOP_FAIL",3),
	SPLIT_VIDEO_FAIL("SPLIT_VIDEO_FAIL",4),
	COPY_FILE_TO_HADOOP_FAIL("COPY_FILE_TO_HADOOP_FAIL",5),
	TRANSCODE_ON_HADOOP_FAIL("TRANSCODE_ON_HADOOP_FAIL",6),
	COPY_FILE_TO_LOCAL_FAIL("COPY_FILE_TO_LOCAL_FAIL",7),
	ASSEMBLE_VIDEO_FAIL("ASSEMBLE_VIDEO_FAIL",8),
	RENAME_OUTPUT_FILE_FAIL("RENAME_OUTPUT_FILE_FAIL",9),
	ENABLE_DTS_FAIL("ENABLE_DTS_FAIL",10),
	MOVE_TO_OUTPUT_PATH_FAIL("MOVE_TO_OUTPUT_PATH_FAIL",11);
	
	// 成员变量
    private String name;
    private int index;

    // 构造方法
    private TRANSCODE_ERROR_CODE(String name, int index) {
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

public class TranscodeTask implements Callable<String> {
	private String inputPath;
	private String originFileName; // original file name, may contain special characters.
	private String procesfileName; // currently processing file name.
	private String outputPath;
	private String indexPath;
	private String splitPath;
	private String transPath;
	private String dtshd_path;
	private String parameter;
	private String output_filename;
	private String outformat;
	private String username;
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
	
	public int callexec(Runtime rt, String[] command) {
		return this.callexec(rt, command, null); 
	}
	
	public int callexec(Runtime rt, String[] command, OutputStream outputStream) {
		Process process = null;
		int result = -1;
		try {
			process = rt.exec(command);
			//启用StreamGobbler线程清理错误流和输入流 防止IO阻塞
			new StreamGobbler(process.getErrorStream(),"ERROR", outputStream).start();
			new StreamGobbler(process.getInputStream(),"INPUT", outputStream).start();
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
	
	public int callexec(Runtime rt, String command, OutputStream outputStream) {
		Process process = null;
		int result = -1;
		try {
			process = rt.exec(command);
			//启用StreamGobbler线程清理错误流和输入流 防止IO阻塞
			new StreamGobbler(process.getErrorStream(),"ERROR", outputStream).start();
			new StreamGobbler(process.getInputStream(),"INPUT", outputStream).start();
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

	public int callexec(Runtime rt, String command) {
		return this.callexec(rt, command, null);
	}

	public TranscodeTask(String inputPath, String fileName, String outputPath, String indexPath, String splitPath,
			String transPath, String dtshd_path, String parameter, String output_filename, String outformat, String username) {
		this.inputPath = inputPath;
		this.originFileName = fileName;
		String filetype = fileName.substring(fileName.lastIndexOf("."), fileName.length());
		this.procesfileName = "Transcoding_" + TranscodeTask.getLocalIP().trim() + "_" + taskid + filetype;
		this.outputPath = outputPath;
		this.indexPath = indexPath;
		this.splitPath = splitPath;
		this.transPath = transPath;
		this.dtshd_path = dtshd_path;
		this.parameter = parameter;
		this.output_filename = output_filename;
		this.outformat = outformat;
		this.username = username;
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

	private boolean clear_local_path() {
		clearDir(new File(indexPath));
		clearDir(new File(splitPath));
		return true;
	}
	
	private boolean clear_cluster() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /" + this.username + "/" + procesfileName;
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Success")); //print success what ever
		return true;
	}

	private boolean prepare_cluster() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /" + this.username + "/" + procesfileName;
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Success")); //print success what ever

		command = hadoop + "fs -mkdir -p /" + this.username + "/" + procesfileName + "/split";
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		command = hadoop + "fs -mkdir -p /" + this.username + "/" + procesfileName + "/index";
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		return true;
	}

	private boolean split_video() throws IOException, InterruptedException {
		String fileFullName = this.inputPath + this.procesfileName;
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;
		
		String file_type = this.procesfileName.substring(this.procesfileName.lastIndexOf("."),this.procesfileName.length());
		
		command = "mkvmerge -o " + this.splitPath + this.procesfileName + ".split%04d" + file_type + " --split " + ParaParser.getSplitSize() + fileFullName;
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;

		return true;
	}

	private String[] generate_idx() throws FileNotFoundException {
		String[] splitList = null;
		File splitVideoPath = new File(splitPath);
		splitList = splitVideoPath.list(filter(".*\\.(mp4|ts|m2ts)"));
		Arrays.sort(splitList);
		for (String splitname : splitList) {
			PrintWriter outTxt = new PrintWriter(indexPath + splitname + ".idx");
			outTxt.println(splitname + "@" + this.parameter + "&" + this.outformat + "$" + this.username);
			outTxt.close();
		}
		return splitList;
	}

	private boolean copy_to_cluster(String[] splitList) throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		for (String splitname : splitList) {
			// copy the index videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + indexPath + splitname + ".idx /" + this.username + "/" + procesfileName + "/index";
			
			exit = callexec(rt, command);
			//print("TaskID=" + this.taskid + ": " + command);
			//println(": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;

			// copy the splits videos to hadoop cluster
			command = hadoop + "fs -copyFromLocal -f " + splitPath + splitname + " /" + this.username + "/" + procesfileName + "/split";
			
			exit = callexec(rt, command);
			print("TaskID=" + this.taskid + ": " + command);
			println(": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;
		}

		return true;
	}

	private boolean transcode_cloud() throws IOException, InterruptedException {
		String hadoop = "/opt/hadoop/hadoop-2.7.1/bin/hadoop ";
		String command = null;
		Runtime rt = Runtime.getRuntime();
		int exit = 0;

		command = hadoop + "fs -rm -r /" + this.username + "/" + procesfileName + "/trans";
		exit = callexec(rt, command);

		// step 06: start transcode, and waiting for its completion.
		command = hadoop + "jar /home/bin/tc.jar TranscoderMR /" + this.username + "/" + procesfileName + "/index /" + this.username + "/" + procesfileName + "/trans";
		
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
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
		String command = null;
		int exit;
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
				return TRANSCODE_ERROR_CODE.OPEN_LOG_FILE_FAIL.getIndex(); // can not open and write the log.txt file.
			}

			// clear index and split directory.
			flag = clear_local_path();
			if (flag == false) {
				return TRANSCODE_ERROR_CODE.CLEAR_LOCAL_TEMP_PATH_FAIL.getIndex(); // can not clear the local temporary path.
			}

			// prepare work directory on hadoop cluster.
			flag = prepare_cluster();
			if (flag == false) {
				return TRANSCODE_ERROR_CODE.CREATE_WORK_PATH_ON_HADOOP_FAIL.getIndex(); // can not create working directory on hadoop
			}

			// use mkvmerge to split the video file.
			flag = split_video();
			if (flag == false) {
				return TRANSCODE_ERROR_CODE.SPLIT_VIDEO_FAIL.getIndex(); // can not split video
			}

			// scan the splits videos and generate the index files.
			splitList = generate_idx();

			// copy the index and video files to hadoop cluster
			flag = copy_to_cluster(splitList);
			if (flag == false) {
				clear_cluster();
				return TRANSCODE_ERROR_CODE.COPY_FILE_TO_HADOOP_FAIL.getIndex(); // can not copy files to hadoop
			}

		} finally {
			local_tx_lock.unlock();
		}

		hadoop_lock.lock();
		try {
			boolean flag = false;
			flag = transcode_cloud();
			if (flag == false) {
				clear_cluster();
				return TRANSCODE_ERROR_CODE.TRANSCODE_ON_HADOOP_FAIL.getIndex(); // transcoding process on hadoop fails
			}
		} finally {
			hadoop_lock.unlock();
		}
		
		local_rx_lock.lock();
		try {
			// copy the trans videos to client machine
			boolean flag = false;
			flag = copy_to_client(hadoop, splitList, rt);
			if (flag == false) {
				return TRANSCODE_ERROR_CODE.COPY_FILE_TO_LOCAL_FAIL.getIndex(); // can not copy files to local client
			}

			// scan the trans path to generate the out.ffconcat.
			generate_concat();
			
			// assemble all the splits with ffmpeg
			flag = assemble_video(rt);
			if (flag == false) {
				return TRANSCODE_ERROR_CODE.ASSEMBLE_VIDEO_FAIL.getIndex(); // can not assemble video
			}
			
			// enable DTS audio if needed 
			if (ParaParser.getAudioDTSEnabled()) {
				flag = enable_audio_dts(rt);
				if (flag == false) {
					return TRANSCODE_ERROR_CODE.ENABLE_DTS_FAIL.getIndex(); // enable dts fail
				}
			} else {
				// do nothing
			}

			// move the final result file to the output path and rename it
			String output_filename = this.output_filename + this.outformat;
			
			String move_filename1 = this.transPath + this.procesfileName + this.outformat;
			
			command = "mv " + move_filename1 + " " + this.outputPath + output_filename;
			exit = callexec(rt,command);
			println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0) {
				return TRANSCODE_ERROR_CODE.RENAME_OUTPUT_FILE_FAIL.getIndex();
			}
			
			if (ParaParser.getAudioDTSEnabled()) {
				String move_filename2 = this.dtshd_path + this.procesfileName.substring(0,this.procesfileName.lastIndexOf(".")) + "_DTS.mp4";
				String move_filename3 = this.dtshd_path + this.procesfileName.substring(0,this.procesfileName.lastIndexOf(".")) + "_DTS.mp4.ts";
				
				command = "mv " + move_filename2 + " " + this.outputPath + this.output_filename + "_DTS.mp4";
				exit = callexec(rt,command);
				println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
				if (exit != 0) {
					return TRANSCODE_ERROR_CODE.MOVE_TO_OUTPUT_PATH_FAIL.getIndex();
				}
			
				command = "mv " + move_filename3 + " " + this.outputPath + this.output_filename + "_DTS.ts";
				exit = callexec(rt,command);
				println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
				if (exit != 0) {
					return TRANSCODE_ERROR_CODE.MOVE_TO_OUTPUT_PATH_FAIL.getIndex();
				}
			}
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			clear_cluster();
			local_rx_lock.unlock();
		}
		return TRANSCODE_ERROR_CODE.SUCCESS.getIndex();
	}
	
	private boolean enable_audio_dts(Runtime rt) {
		clearDir(new File(this.dtshd_path));
		
		String command = null;
		String ffmpeg = "/opt/ffmpeg/ffmpeg-git-20160409-64bit-static/ffmpeg ";
		String python = "python3 ";
		int exit;
		
		// extract the audio wave file to dtshd path
		command = ffmpeg + "-y -i " + this.inputPath + this.procesfileName + "-vn -sn -ar 48k -acodec pcm_s24le " + this.dtshd_path + " " + ParaParser.getAudioTrackSelect() + this.procesfileName.substring(0, this.procesfileName.lastIndexOf(".")) + ".wav";
		exit = callexec(rt,command);
		println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		
		// copy the video file to dtshd path 
		command = ffmpeg + "-y -i " + this.transPath + this.procesfileName + this.outformat + " -c:v copy -c:a copy " + this.dtshd_path + this.procesfileName.substring(0, this.procesfileName.lastIndexOf(".")) + ".mp4";
		exit = callexec(rt,command);
		println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		
		// call the python script to add DTS track
		command = python + "/opt/dts/DTSEncode.py " + this.dtshd_path + this.procesfileName.substring(0, this.procesfileName.lastIndexOf(".")) + ".mp4" + " -ab 384 -o " + this.dtshd_path + this.procesfileName.substring(0, this.procesfileName.lastIndexOf(".")) + "_DTS.mp4 -ts";
		exit = callexec(rt,command);
		println("TaskID=" + this.taskid + ": " + command + ": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		
		return true;
	}

	/**
	 * @param rt
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private boolean assemble_video(Runtime rt) throws IOException, InterruptedException {
		String command;
		String ffmpeg = "/opt/ffmpeg/ffmpeg-git-20160409-64bit-static/ffmpeg ";
		int exit;
		if (this.outformat.intern() == ".mp4".intern()) {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy -bsf:a aac_adtstoasc " + transPath + this.procesfileName + this.outformat;
		} else if (this.outformat.intern() == ".ts".intern()) {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy " + transPath + this.procesfileName + this.outformat;
		} else {
			command = ffmpeg  + "-f concat -i " + transPath + "out.ffconcat -vcodec copy -acodec copy " + transPath + this.procesfileName + this.outformat;
		}
		
		exit = callexec(rt, command);
		print("TaskID=" + this.taskid + ": " + command);
		println(": " + (exit == 0 ? "Success" : "Fail"));
		if (exit != 0)
			return false;
		return true;
	}

	/**
	 * @throws FileNotFoundException
	 */
	private void generate_concat() throws FileNotFoundException {
		File transVideoPath = new File(transPath);
		PrintWriter outTxt = new PrintWriter(transPath + "out.ffconcat");
		String[] transList = transVideoPath.list(filter(".*\\.(mp4|ts)"));
		Arrays.sort(transList);
		for (String filename : transList) {
			outTxt.println("file " + filename);
		}
		outTxt.close();
	}

	private boolean copy_to_client(String hadoop, String[] splitList, Runtime rt)
			throws IOException, InterruptedException {
		String command;
		int exit;
		clearDir(new File(transPath));
		for (String splitname : splitList) {
			command = hadoop + "fs -copyToLocal /" + this.username + "/" + procesfileName + "/trans/" + splitname + this.outformat + " " + transPath;
			
			exit = callexec(rt, command);
			print("TaskID=" + this.taskid + ": " + command);
			println(": " + (exit == 0 ? "Success" : "Fail"));
			if (exit != 0)
				return false;
		}

		return true;
	}
}
