package cn.cntv.transcoder;

import java.io.File;
import java.util.Iterator;
import java.util.UUID;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;


/**
 * parse the transcode parameters from a XML file.
 * 
 */
public class ParaParser {
	private static String transcode_parameters = null; 
	private static String split_file_size = null;
	private static String fileout_format = null;
	private static String output_filename = null;
	private static String video_codec_type = null;
	private static String audio_track_select = null;
	private static boolean audio_dts_enabled = false;

	public static void parser(File inputXml) {
		ParaParser.transcode_parameters = "";
		ParaParser.fileout_format = "";
		SAXReader saxReader = new SAXReader();
		try {
			Document document = saxReader.read(inputXml);
			Element parameters = document.getRootElement();
			for (Iterator i = parameters.elementIterator(); i.hasNext();) {
				Element parameter = (Element) i.next();
				String name = parameter.getName();
				String value = parameter.getText();
				if (value.length() > 0) {
					if (name.intern() == "video_bitrate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-b:v " + value + " ";
					}
					
					if (name.intern() == "video_maxrate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-maxrate " + value + " ";
					}
					
					if (name.intern() == "video_bufsize".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-bufsize " + value + " ";
					}

					if (name.intern() == "video_size".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-s " + value + " ";
					}

					if (name.intern() == "video_frame_rate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-r " + value + " ";
					}
					
					if (name.intern() == "video_gop".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-g " + value + " ";
					}
					
					if (name.intern() == "video_aspect_ratio".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-aspect " + value + " ";
					}
					
					if (name.intern() == "video_chroma_format".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-pix_fmt " + value + " ";
					}
					
					if (name.intern() == "video_codec".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-c:v " + value + " ";
						video_codec_type = value;
					}
					
					if (name.intern() == "video_x264_params".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-x264opts " + value + " ";
					}
					
					if (name.intern() == "video_x265_params".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-x265-params " + value + " ";
					}
					
					if (name.intern() == "video_out_format".intern() && !value.trim().isEmpty()) {
						fileout_format = "." + value;
					}
					
					if (name.intern() == "audio_codec".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-c:a " + value + " ";
					}
					
					if (name.intern() == "audio_channel".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-ac " + value + " ";
					}
					
					if (name.intern() == "audio_samplerate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-ar " + value + " ";
					}
					
					if (name.intern() == "audio_bitrate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-b:a " + value + " ";
					}
					
					if (name.intern() == "subtitle_select".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-filter_complex [0:v][0:s:" + value + "]overlay[v] -map [v]";
					}
					
					if (name.intern() == "audio_select".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-map 0:a:" + value + " ";
						audio_track_select = "-map 0:a:" + value + " ";
					}
					
					if (name.intern() == "audio_dts_enabled".intern() && !value.trim().isEmpty()) {
						if (value.intern() == "yes".intern())
							audio_dts_enabled = true;
						else if (value.intern() == "no".intern())
							audio_dts_enabled = false;
					}
					
					if (name.intern() == "split_size".intern() && !value.trim().isEmpty()) {
						split_file_size = value + " ";
					}
					
					if (name.intern() == "output_filename".intern() && !value.trim().isEmpty()) {
						output_filename = value;
					}
				}
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}
	
	public static String getParameter() {
		return ParaParser.transcode_parameters;
	}
	
	public static String getFileoutFormat() {
		return ParaParser.fileout_format;
	}
	
	public static String getSplitSize() {
		return ParaParser.split_file_size;
	}
	
	public static String getOutputFilename() {
		return ParaParser.output_filename;
	}
	
	public static boolean getAudioDTSEnabled() {
		return ParaParser.audio_dts_enabled;
	}
	
	public static String getVideoCodecType() {
		return ParaParser.video_codec_type;
	}
	
	public static String getAudioTrackSelect() {
		return ParaParser.audio_track_select;
	}
}
