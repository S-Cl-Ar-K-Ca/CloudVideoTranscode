package cn.cntv.transcoder;

import java.io.File;
import java.util.Iterator;
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
	private static String fileout_format = null;

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
					if (name.intern() == "video_bps".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-b:v " + value + " ";
					}

					if (name.intern() == "video_size".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-s " + value + " ";
					}

					if (name.intern() == "frame_rate".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-r " + value + " ";
					}
					
					if (name.intern() == "video_code".intern() && !value.trim().isEmpty()) {
						transcode_parameters += "-c:v " + value + " ";
					}
					
					if (name.intern() == "out_format".intern() && !value.trim().isEmpty()) {
						fileout_format += "." + value;
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
}
