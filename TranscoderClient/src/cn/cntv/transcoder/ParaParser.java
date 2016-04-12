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

	public static String parser(File inputXml) {
		String settings = "";
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
						settings += "-b:v " + value + " ";
					}

					if (name.intern() == "video_size".intern() && !value.trim().isEmpty()) {
						settings += "-s " + value + " ";
					}

					if (name.intern() == "frame_rate".intern() && !value.trim().isEmpty()) {
						settings += "-r " + value + " ";
					}
					
					if (name.intern() == "video_code".intern() && !value.trim().isEmpty()) {
						settings += "-c:v " + value + " ";
					}
				}
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		}

		return settings;
	}
}
