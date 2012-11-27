package mongodb.client.gmongo.util

import net.sf.json.JSON
import net.sf.json.xml.XMLSerializer

import org.json.JSONObject
import org.json.XML
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import groovy.json.*
import groovy.util.logging.Slf4j;


class JsonUtil {
	private def static Logger log = LoggerFactory.getLogger(JsonUtil.class)

	def static String xmltoJson(String message){
		XMLSerializer xmlSerializer = new XMLSerializer()
		xmlSerializer.setSkipNamespaces( true )
		xmlSerializer.setTrimSpaces( true )
		xmlSerializer.setRemoveNamespacePrefixFromElements(true)

		JSON json = xmlSerializer.read(message)

		log.debug("[x] "+json.toString())

		return json.toString()
	}


	def static toJson(String message){
		org.json.JSONObject jsonObj = XML.toJSONObject(message)

		if (log.isDebugEnabled()){
			log.debug(jsonObj.toString(2))
		}

		return jsonObj.toString()
	}

	def static String buildJson(String msg){
		def json =  new groovy.json.JsonBuilder(msg)

		if (log.isDebugEnabled()){
			log.debug(JsonOutput.prettyPrint(json.toString()))
		}

		return json.toString()
	}
}


