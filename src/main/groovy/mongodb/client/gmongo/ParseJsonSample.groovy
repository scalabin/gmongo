package mongodb.client.gmongo

import org.codehaus.jackson.map.*;
import org.codehaus.jackson.*;

import java.io.File;

public class ParseJsonSample {
	public static void main(String[] args) throws Exception {
		JsonFactory f = new MappingJsonFactory();
		JsonParser jp = f.createJsonParser(new File("/home/kara/groovywk/mongo_base/src/main/resources/titles.json"));

		JsonToken current;

		current = jp.nextToken();
		if (current != JsonToken.START_OBJECT) {
			System.out.println("Error: root should be object: quiting.");
			return;
		}

		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = jp.getCurrentName();
			// move from field name to field value
			current = jp.nextToken();
			if (fieldName.equals("records")) {
				if (current == JsonToken.START_ARRAY) {
					// For each of the records in the array
					while (jp.nextToken() != JsonToken.END_ARRAY) {
						// read the record into a tree model,
						// this moves the parsing position to the end of it
						JsonNode node = jp.readValueAsTree();
						// And now we have random access to everything in the object
						System.out.println("field1: " + node.get("field1").getValueAsText());
						System.out.println("field2: " + node.get("field2").getValueAsText());
					}
				} else {
					System.out.println("Error: records should be an array: skipping.");
					jp.skipChildren();
				}
			} else {
				System.out.println("Unprocessed property: " + fieldName);
				jp.skipChildren();
			}
		}
	}
}