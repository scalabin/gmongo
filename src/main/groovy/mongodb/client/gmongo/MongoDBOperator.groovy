package mongodb.client.gmongo

import groovy.util.logging.Slf4j;

import com.mongodb.DB
import net.sf.json.JSONObject

@Singleton
@Slf4j
class MongoDBOperator {

	def callMongo(MongoCallback callback){
		DB db = DBManager.instance.getDB();
		def res
		try{
			db.requestStart()
			res = callback.runInMongo(db)
		}
		catch(ex){
			//throw new RuntimeException(ex)
			log.error("Failed to call Mongo", ex)
		}finally{
			db.requestDone();
		}
		return res
	}
}

