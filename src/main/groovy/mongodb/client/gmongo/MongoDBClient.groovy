package mongodb.client.gmongo

import groovy.util.logging.Slf4j
import net.sf.json.JSONObject

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.mongodb.MongoException

import dsl.config.util.DslConfig


/**
 * @author 
 *
 */
@Singleton
@Slf4j
class MongoDBClient{
	def dbName = DslConfig.instance.factory.mongodb.dbName
	def collectionName = DslConfig.instance.factory.mongodb.collectionName
	def String idName = DslConfig.instance.factory.mongodb.idName

	def read(DB db, String dbName, String collectionName, id){
		def res

		try {
			DBCollection collection = db.getCollection(collectionName)

			BasicDBObject searchQuery = new BasicDBObject()
			searchQuery.put(idName, id.toString())
			DBCursor cursor = collection.find(searchQuery)

			def i = 0
			while (cursor.hasNext()) {
				res=cursor.next()
				log.debug("mresults number " + i++)
			}
			return res
		} catch (UnknownHostException e) {
			log.error("Failed query", e)
		} catch (MongoException e) {
			log.error("Failed mongo query", e)
		}
	}
	
	def read(DB db, String dbName, String collectionName, String field, id){
		def res

		try {
			DBCollection collection = db.getCollection(collectionName)

			BasicDBObject searchQuery = new BasicDBObject()
			searchQuery.put(field, id.toString())
			DBCursor cursor = collection.find(searchQuery)

			def i = 0
			while (cursor.hasNext()) {
				res=cursor.next()
				log.debug("mresults number " + i++)
			}
			return res
		} catch (UnknownHostException e) {
			log.error("Failed query", e)
		} catch (MongoException e) {
			log.error("Failed mongo query", e)
		}
	}

	def removeAll(DB db,  String dbName, String collectionName){
		try {
			DBCollection collection = db.getCollection(collectionName)

			if(collection.find().count()>0){
				collection.remove(new BasicDBObject())
			}

			log.info("Clean mongo done")
		} catch (UnknownHostException e) {
			log.error("Failed clean", e)
		} catch (MongoException e) {
			log.error("Faile clean", e)
		}
	}

	def removeOne(DB db,  String dbName, String collectionName, String id){
		try {
			DBCollection collection = db.getCollection(collectionName)

			DBObject deletePig=new BasicDBObject()
			deletePig.put(idName, id)
			collection.remove(deletePig)
			log.info("Clean one " + id)
		} catch (UnknownHostException e) {
			log.error("Failed clean one", e)
		} catch (MongoException e) {
			log.error("Failed clean one", e)
		}
	}


	def mapWrite(DB db, String dbName, String collectionName, Map dataMap){
		DBCollection collection = db.getCollection(collectionName)
		
		def String id = dataMap[idName]
		def Map res = read(db, dbName, collectionName, id)
		if (res){
			dataMap['_id'] = res._id
			collection.save(new BasicDBObject(dataMap));
		}else{
			collection.save(new BasicDBObject(dataMap));
		}

	}

	def encoder = { mobj ->
		JSONObject jsonObj = new JSONObject()
		jsonObj.putAll(mobj)
		return jsonObj
	}

	def mload(List ids, collection){
		DB db = DBManager.instance.getDB();
		def titles = []

		try{
			db.requestStart()
			ids.each {
				def Map res = read(db, dbName, collection, it)
				if (res){
					res?.remove("_id")
					JSONObject jsonObj = encoder(res)
					titles.add(jsonObj)
				}else{
					log.warn("No record from mongo ${collection}: " + it)
				}
			}
		}
		catch(ex){
			log.warn("Failed load from mongo", ex)
		}finally{
			db.requestDone();
		}
		return titles
	}
}





