package mongodb.client.gmongo

import com.mongodb.DB

interface MongoCallback {
	
	def runInMongo(DB db)

}
