package mongodb.client.gmongo



import com.mongodb.DB
import com.mongodb.Mongo
import com.mongodb.MongoOptions
import com.mongodb.ServerAddress

import dsl.config.util.DslConfig


/**
 * @author 
 *
 */
@Singleton
public class DBManager {

	def addresses = DslConfig.instance.factory.mongodb.addresses
	def poolSize = DslConfig.instance.factory.mongodb.poolSize
	def dbName = DslConfig.instance.factory.mongodb.dbName
	def collectionName = DslConfig.instance.factory.mongodb.collectionName

	def Mongo mongo

	private DBManager() {
		init()
	}

	def DB getDB(){
		return mongo.getDB(dbName)
	}

	def init() throws java.net.UnknownHostException {
		MongoOptions options = new MongoOptions()
		options.autoConnectRetry = true
		options.connectionsPerHost = poolSize

		def list = new ArrayList<ServerAddress>()
		def addressArr = addresses.split(",")
		addressArr.each { str ->
			list.add(new ServerAddress(str.split(":")[0], Integer.parseInt(str.split(":")[1])))
		}

		mongo= new Mongo(list, options)
	}
}
