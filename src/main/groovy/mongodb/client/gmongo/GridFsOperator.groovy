package mongodb.client.gmongo

import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipInputStream

import com.mongodb.DB
import com.mongodb.Mongo
import com.mongodb.gridfs.GridFS
import com.mongodb.gridfs.GridFSDBFile
import com.mongodb.gridfs.GridFSFile
import com.mongodb.gridfs.GridFSInputFile

class GridFsOperator {

	static DB imgDB=null;
	static GridFS gridFS=null;
	static collections = ['titles.json', 'providers.json']


	public void saveFile(File file,String fileName) {
		try {
			GridFSFile mongofile =gridFS.createFile(file);
			mongofile.put("filename", fileName);
			mongofile.put("uploadDate", new Date() );
			mongofile.put("contentType",fileName.substring( fileName.lastIndexOf(".") ));
			mongofile.save();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public List<GridFSDBFile> findFilesByName(String fileName) {
		List<GridFSDBFile> list =gridFS.find(fileName);
		return list;
	}

	public static void gridfsCall() throws Exception {

		//connect to Mongo
		Mongo mongo = new Mongo();
		DB db = mongo.getDB('gridFs');
		GridFS fs = new GridFS(db);

		//save the a file
		File install = new File("/home/kara/git/packagecollector/package-collector/JsonTestDirectory/" + 'VODPackageData_22102012_111512.zip');
		GridFSInputFile inFile =  fs.createFile(install);
		inFile.save();

		//read the file
		GridFSDBFile outFile = fs.findOne('VODPackageData_22102012_111512.zip');
		System.out.println(outFile);
		
		ZipInputStream zin = new ZipInputStream(outFile.getInputStream());
		fromGrid(zin)

		//write output to temp file
		//File temp = File.createTempFile("delme", ".zip");
		//System.out.println(temp.getPath());
		//outFile.writeTo(temp);
	}

	public static void main(String[] args) throws Exception {
		//def file = "/home/kara/git/packagecollector/package-collector/JsonTestDirectory/VODPackageData_22102012_111512.zip"
		//loadZip(file);
		gridfsCall()
	}
	
	public static void fromGrid(ZipInputStream zin) throws IOException {

			  ZipEntry ze = null;
			  
			  InputStreamReader isr = new InputStreamReader(zin);
			  BufferedReader br = new BufferedReader(isr);
			  
			  while ((ze = zin.getNextEntry()) != null) {
				  if (ze.getName() in collections == true){
					  long size = ze.getSize()
					  System.out.println("Unzipping " + ze.getName());
					  System.out.println("Length is " + size);
					  
					  String line;
					  StringBuilder builder = new StringBuilder()
					  while ((line = br.readLine()) != null) {
						  builder.append(line)
					  }
					  
					  
					  //MongoDBImporter loader = new MongoDBImporter()
					  //loader.loadDataByReader(builder.toString(), ze.getName()[0..-6]);
					  
					 
					  
				  }

				zin.closeEntry();
				
			  }
			  br.close()

			  zin.close()
	}
		  
	
	public static void loadZip(String filePath) {
		try {
			ZipFile zf = new ZipFile(filePath);
			Enumeration entries = zf.entries();

			while (entries.hasMoreElements()) {
				ZipEntry ze = (ZipEntry) entries.nextElement();
				if (ze.getName() in collections == true) {
					long size = ze.getSize();
					if (size > 0) {
						println("Length is " + size);
						println(ze.getName()[0..-6]);
						//MongoDBImporter loader = new MongoDBImporter()
						//loader.loadDataStream(zf.getInputStream(ze), ze.getName()[0..-6]);
					}
				}
			}
			zf.close()
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	public static void readZip() {
		try {
			ZipFile zf = new ZipFile("/home/kara/git/packagecollector/package-collector/JsonTestDirectory/VODPackageData_22102012_111512.zip");
			Enumeration entries = zf.entries();

			BufferedReader input = new BufferedReader(new InputStreamReader(
					System.in));
			while (entries.hasMoreElements()) {
				ZipEntry ze = (ZipEntry) entries.nextElement();
				System.out.println("Read " + ze.getName() + "?");
				String inputLine = input.readLine();
				if (inputLine.equalsIgnoreCase("yes")) {
					long size = ze.getSize();
					if (size > 0) {
						System.out.println("Length is " + size);
						BufferedReader br = new BufferedReader(
								new InputStreamReader(zf.getInputStream(ze)));
						String line;
						while ((line = br.readLine()) != null) {
							System.out.println(line);
						}
						br.close();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
