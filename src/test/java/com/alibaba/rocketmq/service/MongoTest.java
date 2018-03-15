package com.alibaba.rocketmq.service;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

public class MongoTest {

	public static void main(String[] args) {
		insertTest();
		
		insertTest();
		
		findTest();
	}

	private static void findTest() {
		try {
			List<ServerAddress> addresses = getAddresses();

			MongoClient mgClient = getMongoClient(addresses);

			DB mqDb = mgClient.getDB("mq_messages");
			DBCollection mqMessageCollection = mqDb.getCollection("messages_20160728");
			System.out.println(mqMessageCollection);

			try {
		        DBCursor cur = mqMessageCollection.find();
		        while (cur.hasNext()) {
		            System.out.println(cur.next());
		        }
		        
		        for (DBObject index : mqMessageCollection.getIndexInfo()) {
		        	System.out.println("IndexInfo: " + index);
		        }

			} catch (Exception e) {
				System.out.println("insert mongo error:" + e.getMessage());
			}

		} catch (Throwable e) {
			System.out.println("open mongo Exeption " + e.getMessage());
		}
	}

	private static MongoClient getMongoClient(List<ServerAddress> addresses) {
		List<MongoCredential> credentialsList = new LinkedList<MongoCredential>();
		MongoCredential credential = MongoCredential.createCredential("lusong", "mq_messages",
				"lusong".toCharArray());
		credentialsList.add(credential);
		MongoClient mgClient = new MongoClient(addresses, credentialsList);
		return mgClient;
	}

	private static List<ServerAddress> getAddresses() throws UnknownHostException {
		List<ServerAddress> addresses = new ArrayList<ServerAddress>();
		String[] mongoHosts = "192.168.56.101:27117,192.168.56.101:27118,192.168.56.101:27119".trim().split(",");
		if (mongoHosts != null && mongoHosts.length > 0) {
			for (String mongoHost : mongoHosts) {
				if (mongoHost != null && mongoHost.length() > 0) {
					String[] mongoServer = mongoHost.split(":");
					if (mongoServer != null && mongoServer.length == 2) {
						//System.out.println(">>>>>>>>add mongo server>>" + mongoServer[0] + ":" + mongoServer[1]);
						ServerAddress address = new ServerAddress(mongoServer[0].trim(),
								Integer.parseInt(mongoServer[1].trim()));
						addresses.add(address);
					}
				}
			}
		}
		return addresses;
	}

	private static void insertTest() {
		try {
			List<ServerAddress> addresses = getAddresses();

			MongoClient mgClient = getMongoClient(addresses);

			DB mqDb = mgClient.getDB("mq_messages");
			DBCollection mqMessageCollection = mqDb.getCollection("messages_20160728");
			System.out.println(mqMessageCollection);

			MongoMqMessage mongoMessage = new MongoMqMessage();
			mongoMessage.setQueueId(1);
			mongoMessage.setStoreSize(1);
			mongoMessage.setQueueOffset(1);

			try {
				DBObject dbObject = BasicDBObjectUtils.castModel2DBObject(mongoMessage);
				WriteResult result = mqMessageCollection.insert(dbObject);
				System.out.println(">>>Result:" + result);

			} catch (Exception e) {
				System.out.println("insert mongo error:" + e.getMessage());
			}

		} catch (Throwable e) {
			System.out.println("open mongo Exeption " + e.getMessage());
		}
	}

}
