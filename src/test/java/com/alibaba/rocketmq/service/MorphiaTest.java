package com.alibaba.rocketmq.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MorphiaTest {

	public static void main(String[] args) {
		List<ServerAddress> addresses = getAddresses();

		MongoClient mongoClient = getMongoClient(addresses);
		Morphia morphia = new Morphia();

		Datastore datastore = morphia.createDatastore(mongoClient, "mq_messages");
		//System.out.println(datastore.getDB());

		DBCollection collection = datastore.getDB().getCollection("messages_20160728");
		long allCount = collection.count();
		System.out.println(">>>>>>allCount:" + allCount);

		DBObject query = new BasicDBObject();
		query.put("topic", "xxx-lusong007");

		long topicCount = collection.count(query);
		System.out.println(">>>>>>topicCount:" + topicCount);
		
		Iterator<DBObject> it2 = collection.find(query).iterator();
		while (it2.hasNext()) {
			//System.out.println(">>>>>>>>>>>fetch: " + it2.next().toMap());
		}

	}

	private static MongoClient getMongoClient(List<ServerAddress> addresses) {
		List<MongoCredential> credentialsList = new LinkedList<MongoCredential>();
		MongoCredential credential = MongoCredential.createCredential("lusong", "mq_messages", "lusong".toCharArray());
		credentialsList.add(credential);
		MongoClient mgClient = new MongoClient(addresses, credentialsList);
		return mgClient;
	}

	private static List<ServerAddress> getAddresses() {
		List<ServerAddress> addresses = new ArrayList<ServerAddress>();
		String[] mongoHosts = "192.168.56.101:27117,192.168.56.101:27118,192.168.56.101:27119".trim().split(",");
		if (mongoHosts != null && mongoHosts.length > 0) {
			for (String mongoHost : mongoHosts) {
				if (mongoHost != null && mongoHost.length() > 0) {
					String[] mongoServer = mongoHost.split(":");
					if (mongoServer != null && mongoServer.length == 2) {
						// System.out.println(">>>>>>>>add mongo server>>" + mongoServer[0] + ":" + mongoServer[1]);
						ServerAddress address = new ServerAddress(mongoServer[0].trim(),
								Integer.parseInt(mongoServer[1].trim()));
						addresses.add(address);
					}
				}
			}
		}
		return addresses;
	}

}
