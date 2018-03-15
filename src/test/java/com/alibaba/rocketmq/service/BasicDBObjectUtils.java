package com.alibaba.rocketmq.service;

import java.lang.reflect.Method;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BasicDBObjectUtils {

	public static <T> DBObject castModel2DBObject(T entity) throws Exception {
		Method[] method = entity.getClass().getMethods();

		DBObject dbObject = new BasicDBObject();
		for (Method m : method) {
			// System.out.println(m.getName());
			if (m.getName().startsWith("get")) {
				String name = m.getName().replace("get", "");
				for (Method m2 : method) {
					if (m2.getName().equals("set" + name)) {
						name = name.substring(0, 1).toLowerCase()
								+ name.substring(1);
						Object returnVal = m.invoke(entity, new Object[] {});
						if (returnVal != null) {
							// System.out.println(name + " : " +
							// m.invoke(shipping, new Object[] {}));
							dbObject.put(name, returnVal);
						}
					}
				}
			}
		}
		//System.out.println("dbObject: " + dbObject);
		return dbObject;
	}
	
}
