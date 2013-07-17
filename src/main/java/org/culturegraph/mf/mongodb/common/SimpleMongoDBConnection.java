/*
 *  Copyright 2013 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.culturegraph.mf.mongodb.common;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Setups the MongoDB connection.
 * 
 * @author Thomas Seidel
 * 
 */
public class SimpleMongoDBConnection implements MongoDBConnection {

	private final DBCollection dbCollection;

	/**
	 * @param uri
	 *            monogdb://user:pass@host:port/database.collection?options...
	 * @see MongoClientURI
	 */
	public SimpleMongoDBConnection(final String uri)
			throws UnknownHostException {
		final MongoClientURI mongoClientUri = new MongoClientURI(uri);
		final MongoClient mongoClient = new MongoClient(mongoClientUri);
		final DB db = mongoClient.getDB(mongoClientUri.getDatabase());
		dbCollection = db.getCollection(mongoClientUri.getCollection());
	}

	@Override
	public final DBCursor find(final DBObject dbObject) {
		return dbCollection.find(dbObject);
	}

	@Override
	public final void save(final DBObject dbObject) {
		dbCollection.save(dbObject);
	}

}
