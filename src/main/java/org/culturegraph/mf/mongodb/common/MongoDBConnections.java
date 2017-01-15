/*
 *  Copyright 2013, 2014 Deutsche Nationalbibliothek
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

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.mongodb.MongoDBWriter;
import org.culturegraph.mf.mongodb.MongoDBReader;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Utility class for {@link MongoDBConnection}.
 *
 * @author Thomas Seidel
 */
public final class MongoDBConnections {

	private MongoDBConnections() {
		throw new AssertionError("No instances allowed");
	}

	/**
	 * Wraps a {@link MongoDBConnection} to ignore
	 * {@link MongoDBConnection#close()} calls. Use this method if you like to
	 * share an instance of {@code MongoDBConnection} between multiple
	 * {@link MongoDBReader} or {@link MongoDBWriter}. In this case, a call to
	 * {@link StreamReceiver#closeStream()} should not close the connection.
	 *
	 * @param mongoDBConnection the actual connection.
	 * @return the actual connection wrapped to ignore the
	 *         {@link MongoDBConnection#close()} calls
	 */
	public static MongoDBConnection unclosableConnection(
			final MongoDBConnection mongoDBConnection) {
		return new UncloseableMongoDBConnection(mongoDBConnection);
	}

	/**
	 * Wraps a MongoDBConnection to ignore {@link MongoDBConnection#close()}
	 * calls.
	 *
	 * @author Thomas Seidel
	 */
	private static final class UncloseableMongoDBConnection
			implements MongoDBConnection {

		private final MongoDBConnection mongoDBConnection;

		UncloseableMongoDBConnection(final MongoDBConnection mongoDBConnection) {
			this.mongoDBConnection = mongoDBConnection;
		}

		@Override
		public DBCursor find(final DBObject dbObject) {
			return mongoDBConnection.find(dbObject);
		}

		@Override
		public void save(final DBObject dbObject) {
			mongoDBConnection.save(dbObject);
		}

		@Override
		public void close() {
			// ignore call
		}

	}

}
