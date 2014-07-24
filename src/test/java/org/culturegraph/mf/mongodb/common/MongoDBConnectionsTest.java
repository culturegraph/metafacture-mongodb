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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.mongodb.DBObject;

/**
 * 
 * @author Thomas Seidel
 * 
 */
public final class MongoDBConnectionsTest {

	@Mock
	private MongoDBConnection mongoDBConnection;

	@Mock
	private DBObject dbObject;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void unclosableMongoDBConnectionShouldForwardAllCallsExceptClose() {
		final MongoDBConnection unclosableMongoDBConnection = MongoDBConnections
				.unclosableConnection(mongoDBConnection);
		unclosableMongoDBConnection.find(dbObject);
		Mockito.verify(mongoDBConnection).find(dbObject);
		unclosableMongoDBConnection.save(dbObject);
		Mockito.verify(mongoDBConnection).save(dbObject);
	}

	@Test
	public void unclosableMongoDBConnectionShouldIgnoreCloseCall() {
		final MongoDBConnection unclosableMongoDBConnection = MongoDBConnections
				.unclosableConnection(mongoDBConnection);
		unclosableMongoDBConnection.close();
		Mockito.verify(mongoDBConnection, Mockito.never()).close();
	}

}
