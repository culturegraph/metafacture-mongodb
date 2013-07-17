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

package org.culturegraph.mf.mongodb.sink;

import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * 
 * @author Thomas Seidel
 * 
 */
public final class MongoDBWriterTest {

	private static final String EXPECTED_DBOBJECT_AS_JSON = "{ \"_id\" : \"42\","
			+ "   \"data\" : ["
			+ "     { \"#a\" : \"value1\" },"
			+ "     { \"#A\" : ["
			+ "       { \"#B\" : ["
			+ "         { \"#b\" : \"value2\" }"
			+ "         ]"
			+ "       },"
			+ "       { \"#a\" : \"value3\" }"
			+ "       ]"
			+ "     }"
			+ "   ]"
			+ " }";

	private MongoDBWriter mongoDBWriter;

	@Mock
	private MongoDBConnection mongoDBConnection;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		mongoDBWriter = new MongoDBWriter(mongoDBConnection);
	}

	@Test
	public void shouldSaveStreamAsSingleDBObject() {
		mongoDBWriter.startRecord("42");
		mongoDBWriter.literal("a", "value1");
		mongoDBWriter.startEntity("A");
		mongoDBWriter.startEntity("B");
		mongoDBWriter.literal("b", "value2");
		mongoDBWriter.endEntity();
		mongoDBWriter.literal("a", "value3");
		mongoDBWriter.endEntity();
		mongoDBWriter.endRecord();
		mongoDBWriter.closeStream();

		final DBObject expected = (DBObject) JSON
				.parse(EXPECTED_DBOBJECT_AS_JSON);
		Mockito.verify(mongoDBConnection).save(expected);
	}

}
