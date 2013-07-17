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

import org.culturegraph.mf.mongodb.sink.MongoDBWriter;
import org.culturegraph.mf.mongodb.source.MongoDBReader;

/**
 * Defines special keys to be used by MongoDBWriter and MongoDBReader
 * 
 * @author Thomas Seidel
 * 
 * @see MongoDBWriter
 * @see MongoDBReader
 * 
 */
public final class MongoDBKeys {

	/**
	 * The key used to store a record identifier
	 */
	public static final String RECORD_ID_KEY = "_id";

	/**
	 * The key used to store the entities and literals
	 */
	public static final String DATA_KEY = "data";

	/**
	 * The prefix to put in front of every entity and literal key
	 */
	public static final String KEY_PREFIX = "#";

	private MongoDBKeys() {

	}

}
