/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package org.ow2.proactive.addons.inmemory_keyvalue_store;

import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.core.ProActiveException;


/**
 * @author ActiveEon Team
 */
public class InMemoryKeyValueStoreRunner {

    public static final String KEY_VALUE_STORE_STUB_NAME = "keyvalue-store";

    public InMemoryKeyValueStoreRunner() {

    }

    public String exposeKeyValueStore(InMemoryKeyValueStore stub) throws ProActiveException {
        return register(stub, KEY_VALUE_STORE_STUB_NAME);
    }

    private String register(InMemoryKeyValueStore activeObject, String name) throws ProActiveException {
        return PAActiveObject.registerByName(activeObject, name);
    }

    public static void main(String[] args) throws ProActiveException {
        InMemoryKeyValueStoreRunner inMemoryKeyValueStoreRunner = new InMemoryKeyValueStoreRunner();
        InMemoryKeyValueStore keyValueStore = InMemoryKeyValueStore.newActive();
        String url = inMemoryKeyValueStoreRunner.exposeKeyValueStore(keyValueStore);
        System.out.println(url);
    }

}
