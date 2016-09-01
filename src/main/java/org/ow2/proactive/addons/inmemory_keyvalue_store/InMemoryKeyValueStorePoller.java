/*
 * ################################################################
 *
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2016 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 *  Initial developer(s):               The ProActive Team
 *                        http://proactive.inria.fr/team_members.htm
 *  Contributor(s):
 *
 * ################################################################
 * $$PROACTIVE_INITIAL_DEV$$
 */
package org.ow2.proactive.addons.inmemory_keyvalue_store;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.api.PAFuture;


/**
 * @author ActiveEon Team
 */
public class InMemoryKeyValueStorePoller {

    public static Serializable awaitUntilKeyIsPresent(String url, String key, long pollIntervalInMs,
            boolean deleteKey) throws IOException, ActiveObjectCreationException {
        return awaitUntilKeyIsPresent(url, key, pollIntervalInMs, 7, TimeUnit.DAYS, deleteKey);
    }

    public static Serializable awaitUntilKeyIsPresent(String url, String key, long pollIntervalInMs,
            long timeoutValue, boolean deleteKey) throws IOException, ActiveObjectCreationException {
        return awaitUntilKeyIsPresent(url, key, pollIntervalInMs, timeoutValue, TimeUnit.MILLISECONDS,
                deleteKey);
    }

    public static Serializable awaitUntilKeyIsPresent(String url, String key, long pollIntervalInMs,
            long timeoutValue, TimeUnit timeoutUnit, boolean deleteKey)
            throws IOException, ActiveObjectCreationException {

        InMemoryKeyValueStore keyValueStoreStub = PAActiveObject.lookupActive(InMemoryKeyValueStore.class,
                url);

        return awaitUntilKeyIsPresent(keyValueStoreStub, key, pollIntervalInMs, timeoutValue, timeoutUnit,
                deleteKey);
    }

    public static Serializable awaitUntilKeyIsPresent(InMemoryKeyValueStore keyValueStoreStub, String key,
            long pollIntervalInMs, long timeoutValue, TimeUnit timeoutUnit, boolean deleteKey) {
        ConditionFactory conditionFactory = Awaitility.await().pollInterval(pollIntervalInMs,
                TimeUnit.MILLISECONDS);

        conditionFactory.atMost(timeoutValue, timeoutUnit).until(() -> isKeyPresent(keyValueStoreStub, key));

        if (deleteKey) {
            return PAFuture.getFutureValue(keyValueStoreStub.remove(key));
        } else {
            return keyValueStoreStub.get(key);
        }
    }

    private static boolean isKeyPresent(InMemoryKeyValueStore keyValueStoreStub, String key) {
        return keyValueStoreStub.contains(key);
    }

}
