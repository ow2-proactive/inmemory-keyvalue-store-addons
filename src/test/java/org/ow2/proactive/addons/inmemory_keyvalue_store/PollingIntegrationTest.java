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

import static com.google.common.truth.Truth.assertThat;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;


/**
 * @author ActiveEon Team
 */
public class PollingIntegrationTest extends IntegrationTest {

    @Test
    public void testSimplePollingForKey() {
        String key = "URL";
        String value = "pnp://powaaa";

        new Thread(() -> {
            try {
                Thread.sleep(3000);
                inMemoryKeyValueStoreStub.put(key, value);
                System.out.println("Key has been added");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        System.out.println("Waiting for key '" + key + "' to be added in remote key/value store");

        Serializable receivedValue = InMemoryKeyValueStorePoller.awaitUntilKeyIsPresent(inMemoryKeyValueStoreStub,
                                                                                        key,
                                                                                        500,
                                                                                        1,
                                                                                        TimeUnit.DAYS,
                                                                                        true);

        assertThat(inMemoryKeyValueStoreStub.contains(key)).isFalse();
        assertThat(value).isEqualTo(receivedValue);
    }

}
