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
import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.core.node.NodeException;
import org.ow2.proactive.addons.inmemory_keyvalue_store.pubsub.BlockingEventListener;
import org.ow2.proactive.addons.inmemory_keyvalue_store.pubsub.Subscriber;


/**
 * @author ActiveEon Team
 */
public class PublishSubscribeIntegrationTest extends IntegrationTest {

    @Test
    public void testNotificationReceptionPublishSubscribePublicationBeforeSubscription()
            throws InterruptedException, ActiveObjectCreationException, NodeException {
        String channel = "topic";
        String key = "REGISTRY_URL";
        String value = "scheme://";

        new Thread(() -> {
            try {
                inMemoryKeyValueStoreStub.waitSubscriptions(channel, 1);
                inMemoryKeyValueStoreStub.publish(channel, key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        TimeUnit.SECONDS.sleep(3);

        Subscriber subscriber = Subscriber.newActive();
        BlockingEventListener blockingEventListener = BlockingEventListener.newActive();
        subscriber.addEventListener(blockingEventListener);

        assertThat(inMemoryKeyValueStoreStub.subscribe(channel, subscriber)).isTrue();

        Serializable valueReceived = blockingEventListener.awaitEventReception();

        assertThat(valueReceived).isEqualTo(value);

        assertThat(inMemoryKeyValueStoreStub.unsubscribe(channel, subscriber, true)).isTrue();
        assertThat(inMemoryKeyValueStoreStub.contains(channel, key)).isFalse();
    }

}
