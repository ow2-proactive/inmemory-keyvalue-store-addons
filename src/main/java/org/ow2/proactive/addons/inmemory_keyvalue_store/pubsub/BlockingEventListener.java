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
package org.ow2.proactive.addons.inmemory_keyvalue_store.pubsub;

import java.io.Serializable;
import java.util.concurrent.Semaphore;

import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.Body;
import org.objectweb.proactive.InitActive;
import org.objectweb.proactive.annotation.ImmediateService;
import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.core.node.NodeException;
import org.objectweb.proactive.extensions.annotation.ActiveObject;

import com.google.common.base.Optional;


/**
 * @author ActiveEon Team
 */
@ActiveObject
public class BlockingEventListener implements InitActive, EventListener {

    private Semaphore semaphore;

    // using optional from Guava since the one from JDK is not Serializable
    private Optional<String> expectedKey;

    // this field can be updated by one thread at a time only
    // but read by multiple at the same time.
    // volatile is used to ensure visibility in all threads
    private volatile Serializable lastValueReceived;

    /**
     * This public empty noarg constructor is required by ProActive.
     * However, you must not use it. Please see {@link #newActive()}.
     */
    public BlockingEventListener() {

    }

    public static BlockingEventListener newActive() throws ActiveObjectCreationException, NodeException {
        return PAActiveObject.newActive(BlockingEventListener.class, new Object[0]);
    }

    @Override
    public void initActivity(Body body) {
        expectedKey = Optional.absent();
        semaphore = new Semaphore(0, true);
    }

    @Override
    public void onEvent(String channel, String key, Serializable value) {
        if (!expectedKey.isPresent() || (expectedKey.isPresent() && key.equals(expectedKey.get()))) {
            int nbPermitsBeforeReleasing = semaphore.availablePermits();
            semaphore.release();

            if (nbPermitsBeforeReleasing == 0) {
                lastValueReceived = value;
            }
        }
    }

    @ImmediateService
    public Serializable awaitEventReception() throws InterruptedException {
        return awaitEventReception(Optional.absent());
    }

    @ImmediateService
    public Serializable awaitEventReception(String key) throws InterruptedException {
        return awaitEventReception(Optional.of(key));
    }

    private Serializable awaitEventReception(Optional<String> key) throws InterruptedException {
        reset();
        expectedKey = key;
        semaphore.acquire();
        return lastValueReceived;
    }

    public boolean reset() {
        semaphore.drainPermits();
        return true;
    }

}
