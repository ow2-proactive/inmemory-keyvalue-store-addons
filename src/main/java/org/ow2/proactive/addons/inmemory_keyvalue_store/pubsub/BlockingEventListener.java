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

    private Serializable lastValueReceived;

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
