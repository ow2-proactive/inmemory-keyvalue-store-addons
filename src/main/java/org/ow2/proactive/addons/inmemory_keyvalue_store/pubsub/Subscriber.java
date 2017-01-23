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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.Body;
import org.objectweb.proactive.InitActive;
import org.objectweb.proactive.annotation.ImmediateService;
import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.core.node.NodeException;
import org.objectweb.proactive.extensions.annotation.ActiveObject;


/**
 * @author ActiveEon Team
 */
@ActiveObject
public class Subscriber implements InitActive {

    private ConcurrentMap<EventListener, Boolean> listeners;

    /**
     * This public empty noarg constructor is required by ProActive.
     * However, you must not use it. Please see {@link #newActive()}.
     */
    public Subscriber() {

    }

    public static Subscriber newActive() throws ActiveObjectCreationException, NodeException {
        return PAActiveObject.newActive(Subscriber.class, new Object[0]);
    }

    @Override
    public void initActivity(Body body) {
        listeners = new ConcurrentHashMap<>();
    }

    @ImmediateService
    public Boolean addEventListener(EventListener listener) {
        return listeners.put(listener, Boolean.TRUE);
    }

    @ImmediateService
    public void receive(String channel, String key, Serializable value) {
        for (EventListener listener : listeners.keySet()) {
            listener.onEvent(channel, key, value);
        }
    }

}
