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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.Body;
import org.objectweb.proactive.InitActive;
import org.objectweb.proactive.annotation.ImmediateService;
import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.core.node.NodeException;
import org.objectweb.proactive.extensions.annotation.ActiveObject;
import org.ow2.proactive.addons.inmemory_keyvalue_store.pubsub.Subscriber;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;


/**
 * @author ActiveEon Team
 */
@ActiveObject
public class InMemoryKeyValueStore implements InitActive {

    public static final String DEFAULT_CONTEXT = "default";

    // key is the channel name
    // value is the set of subscribers
    private Multimap<String, String> channels;

    // channel name, key, value
    private Table<String, String, Serializable> data;

    /**
     * This public empty noarg constructor is required by ProActive.
     * However, you must not use it. Please see {@link #newActive()}.
     */
    public InMemoryKeyValueStore() {

    }

    public static InMemoryKeyValueStore newActive() throws ActiveObjectCreationException, NodeException {
        return PAActiveObject.newActive(InMemoryKeyValueStore.class, new Object[0]);
    }

    @Override
    public void initActivity(Body body) {
        channels = HashMultimap.create();
        data = HashBasedTable.create();
    }

    @ImmediateService
    public Serializable put(String key, Serializable value) {
        return put(DEFAULT_CONTEXT, key, value);
    }

    @ImmediateService
    public Serializable put(String context, String key, Serializable value) {
        synchronized (data) {
            return data.put(context, key, value);
        }
    }

    @ImmediateService
    public boolean contains(String context, String key) {
        synchronized (data) {
            return data.row(context).containsKey(key);
        }
    }

    @ImmediateService
    public boolean contains(String key) {
        return contains(DEFAULT_CONTEXT, key);
    }

    @ImmediateService
    public Serializable get(String context, String key) {
        synchronized (data) {
            return data.row(context).get(key);
        }
    }

    @ImmediateService
    public Serializable get(String key) {
        return get(DEFAULT_CONTEXT, key);
    }

    @ImmediateService
    public Serializable remove(String context, String key) {
        synchronized (data) {
            return data.row(context).remove(key);
        }
    }

    @ImmediateService
    public Serializable remove(String key) {
        return remove(DEFAULT_CONTEXT, key);
    }

    @ImmediateService
    public Map<String, Map<String, Serializable>> getData() {
        synchronized (data) {
            Map<String, Map<String, Serializable>> rowMap = data.rowMap();

            // rowMap objects returned by guava are not Serializable
            Map<String, Map<String, Serializable>> result = new HashMap<>(rowMap.size());
            for (Map.Entry<String, Map<String, Serializable>> entries : rowMap.entrySet()) {
                result.put(entries.getKey(), new HashMap<>(entries.getValue()));
            }

            return result;
        }
    }

    @ImmediateService
    public Multimap<String, String> getChannels() {
        synchronized (channels) {
            return ImmutableMultimap.copyOf(channels);
        }
    }

    // Publish/Subscribe API

    @ImmediateService
    public boolean subscribe(String channel, Subscriber subscriber) {
        synchronized (channels) {
            boolean result = channels.put(channel, PAActiveObject.getUrl(subscriber));
            // used to weak up publish operations that are waiting for a subscriber
            channels.notifyAll();
            return result;
        }
    }

    @ImmediateService
    public boolean unsubscribe(String channel, Subscriber subscriber, boolean deleteAssociatedChannelData) {
        synchronized (channels) {
            if (deleteAssociatedChannelData) {
                synchronized (data) {
                    data.row(channel).clear();
                }
            }

            return channels.remove(channel, PAActiveObject.getUrl(subscriber));
        }
    }

    @ImmediateService
    public Collection<String> unsubscribeAll(String channel) {
        synchronized (channels) {
            return channels.removeAll(channel);
        }
    }

    // FIXME: wait is performed on server side, thus forcing the method call to keep
    // a connection open until the condition is satisfied. This connection may timeout
    // depending of the value configured at the ProActive level.
    // Fixing such a behaviour requires to handle the synchronization on the caller.
    // It means that a new Active Objects will be needed to receive an acknowledgement
    // saying that the condition is satisfied.
    // This change has not be done yet since it was not required for the CLIF project
    @ImmediateService
    public boolean waitSubscriptions(String channel, int minNumberOfSubscriptionAwaited) {
        synchronized (channels) {
            while (channels.get(channel).size() < minNumberOfSubscriptionAwaited) {
                try {
                    // use timeout to be able to interrupt the thread is required
                    // TODO: extract timeout value as parameter?
                    channels.wait(1000);
                } catch (InterruptedException e) {
                    // TODO use a logger
                    e.printStackTrace();
                }
            }
        }

        return true;
    }

    @ImmediateService
    public Serializable publish(String channel, String key, Serializable value) {
        synchronized (data) {
            Serializable result = data.put(channel, key, value);
            notify(channel, key, value);
            return result;
        }
    }

    private void notify(String channel, String key, Serializable value) {
        synchronized (channels) {
            for (String subscriberUrl : channels.get(channel)) {
                try {
                    Subscriber subscriber = lookupActive(subscriberUrl);
                    subscriber.receive(channel, key, value);
                } catch (IOException | ActiveObjectCreationException e) {
                    // TODO use a logger
                    e.printStackTrace();
                }
            }
        }
    }

    private Subscriber lookupActive(String url) throws IOException, ActiveObjectCreationException {
        return PAActiveObject.lookupActive(Subscriber.class, url);
    }

}
