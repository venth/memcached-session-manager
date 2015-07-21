/*
 * Copyright 2013 Martin Grotzke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.javakaffee.web.msm;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Factory to create the {@link MemcachedClient}, either directly the spymemcached {@link MemcachedClient}
 * or the {@link com.couchbase.client.CouchbaseClient}.
 *
 * @author <a href="mailto:martin.grotzke@javakaffee.de">Martin Grotzke</a>
 */
public class MemcachedClientFactory {

    public static final String PROTOCOL_BINARY = "binary";

    static interface CouchbaseClientFactory {
        MemcachedClient createCouchbaseClient(MemcachedNodesManager memcachedNodesManager,
                String memcachedProtocol, String username, String password, long operationTimeout,
                long maxReconnectDelay, Statistics statistics );
    }

    protected MemcachedClient createMemcachedClient(final MemcachedNodesManager memcachedNodesManager,
            final String memcachedProtocol, final String username, final String password, final long operationTimeout,
            final long maxReconnectDelay, final Statistics statistics ) {
        try {
            final ConnectionType connectionType = ConnectionType.valueOf(memcachedNodesManager.isCouchbaseBucketConfig(), username, password);
            if (connectionType.isCouchbaseBucketConfig()) {
                return createCouchbaseClient(memcachedNodesManager, memcachedProtocol, username, password, operationTimeout, maxReconnectDelay,
                        statistics);
            }
            final ConnectionFactory connectionFactory = createConnectionFactory(memcachedNodesManager, connectionType, memcachedProtocol,
                    username, password, operationTimeout, maxReconnectDelay, statistics);
            return new JvmRoutelessMemcachedClient(connectionFactory, memcachedNodesManager.getAllMemcachedAddresses());
        } catch (final Exception e) {
            throw new RuntimeException("Could not create memcached client", e);
        }
    }

    protected MemcachedClient createCouchbaseClient(final MemcachedNodesManager memcachedNodesManager,
            final String memcachedProtocol, final String username, final String password, final long operationTimeout,
            final long maxReconnectDelay, final Statistics statistics) {
        try {
            final CouchbaseClientFactory factory = Class.forName("de.javakaffee.web.msm.CouchbaseClientFactory").asSubclass(CouchbaseClientFactory.class).newInstance();
            return factory.createCouchbaseClient(memcachedNodesManager, memcachedProtocol, username, password, operationTimeout, maxReconnectDelay, statistics);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected ConnectionFactory createConnectionFactory(final MemcachedNodesManager memcachedNodesManager,
            final ConnectionType connectionType, final String memcachedProtocol, final String username, final String password, final long operationTimeout,
            final long maxReconnectDelay, final Statistics statistics ) {
        if (PROTOCOL_BINARY.equals( memcachedProtocol )) {
            if (connectionType.isSASL()) {
                final AuthDescriptor authDescriptor = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));
                return memcachedNodesManager.isEncodeNodeIdInSessionId()
                        ? new SuffixLocatorBinaryConnectionFactory( memcachedNodesManager,
                                memcachedNodesManager.getSessionIdFormat(), statistics, operationTimeout, maxReconnectDelay,
                                authDescriptor)
                        : new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                .setAuthDescriptor(authDescriptor)
                                .setOpTimeout(operationTimeout)
                                .setMaxReconnectDelay(maxReconnectDelay)
                                .build();
            }
            else {
                return memcachedNodesManager.isEncodeNodeIdInSessionId() ? new SuffixLocatorBinaryConnectionFactory( memcachedNodesManager,
                        memcachedNodesManager.getSessionIdFormat(),
                        statistics, operationTimeout, maxReconnectDelay ) : new BinaryConnectionFactory() {
                    @Override
                    public long getOperationTimeout() {
                        return operationTimeout;
                    }
                    @Override
                    public long getMaxReconnectDelay() {
                        return maxReconnectDelay;
                    }
                };
            }
        }
        return memcachedNodesManager.isEncodeNodeIdInSessionId()
                ? new SuffixLocatorConnectionFactory( memcachedNodesManager, memcachedNodesManager.getSessionIdFormat(), statistics, operationTimeout, maxReconnectDelay )
                : new DefaultConnectionFactory() {
                    @Override
                    public long getOperationTimeout() {
                        return operationTimeout;
                    }
                    @Override
                    public long getMaxReconnectDelay() {
                        return maxReconnectDelay;
                    }
                };
    }

    static class ConnectionType {

        private final boolean couchbaseBucketConfig;
        private final String username;
        private final String password;
        public ConnectionType(final boolean couchbaseBucketConfig, final String username, final String password) {
            this.couchbaseBucketConfig = couchbaseBucketConfig;
            this.username = username;
            this.password = password;
        }
        public static ConnectionType valueOf(final boolean couchbaseBucketConfig, final String username, final String password) {
            return new ConnectionType(couchbaseBucketConfig, username, password);
        }
        boolean isCouchbaseBucketConfig() {
            return couchbaseBucketConfig;
        }
        boolean isSASL() {
            return !couchbaseBucketConfig && !isBlank(username) && !isBlank(password);
        }
        boolean isDefault() {
            return !isCouchbaseBucketConfig() && !isSASL();
        }

        boolean isBlank(final String value) {
            return value == null || value.trim().length() == 0;
        }
    }

}

class JvmRoutelessMemcachedClient extends MemcachedClient {

    public JvmRoutelessMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
        super(cf, addrs);
    }

    protected String stripJvmRoute(String key) {
        if (key == null) {
            return key;
        }

        String result = key;

        int jvmRouteStart = key.indexOf(".");
        if (jvmRouteStart > -1) {
            result = key.substring(0, jvmRouteStart);
        }
        return result;
    }

    protected String[] stripJvmRoute(String[] keys) {
        if (keys == null) {
            return keys;
        }

        return Stream.of(keys).map(this::stripJvmRoute).toArray(String[]::new);
    }

    protected Collection<String> stripJvmRoute(Collection<String> keys) {
        if (keys == null) {
            return keys;
        }

        return keys.stream().map(this::stripJvmRoute).collect(Collectors.toList());
    }

    @Override
    public <T> OperationFuture<Boolean> touch(String key, int exp) {
        return super.touch(stripJvmRoute(key), exp);
    }

    @Override
    public <T> OperationFuture<Boolean> touch(String key, int exp, Transcoder<T> tc) {
        return super.touch(stripJvmRoute(key), exp, tc);
    }

    @Override
    public OperationFuture<Boolean> append(long cas, String key, Object val) {
        return super.append(cas, stripJvmRoute(key), val);
    }

    @Override
    public OperationFuture<Boolean> append(String key, Object val) {
        return super.append(stripJvmRoute(key), val);
    }

    @Override
    public <T> OperationFuture<Boolean> append(long cas, String key, T val, Transcoder<T> tc) {
        return super.append(cas, stripJvmRoute(key), val, tc);
    }

    @Override
    public <T> OperationFuture<Boolean> append(String key, T val, Transcoder<T> tc) {
        return super.append(stripJvmRoute(key), val, tc);
    }

    @Override
    public OperationFuture<Boolean> prepend(long cas, String key, Object val) {
        return super.prepend(cas, stripJvmRoute(key), val);
    }

    @Override
    public OperationFuture<Boolean> prepend(String key, Object val) {
        return super.prepend(stripJvmRoute(key), val);
    }

    @Override
    public <T> OperationFuture<Boolean> prepend(long cas, String key, T val, Transcoder<T> tc) {
        return super.prepend(cas, stripJvmRoute(key), val, tc);
    }

    @Override
    public <T> OperationFuture<Boolean> prepend(String key, T val, Transcoder<T> tc) {
        return super.prepend(stripJvmRoute(key), val, tc);
    }

    @Override
    public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc) {
        return super.asyncCAS(stripJvmRoute(key), casId, value, tc);
    }

    @Override
    public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp, T value, Transcoder<T> tc) {
        return super.asyncCAS(stripJvmRoute(key), casId, exp, value, tc);
    }

    @Override
    public OperationFuture<CASResponse> asyncCAS(String key, long casId, Object value) {
        return super.asyncCAS(stripJvmRoute(key), casId, value);
    }

    @Override
    public OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp, Object value) {
        return super.asyncCAS(stripJvmRoute(key), casId, exp, value);
    }

    @Override
    public <T> CASResponse cas(String key, long casId, T value, Transcoder<T> tc) {
        return super.cas(stripJvmRoute(key), casId, value, tc);
    }

    @Override
    public <T> CASResponse cas(String key, long casId, int exp, T value, Transcoder<T> tc) {
        return super.cas(stripJvmRoute(key), casId, exp, value, tc);
    }

    @Override
    public CASResponse cas(String key, long casId, Object value) {
        return super.cas(stripJvmRoute(key), casId, value);
    }

    @Override
    public CASResponse cas(String key, long casId, int exp, Object value) {
        return super.cas(stripJvmRoute(key), casId, exp, value);
    }

    @Override
    public <T> OperationFuture<Boolean> add(String key, int exp, T o, Transcoder<T> tc) {
        return super.add(stripJvmRoute(key), exp, o, tc);
    }

    @Override
    public OperationFuture<Boolean> add(String key, int exp, Object o) {
        return super.add(stripJvmRoute(key), exp, o);
    }

    @Override
    public <T> OperationFuture<Boolean> set(String key, int exp, T o, Transcoder<T> tc) {
        return super.set(stripJvmRoute(key), exp, o, tc);
    }

    @Override
    public OperationFuture<Boolean> set(String key, int exp, Object o) {
        return super.set(stripJvmRoute(key), exp, o);
    }

    @Override
    public <T> OperationFuture<Boolean> replace(String key, int exp, T o, Transcoder<T> tc) {
        return super.replace(stripJvmRoute(key), exp, o, tc);
    }

    @Override
    public OperationFuture<Boolean> replace(String key, int exp, Object o) {
        return super.replace(stripJvmRoute(key), exp, o);
    }

    @Override
    public <T> GetFuture<T> asyncGet(String key, Transcoder<T> tc) {
        return super.asyncGet(stripJvmRoute(key), tc);
    }

    @Override
    public GetFuture<Object> asyncGet(String key) {
        return super.asyncGet(stripJvmRoute(key));
    }

    @Override
    public <T> OperationFuture<CASValue<T>> asyncGets(String key, Transcoder<T> tc) {
        return super.asyncGets(stripJvmRoute(key), tc);
    }

    @Override
    public OperationFuture<CASValue<Object>> asyncGets(String key) {
        return super.asyncGets(stripJvmRoute(key));
    }

    @Override
    public <T> CASValue<T> gets(String key, Transcoder<T> tc) {
        return super.gets(stripJvmRoute(key), tc);
    }

    @Override
    public <T> CASValue<T> getAndTouch(String key, int exp, Transcoder<T> tc) {
        return super.getAndTouch(stripJvmRoute(key), exp, tc);
    }

    @Override
    public CASValue<Object> getAndTouch(String key, int exp) {
        return super.getAndTouch(stripJvmRoute(key), exp);
    }

    @Override
    public CASValue<Object> gets(String key) {
        return super.gets(stripJvmRoute(key));
    }

    @Override
    public <T> T get(String key, Transcoder<T> tc) {
        return super.get(stripJvmRoute(key), tc);
    }

    @Override
    public Object get(String key) {
        return super.get(stripJvmRoute(key));
    }

    @Override
    public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter, Iterator<Transcoder<T>> tcIter) {
        return super.asyncGetBulk(stripJvmRoute(keyIter), tcIter);
    }

    @Override
    public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Iterator<Transcoder<T>> tcIter) {
        return super.asyncGetBulk(stripJvmRoute(keys), tcIter);
    }

    @Override
    public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter, Transcoder<T> tc) {
        return super.asyncGetBulk(stripJvmRoute(keyIter), tc);
    }

    @Override
    public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc) {
        return super.asyncGetBulk(stripJvmRoute(keys), tc);
    }

    @Override
    public BulkFuture<Map<String, Object>> asyncGetBulk(Iterator<String> keyIter) {
        return super.asyncGetBulk(stripJvmRoute(keyIter));
    }

    @Override
    public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
        return super.asyncGetBulk(stripJvmRoute(keys));
    }

    @Override
    public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc, String... keys) {
        return super.asyncGetBulk(tc, stripJvmRoute(keys));
    }

    @Override
    public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
        return super.asyncGetBulk(stripJvmRoute(keys));
    }

    @Override
    public OperationFuture<CASValue<Object>> asyncGetAndTouch(String key, int exp) {
        return super.asyncGetAndTouch(stripJvmRoute(key), exp);
    }

    @Override
    public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(String key, int exp, Transcoder<T> tc) {
        return super.asyncGetAndTouch(stripJvmRoute(key), exp, tc);
    }

    @Override
    public <T> Map<String, T> getBulk(Iterator<String> keyIter, Transcoder<T> tc) {
        return super.getBulk(stripJvmRoute(keyIter), tc);
    }

    @Override
    public Map<String, Object> getBulk(Iterator<String> keyIter) {
        return super.getBulk(stripJvmRoute(keyIter));
    }

    private Iterator<String> stripJvmRoute(Iterator<String> keyIter) {
        if (keyIter == null) {
            return null;
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(keyIter, Spliterator.NONNULL), false)
                .map(this::stripJvmRoute)
                .iterator();
    }

    @Override
    public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc) {
        return super.getBulk(stripJvmRoute(keys), tc);
    }

    @Override
    public Map<String, Object> getBulk(Collection<String> keys) {
        return super.getBulk(stripJvmRoute(keys));
    }

    @Override
    public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
        return super.getBulk(tc, stripJvmRoute(keys));
    }

    @Override
    public Map<String, Object> getBulk(String... keys) {
        return super.getBulk(stripJvmRoute(keys));
    }

    @Override
    public long incr(String key, long by) {
        return super.incr(stripJvmRoute(key), by);
    }

    @Override
    public long incr(String key, int by) {
        return super.incr(stripJvmRoute(key), by);
    }

    @Override
    public long decr(String key, long by) {
        return super.decr(stripJvmRoute(key), by);
    }

    @Override
    public long decr(String key, int by) {
        return super.decr(stripJvmRoute(key), by);
    }

    @Override
    public long incr(String key, long by, long def, int exp) {
        return super.incr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public long incr(String key, int by, long def, int exp) {
        return super.incr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public long decr(String key, long by, long def, int exp) {
        return super.decr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public long decr(String key, int by, long def, int exp) {
        return super.decr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, long by) {
        return super.asyncIncr(stripJvmRoute(key), by);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, int by) {
        return super.asyncIncr(stripJvmRoute(key), by);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, long by) {
        return super.asyncDecr(stripJvmRoute(key), by);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, int by) {
        return super.asyncDecr(stripJvmRoute(key), by);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, long by, long def, int exp) {
        return super.asyncIncr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, int by, long def, int exp) {
        return super.asyncIncr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, long by, long def, int exp) {
        return super.asyncDecr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, int by, long def, int exp) {
        return super.asyncDecr(stripJvmRoute(key), by, def, exp);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, long by, long def) {
        return super.asyncIncr(stripJvmRoute(key), by, def);
    }

    @Override
    public OperationFuture<Long> asyncIncr(String key, int by, long def) {
        return super.asyncIncr(stripJvmRoute(key), by, def);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, long by, long def) {
        return super.asyncDecr(stripJvmRoute(key), by, def);
    }

    @Override
    public OperationFuture<Long> asyncDecr(String key, int by, long def) {
        return super.asyncDecr(stripJvmRoute(key), by, def);
    }

    @Override
    public long incr(String key, long by, long def) {
        return super.incr(stripJvmRoute(key), by, def);
    }

    @Override
    public long incr(String key, int by, long def) {
        return super.incr(stripJvmRoute(key), by, def);
    }

    @Override
    public long decr(String key, long by, long def) {
        return super.decr(stripJvmRoute(key), by, def);
    }

    @Override
    public long decr(String key, int by, long def) {
        return super.decr(stripJvmRoute(key), by, def);
    }

    @Override
    public OperationFuture<Boolean> delete(String key, int hold) {
        return super.delete(stripJvmRoute(key), hold);
    }

    @Override
    public OperationFuture<Boolean> delete(String key) {
        return super.delete(stripJvmRoute(key));
    }

    @Override
    public OperationFuture<Boolean> delete(String key, long cas) {
        return super.delete(stripJvmRoute(key), cas);
    }
}
