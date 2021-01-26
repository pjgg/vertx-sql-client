/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.vertx.sqlclient.impl.pool;

import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.impl.clientconnection.ConnectResult;
import io.vertx.core.net.impl.clientconnection.Lease;
import io.vertx.core.net.impl.pool.ConnectionEventListener;
import io.vertx.core.net.impl.pool.ConnectionPool;
import io.vertx.core.net.impl.pool.Connector;
import io.vertx.core.net.impl.pool.SimpleConnectionPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.impl.Connection;
import io.vertx.sqlclient.impl.ConnectionFactory;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.spi.DatabaseMetadata;
import io.vertx.core.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Todo :
 *
 * - handle timeout when acquiring a connection
 * - for per statement pooling, have several physical connection and use the less busy one to avoid head of line blocking effect
 *
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class SqlConnectionPool {

  private final ConnectionFactory factory;
  private final ContextInternal context;
  private final ConnectionPool<PooledConnection> pool;
  private final int pipeliningLimit;
  private final int maxSize;
  private boolean closed;

  public SqlConnectionPool(ConnectionFactory factory, Context context, int maxSize, int pipeliningLimit, int maxWaitQueueSize) {
    Objects.requireNonNull(factory, "No null connector");
    if (maxSize < 1) {
      throw new IllegalArgumentException("Pool max size must be > 0");
    }
    if (pipeliningLimit < 1) {
      throw new IllegalArgumentException("Pipelining limit must be > 0");
    }
    this.pool = new SimpleConnectionPool<>(connector, maxSize, maxSize, maxWaitQueueSize);
    this.context = (ContextInternal) context;
    this.pipeliningLimit = pipeliningLimit;
    this.maxSize = maxSize;
    this.factory = factory;
  }

  private final Connector<PooledConnection> connector = new Connector<PooledConnection>() {
    @Override
    public void connect(EventLoopContext context, ConnectionEventListener listener, Handler<AsyncResult<ConnectResult<PooledConnection>>> handler) {
      PromiseInternal<Connection> promise = context.promise();
      factory.connect(promise);
      promise.future()
        .map(connection -> {
          PooledConnection pooled = new PooledConnection(connection, listener);
          connection.init(pooled);
          return new ConnectResult<>(pooled, pipeliningLimit, 1);
        })
        .onComplete(handler);
    }

    @Override
    public boolean isValid(PooledConnection connection) {
      return true;
    }
  };

  public int available() {
    return maxSize - pool.size();
  }

  public int size() {
    return pool.size();
  }

  public <R> Future<R> execute(ContextInternal context, CommandBase<R> cmd) {
    Promise<R> promise = context.promise();
    pool.acquire((EventLoopContext) context, 1, ar -> {
      if (ar.succeeded()) {
        Lease<PooledConnection> lease = ar.result();
        PooledConnection pooled = lease.get();
        pooled.schedule(context, cmd)
          .onComplete(promise)
          .onComplete(v -> {
            lease.recycle();
          });
      } else {
        promise.fail(ar.cause());
      }
    });
    return promise.future();
  }

  public void acquire(ContextInternal context, Handler<AsyncResult<Connection>> waiter) {
    pool.acquire((EventLoopContext) context, 1, ar -> {
      if (ar.succeeded()) {
        Lease<PooledConnection> lease = ar.result();
        PooledConnection pooled = lease.get();
        pooled.lease = lease;
        waiter.handle(Future.succeededFuture(pooled));
      } else {
        waiter.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public Future<Void> close() {
    PromiseInternal<Void> promise = context.promise();
    context.emit(promise, this::close);
    return promise.future();
  }

  public void close(Promise<Void> promise) {
    if (closed) {
      promise.fail("Connection pool already closed");
      return;
    }
    closed = true;
    List<Future> futures = pool
      .close()
      .stream().map(f -> f
        .flatMap(c -> {
          Promise<Void> p = Promise.promise();
          c.close(p);
          return p.future();
        }))
      .collect(Collectors.toList());
    CompositeFuture
      .join(futures)
      .<Void>mapEmpty()
      .onComplete(promise);
  }

  private class PooledConnection implements Connection, Connection.Holder  {

    private final Connection conn;
    private final ConnectionEventListener listener;
    private Holder holder;
    private Lease<PooledConnection> lease;

    PooledConnection(Connection conn, ConnectionEventListener listener) {
      this.conn = conn;
      this.listener = listener;
    }

    @Override
    public boolean isSsl() {
      return conn.isSsl();
    }

    @Override
    public DatabaseMetadata getDatabaseMetaData() {
      return conn.getDatabaseMetaData();
    }

    @Override
    public <R> Future<R> schedule(ContextInternal context, CommandBase<R> cmd) {
      return conn.schedule(context, cmd);
    }

    /**
     * Close the underlying connection
     */
    private void close(Promise<Void> promise) {
      conn.close(this, promise);
    }

    @Override
    public void init(Holder holder) {
      if (this.holder != null) {
        throw new IllegalStateException();
      }
      this.holder = holder;
    }

    @Override
    public void close(Holder holder, Promise<Void> promise) {
      if (context != null) {
        context.emit(v -> doClose(holder, promise));
      } else {
        doClose(holder, promise);
      }
    }

    private void doClose(Holder holder, Promise<Void> promise) {
      if (holder != this.holder) {
        String msg;
        if (this.holder == null) {
          msg = "Connection released twice";
        } else {
          msg = "Connection released by " + holder + " owned by " + this.holder;
        }
        // Log it ?
        promise.fail(msg);
      } else {
        Lease<PooledConnection> l = this.lease;
        this.holder = null;
        this.lease = null;
        l.recycle();
        promise.complete();
      }
    }

    @Override
    public void handleClosed() {
      if (holder != null) {
        holder.handleClosed();
      }
      listener.remove();
    }

    @Override
    public void handleEvent(Object event) {
      if (holder != null) {
        holder.handleEvent(event);
      }
    }

    @Override
    public void handleException(Throwable err) {
      if (holder != null) {
        holder.handleException(err);
      }
    }

    @Override
    public int getProcessId() {
      return conn.getProcessId();
    }

    @Override
    public int getSecretKey() {
      return conn.getSecretKey();
    }
  }
}
