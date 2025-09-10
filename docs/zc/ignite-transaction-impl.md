# Apache Ignite 事务实现原理详解

## 1. 引言

Apache Ignite 是一个高性能、分布式内存计算平台，支持完整的 ACID 事务特性。本文将基于提供的 trace 信息和源码分析，深入探讨 Ignite 事务的实现机制，特别是事务隔离级别的保证方式。

## 2. 事务核心组件

### 2.1 事务管理器 (IgniteTxManager)

IgniteTxManager 是事务管理的核心组件，负责事务的创建、管理和协调。主要功能包括：

- 创建新的事务实例
- 管理事务生命周期
- 维护事务映射关系
- 处理事务提交和回滚

```java
public GridNearTxLocal newTx(
    boolean implicit,
    boolean implicitSingle,
    @Nullable GridCacheContext sysCacheCtx,
    TransactionConcurrency concurrency,
    TransactionIsolation isolation,
    long timeout,
    boolean storeEnabled,
    int txSize,
    @Nullable String lb,
    @Nullable Map<String, String> appAttrs
) {
    // ...
    GridNearTxLocal tx = new GridNearTxLocal(
        cctx,
        implicit,
        implicitSingle,
        sysCacheCtx != null,
        sysCacheCtx != null ? sysCacheCtx.ioPolicy() : SYSTEM_POOL,
        concurrency,
        isolation,
        timeout,
        storeEnabled,
        txSize,
        securitySubjectId(cctx),
        taskNameHash,
        lb,
        txDumpsThrottling
    );
    // ...
    return onCreated(sysCacheCtx, tx);
}
```

### 2.2 事务实现 (GridNearTxLocal)

GridNearTxLocal 是 Ignite 中近端事务的具体实现，代表了用户直接交互的事务对象。它负责：

- 维护事务状态
- 管理事务条目
- 执行事务准备和提交操作
- 管理分布式锁

### 2.3 事务代理 (TransactionProxyImpl)

TransactionProxyImpl 是面向用户的事务代理对象，提供了对外的事务操作接口。所有用户对事务的操作都通过这个代理进行，它负责：

- 线程安全控制
- 分布式追踪支持
- 事务状态管理
- 提交和回滚操作

## 3. 事务启动流程分析

根据 trace 信息，事务启动流程如下：

1. **获取事务管理器**：通过 `IgniteKernal.transactions()` 获取事务管理器实例
2. **线程安全检查**：通过 `GridKernalGatewayImpl.readLock()` 确保线程安全
3. **集群状态检查**：验证集群是否处于活跃状态
4. **创建事务实例**：调用 `IgniteTransactionsImpl.txStart()` 创建新事务
5. **初始化事务**：在 `IgniteTransactionsImpl.txStart0()` 中初始化事务参数
6. **事务注册**：通过 `IgniteTxManager.onCreated()` 将事务注册到事务管理器中

```java
private GridNearTxLocal txStart0(
    TransactionConcurrency concurrency,
    TransactionIsolation isolation,
    long timeout,
    int txSize,
    @Nullable GridCacheContext sysCacheCtx
) {
    cctx.kernalContext().gateway().readLock();

    Span span = cctx.kernalContext().tracing().create(TX, null, lb);

    MTC.supportInitial(span);

    span.addTag("isolation", isolation::name);
    span.addTag("concurrency", concurrency::name);
    span.addTag("timeout", () -> String.valueOf(timeout));

    if (lb != null)
        span.addTag("label", () -> lb);

    try {
        GridNearTxLocal tx = cctx.tm().userTx(sysCacheCtx);

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        tx = cctx.tm().newTx(
            false,
            false,
            sysCacheCtx,
            concurrency,
            isolation,
            timeout,
            true,
            txSize,
            lb,
            appAttrs
        );

        assert tx != null;
        return tx;
    }
    finally {
        cctx.kernalContext().gateway().readUnlock();
    }
}
```

## 4. 事务执行流程

### 4.1 事务准备阶段

当事务执行提交操作时，会先进入准备阶段：

```java
public IgniteInternalFuture<?> prepareNearTxLocal() {
    enterSystemSection();

    // We assume that prepare start time should be set only once for the transaction.
    prepareStartTime.compareAndSet(0, System.nanoTime());

    GridNearTxPrepareFutureAdapter fut = (GridNearTxPrepareFutureAdapter)prepFut;

    if (fut == null) {
        long timeout = remainingTime();

        // Future must be created before any exception can be thrown.
        if (optimistic()) {
            fut = serializable() ?
                new GridNearOptimisticSerializableTxPrepareFuture(cctx, this) :
                new GridNearOptimisticTxPrepareFuture(cctx, this);
        }
        else
            fut = new GridNearPessimisticTxPrepareFuture(cctx, this);

        if (!PREP_FUT_UPD.compareAndSet(this, null, fut))
            return prepFut;

        if (trackTimeout)
            prepFut.listen(this::removeTimeoutHandler);

        if (timeout == -1) {
            fut.onDone(this, timeoutException());

            return fut;
        }
    }
    else
        // Prepare was called explicitly.
        return fut;

    mapExplicitLocks();

    if (cctx.kernalContext().deploy().enabled() && deploymentLdrId != null)
        U.restoreDeploymentContext(cctx.kernalContext(), deploymentLdrId);

    fut.prepare();

    return fut;
}
```

### 4.2 事务提交阶段

事务提交通过 `commit()` 方法触发，最终调用 `GridCacheSharedContext.commitTxAsync()`：

```java
@Override public void commit() {
    Span span = MTC.span();

    try (TraceSurroundings ignored =
             MTC.support(cctx.kernalContext().tracing().create(TX_COMMIT, span))) {
        enter();

        try {
            IgniteInternalFuture<IgniteInternalTx> commitFut = cctx.commitTxAsync(tx);

            if (async)
                saveFuture(commitFut);
            else
                commitFut.get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            leave();
        }
    }
    finally {
        span.end();
    }
}
```

## 5. 分布式隔离级别实现

Ignite 支持三种事务隔离级别：

### 5.1 READ_COMMITTED（读已提交）

这是最低的隔离级别。在该级别下：

- 读操作总是从全局内存或持久化存储中获取最新提交的值
- 同一事务中对同一键的多次读取可能返回不同值，因为其他事务可能已更新该值
- 不提供可重复读保证

### 5.2 REPEATABLE_READ（可重复读）

这是 Ignite 的默认隔离级别。在该级别下：

- 如果一个值在事务中被读取一次，后续的所有读取都将返回相同的事务内值
- 读取的值存储在事务内存中，确保同一事务中对同一键的多次访问返回一致的值
- 对于悲观并发模型，会在访问值之前获取键上的锁

```java
/**
 * Repeatable read isolation level. This isolation level means that
 * if a value was read once within transaction, then all consecutive
 * reads will provide the same in-transaction value. With this
 * isolation level accessed values are stored within in-transaction
 * memory, so consecutive access to the same key within the same
 * transaction will always return the value that was previously read
 * or updated within this transaction. If concurrency is
 * PESSIMISTIC, then a lock on the key will be acquired prior to accessing the value.
 */
REPEATABLE_READ = 1,
```

### 5.3 SERIALIZABLE（可串行化）

这是最高的隔离级别。在该级别下：

- 所有事务都以完全隔离的方式执行，就像系统中的所有事务都是串行执行的一样
- 读访问的行为与 REPEATABLE_READ 级别相同
- 在乐观并发模式下，如果某些事务无法串行化隔离，系统会选择一个胜者，其他冲突的事务将抛出 TransactionOptimisticException

```java
/**
 * Serializable isolation level. This isolation level means that all
 * transactions occur in a completely isolated fashion, as if all
 * transactions in the system had executed serially, one after the
 * other. Read access with this level happens the same way as with
 * REPEATABLE_READ level. However, in OPTIMISTIC mode, if some transactions
 * cannot be serially isolated from each other, then one winner will
 * be picked and the other transactions in conflict will result in
 * IgniteError being thrown.
 */
SERIALIZABLE = 2
```

## 6. 并发控制模型

Ignite 支持两种并发控制模型：

### 6.1 悲观锁 (PESSIMISTIC)

在悲观锁模式下：

- 在执行操作前获取数据的锁
- 适用于写操作较多的场景
- 可能导致死锁，但能提供更强的一致性保证

### 6.2 乐观锁 (OPTIMISTIC)

在乐观锁模式下：

- 所有缓存操作在提交前都不会分发到其他节点
- 提交时发送 PREPARE 消息获取事务锁
- 如果事务冲突，会抛出 TransactionOptimisticException

## 7. 分布式事务协调

Ignite 使用两阶段提交 (2PC) 协议来保证分布式事务的原子性：

1. **准备阶段**：事务协调器向所有参与者发送准备请求，参与者执行事务操作并锁定资源
2. **提交阶段**：如果所有参与者都准备成功，协调器发送提交请求，否则发送回滚请求

## 8. 总结

Apache Ignite 通过以下机制实现事务支持：

1. **分层架构**：通过事务管理器、事务实现和事务代理的分层设计，提供了清晰的职责分离
2. **线程安全**：通过读写锁机制保证多线程环境下的安全性
3. **分布式追踪**：集成追踪机制，便于监控和调试
4. **多种隔离级别**：支持 READ_COMMITTED、REPEATABLE_READ 和 SERIALIZABLE 三种隔离级别
5. **并发控制**：支持悲观锁和乐观锁两种并发模型
6. **两阶段提交**：使用 2PC 协议保证分布式事务的原子性

这些机制的组合使得 Ignite 能够在分布式环境中提供强一致性的事务支持，满足企业级应用的需求。