/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class TransactionACIDExample {

    /** Cache name. */
    private static final String CACHE_NAME = TransactionACIDExample.class.getSimpleName();
    
    private static final TransactionIsolation TRANSACTION_ISOLATION = 
             REPEATABLE_READ;
//        READ_COMMITTED;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Parallel transactions example started.");

            CacheConfiguration<Integer, Account> cfg = new CacheConfiguration<>(CACHE_NAME);

            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initialize.
                cache.put(1, new Account(1, 1000));
                cache.put(2, new Account(2, 2000));

                System.out.println();
                System.out.println(">>> Accounts before transactions: ");
                System.out.println(">>> " + cache.get(1));
                System.out.println(">>> " + cache.get(2));

                // Run two parallel transactions
                executeParallelTransactionsIsolation(cache);
                
                executeParallelTransactionsLockDemo(cache);

                System.out.println();
                System.out.println(">>> Accounts after transactions: ");
                System.out.println(">>> " + cache.get(1));
                System.out.println(">>> " + cache.get(2));

                System.out.println(">>> Parallel transactions example finished.");
            }
        }
    }

    /**
     * Execute two parallel transactions demonstrating REPEATABLE_READ isolation.
     *
     * @param cache Cache instance.
     */
    /**
     * Execute two parallel transactions demonstrating REPEATABLE_READ isolation.
     *
     * @param cache Cache instance.
     */
    private static void executeParallelTransactionsIsolation(IgniteCache<Integer, Account> cache) throws InterruptedException {
        // 主线程等待两个事务都执行完成再结束
        CountDownLatch latch = new CountDownLatch(2);

        CyclicBarrier barrier1 = new CyclicBarrier(2, () -> System.out.println("Both transactions have read initial values"));

        CyclicBarrier barrier2 = new CyclicBarrier(2, () -> System.out.println("Transaction 1 has completed updates"));

        CyclicBarrier barrier3 = new CyclicBarrier(2, () -> System.out.println("Transaction 1 has committed"));

        // First transaction - transfer 100 from account 1 to account 2
        Thread tx1Thread = new Thread(() -> {
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, TRANSACTION_ISOLATION)) {
                System.out.println(">>> Transaction 1 started");

                // Read both accounts
                Account acct1 = cache.get(1);
                Account acct2 = cache.get(2);

                System.out.println(">>> Transaction 1 - Account 1 before update: " + acct1);
                System.out.println(">>> Transaction 1 - Account 2 before update: " + acct2);

                // 等待事务2第一次读取
                barrier1.await();

                // Perform transfer
                acct1.update(-100); // Deduct 100 from account 1
                acct2.update(100);  // Add 100 to account 2

                cache.put(1, acct1);
                cache.put(2, acct2);

                // Read again to demonstrate repeatable read - values should be consistent
                Account acct1After = cache.get(1);
                Account acct2After = cache.get(2);

                System.out.println(">>> Transaction 1 - Account 1 after update: " + acct1After);
                System.out.println(">>> Transaction 1 - Account 2 after update: " + acct2After);

                // 当前事务1更新完成但是尚未提交，等待事务2第二次读取
                barrier2.await();

                tx.commit();
                System.out.println(">>> Transaction 1 committed");

                // 等待事务2第三次读取
                barrier3.await();
            }
            catch (Exception e) {
                System.out.println(">>> Transaction 1 failed: " + e.getMessage());
            }
            finally {
                latch.countDown();
            }
        });

        // Second transaction - transfer 200 from account 2 to account 1
        Thread tx2Thread = new Thread(() -> {
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, TRANSACTION_ISOLATION)) {
                System.out.println(">>> Transaction 2 started");

                barrier1.await();
                
                // 第一次查询 - 在事务1更新前
                Account acct1 = cache.get(1);
                Account acct2 = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 before any update: " + acct1);
                System.out.println(">>> Transaction 2 - Account 2 before any update: " + acct2);

                // 等待事务1完成更新
                barrier2.await();
                
                // 第二次查询 - 在事务1更新后但提交前
                Account acct1AfterUpdate = cache.get(1);
                Account acct2AfterUpdate = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 after tx1 update: " + acct1AfterUpdate);
                System.out.println(">>> Transaction 2 - Account 2 after tx1 update: " + acct2AfterUpdate);

                // 等待事务1提交完成
                barrier3.await();
                
                // 第三次查询 - 在事务1提交后
                Account acct1AfterTx1 = cache.get(1);
                Account acct2AfterTx1 = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 after tx1 commit: " + acct1AfterTx1);
                System.out.println(">>> Transaction 2 - Account 2 after tx1 commit: " + acct2AfterTx1);

                // 执行事务2的转账操作
                acct1AfterTx1.update(-200); // Deduct 200 from account 1
                acct2AfterTx1.update(200);  // Add 200 to account 2

                cache.put(1, acct1AfterTx1);
                cache.put(2, acct2AfterTx1);

                Account acct1AfterTx2 = cache.get(1);
                Account acct2AfterTx2 = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 after tx2 update: " + acct1AfterTx2);
                System.out.println(">>> Transaction 2 - Account 2 after tx2 update: " + acct2AfterTx2);

                tx.commit();
                System.out.println(">>> Transaction 2 committed");
            }
            catch (Exception e) {
                System.out.println(">>> Transaction 2 failed: " + e.getMessage());
            }
            finally {
                latch.countDown();
            }
        });

        // Start both transactions
        tx1Thread.start();
        tx2Thread.start();

        // Wait for both transactions to complete
        latch.await();
    }

    /**
     * Execute two parallel transactions demonstrating lock waiting when updating the same row.
     *
     * @param cache Cache instance.
     */
    private static void executeParallelTransactionsLockDemo(IgniteCache<Integer, Account> cache) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);

        // 用于控制事务执行顺序的屏障
        CyclicBarrier startBarrier = new CyclicBarrier(2, () ->
                System.out.println("Both transactions started"));

        CyclicBarrier tx1UpdatedBarrier = new CyclicBarrier(2, () ->
                System.out.println("Transaction 1 updated account but not committed yet"));

        CyclicBarrier tx1CommittedBarrier = new CyclicBarrier(2, () ->
                System.out.println("Transaction 1 committed"));

        // First transaction - lock account 1
        Thread tx1Thread = new Thread(() -> {
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, TRANSACTION_ISOLATION)) {
                System.out.println(">>> Transaction 1 started - will lock account 1");

                // 读取账户1
                Account acct1 = cache.get(1);
                System.out.println(">>> Transaction 1 - Account 1 before update: " + acct1);

                // 等待事务2开始
                startBarrier.await();

                // 更新账户1，此时会锁定该记录
                acct1.update(-50); // 减少50
                cache.put(1, acct1);

                System.out.println(">>> Transaction 1 - Account 1 updated to: " + acct1);
                System.out.println(">>> Transaction 1 - holding lock on account 1");

                // 通知事务2可以尝试更新同一账户
                tx1UpdatedBarrier.await();

                // 等待一段时间，让事务2尝试获取锁
                Thread.sleep(2000);

                // 提交事务，释放锁
                tx.commit();
                System.out.println(">>> Transaction 1 committed - lock released");

                tx1CommittedBarrier.await();
            }
            catch (Exception e) {
                System.out.println(">>> Transaction 1 failed: " + e.getMessage());
                e.printStackTrace();
            }
            finally {
                latch.countDown();
            }
        });

        // Second transaction - try to update the same account (will wait for lock)
        Thread tx2Thread = new Thread(() -> {
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, TRANSACTION_ISOLATION)) {
                System.out.println(">>> Transaction 2 started - will try to update account 1");

                // 等待事务1开始
                startBarrier.await();

                // 等待事务1锁定账户1
                tx1UpdatedBarrier.await();

                System.out.println(">>> Transaction 2 - trying to read account 1 (will wait for lock)");

                // 尝试读取被锁定的账户 - 这将等待直到锁被释放
                long startTime = System.currentTimeMillis();
                Account acct1 = cache.get(1);
                long endTime = System.currentTimeMillis();

                System.out.println(">>> Transaction 2 - Account 1 read after " + (endTime - startTime) + "ms: " + acct1);
                System.out.println(">>> Transaction 2 - updating account 1");

                // 更新账户1
                acct1.update(-100); // 再减少100
                cache.put(1, acct1);

                System.out.println(">>> Transaction 2 - Account 1 updated to: " + acct1);

                // 等待事务1提交完成
                tx1CommittedBarrier.await();

                // 提交事务2
                tx.commit();
                System.out.println(">>> Transaction 2 committed");
            }
            catch (Exception e) {
                System.out.println(">>> Transaction 2 failed: " + e.getMessage());
                e.printStackTrace();
            }
            finally {
                latch.countDown();
            }
        });

        // Start both transactions
        tx1Thread.start();
        tx2Thread.start();

        // Wait for both transactions to complete
        latch.await();

        // 显示最终结果
        Account finalAcct1 = cache.get(1);
        System.out.println(">>> Final Account 1 balance: " + finalAcct1);
    }
    
    /**
     * Account.
     */
    private static class Account implements Serializable {
        /** Account ID. */
        private int id;

        /** Account balance. */
        private double balance;

        /**
         * @param id Account ID.
         * @param balance Balance.
         */
        Account(int id, double balance) {
            this.id = id;
            this.balance = balance;
        }

        /**
         * Change balance by specified amount.
         *
         * @param amount Amount to add to balance (may be negative).
         */
        void update(double amount) {
            balance += amount;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account [id=" + id + ", balance=$" + balance + ']';
        }
    }
}
