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

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class TransactionACIDExample {

    /** Cache name. */
    private static final String CACHE_NAME = TransactionACIDExample.class.getSimpleName();

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
                executeParallelTransactions(cache);

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
    private static void executeParallelTransactions(IgniteCache<Integer, Account> cache) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);

        // First transaction - transfer 100 from account 1 to account 2
        Thread tx1Thread = new Thread(() -> {
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                System.out.println(">>> Transaction 1 started");

                // Read both accounts
                Account acct1 = cache.get(1);
                Account acct2 = cache.get(2);

                System.out.println(">>> Transaction 1 - Account 1 before update: " + acct1);
                System.out.println(">>> Transaction 1 - Account 2 before update: " + acct2);

                // Simulate some processing time
                Thread.sleep(2000);

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

                tx.commit();
                System.out.println(">>> Transaction 1 committed");
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
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                System.out.println(">>> Transaction 2 started");

                // Read both accounts
                Account acct1 = cache.get(1);
                Account acct2 = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 before update: " + acct1);
                System.out.println(">>> Transaction 2 - Account 2 before update: " + acct2);

                // Simulate some processing time
                Thread.sleep(2000);

                // Perform transfer
                acct2.update(-200); // Deduct 200 from account 2
                acct1.update(200);  // Add 200 to account 1

                cache.put(2, acct2);
                cache.put(1, acct1);

                // Read again to demonstrate repeatable read - values should be consistent
                Account acct1After = cache.get(1);
                Account acct2After = cache.get(2);

                System.out.println(">>> Transaction 2 - Account 1 after update: " + acct1After);
                System.out.println(">>> Transaction 2 - Account 2 after update: " + acct2After);

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
