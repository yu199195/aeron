/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples.cluster.messages;

/**
 * User 消息 POJO。
 */
public class User
{
    public final long userId;
    public final String username;
    public final String email;
    public final int age;
    public final double balance;
    public final boolean isActive;

    public User(
        final long userId,
        final String username,
        final String email,
        final int age,
        final double balance,
        final boolean isActive)
    {
        this.userId = userId;
        this.username = username;
        this.email = email;
        this.age = age;
        this.balance = balance;
        this.isActive = isActive;
    }

    @Override
    public String toString()
    {
        return String.format("User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}",
            userId, username, email, age, balance, isActive);
    }
}
