/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.cassandra.management;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.UserRepository;
import io.gravitee.repository.management.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraUserRepository implements UserRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraUserRepository.class);

    private final String USERS_TABLE = "users";

    @Autowired
    private Session session;

    @Override
    public User create(User user) throws TechnicalException {
        LOGGER.debug("Create User [{}]", user.getUsername());

        Statement insert = QueryBuilder.insertInto(USERS_TABLE)
                .values(new String[]{"username", "source", "source_id", "password", "email", "firstname", "lastname",
                        "created_at", "updated_at", "last_connection_at", "picture"},
                        new Object[]{user.getUsername(), user.getSource(), user.getSourceId(), user.getPassword(),
                        user.getEmail(), user.getFirstname(), user.getLastname(),
                        user.getCreatedAt(), user.getUpdatedAt(), user.getLastConnectionAt(), user.getPicture()});

        session.execute(insert);

        final Optional<User> createdUser = findByUsername(user.getUsername());

        if (createdUser.isPresent()) {
            return createdUser.get();
        }

        return null;
    }

    @Override
    public User update(User user) throws TechnicalException {
        LOGGER.debug("Update User [{}]", user.getUsername());

        Statement update = QueryBuilder.update(USERS_TABLE)
                .with(set("source", user.getSource()))
                .and(set("source_id", user.getSourceId()))
                .and(set("password", user.getPassword()))
                .and(set("email", user.getEmail()))
                .and(set("firstname", user.getFirstname()))
                .and(set("lastname", user.getLastname()))
                .and(set("updated_at", user.getUpdatedAt()))
                .and(set("last_connection_at", user.getLastConnectionAt()))
                .and(set("picture", user.getPicture()))
                .where(eq("username", user.getUsername()));

        session.execute(update);

        final Optional<User> updatedUser = findByUsername(user.getUsername());

        if (updatedUser.isPresent()) {
            LOGGER.debug("User updated");
            return updatedUser.get();
        }

        return null;
    }

    @Override
    public Optional<User> findByUsername(String username) throws TechnicalException {
        LOGGER.debug("Find User by Username [{}]", username);

        final Statement select = QueryBuilder.select().all().from(USERS_TABLE).where(eq("username", username));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(userFromRow(row));
    }

    @Override
    public Set<User> findByUsernames(List<String> usernames) throws TechnicalException {
        LOGGER.debug("Find User by Username list");

        final Statement select = QueryBuilder.select().all().from(USERS_TABLE).where(in("username", usernames));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::userFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<User> findAll() throws TechnicalException {
        LOGGER.debug("Find all Users");

        final Statement select = QueryBuilder.select().all().from(USERS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::userFromRow).collect(Collectors.toSet());
    }

    private User userFromRow(Row row) {
        if (row != null) {
            final User user = new User();
            user.setUsername(row.getString("username"));
            user.setSource(row.getString("source"));
            user.setSourceId(row.getString("source_id"));
            user.setPassword(row.getString("password"));
            user.setEmail(row.getString("email"));
            user.setFirstname(row.getString("firstname"));
            user.setLastname(row.getString("lastname"));
            user.setCreatedAt(row.getTimestamp("created_at"));
            user.setUpdatedAt(row.getTimestamp("updated_at"));
            user.setLastConnectionAt(row.getTimestamp("last_connection_at"));
            user.setPicture(row.getString("picture"));
            return user;
        }
        return null;
    }
}
