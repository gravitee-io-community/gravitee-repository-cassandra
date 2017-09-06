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
import io.gravitee.repository.management.api.GroupRepository;
import io.gravitee.repository.management.model.Group;
import io.gravitee.repository.management.model.GroupEvent;
import io.gravitee.repository.management.model.GroupEventRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraGroupRepository implements GroupRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraGroupRepository.class);

    private final String GROUPS_TABLE = "groups";

    @Autowired
    private Session session;

    @Override
    public Set<Group> findAll() throws TechnicalException {
        LOGGER.debug("Find all Groups");

        final Statement select = QueryBuilder.select().all().from(GROUPS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::groupFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Group> findByIds(Set<String> ids) throws TechnicalException {
        LOGGER.debug("Find Group by IDs [{}]", ids);

        final Statement select = QueryBuilder.select().all().from(GROUPS_TABLE).where(in("id", new ArrayList<>(ids)));
        final ResultSet resultSet = session.execute(select);
        List<Row> all = resultSet.all();

        return all.stream().map(this::groupFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<Group> findById(String groupId) throws TechnicalException {
        LOGGER.debug("Find Group by ID [{}]", groupId);

        final Statement select = QueryBuilder.select().all().from(GROUPS_TABLE).where(eq("id", groupId));
        final ResultSet resultSet = session.execute(select);

        final Row row = resultSet.one();

        return Optional.ofNullable(groupFromRow(row));
    }

    @Override
    public Group create(Group group) throws TechnicalException {
        LOGGER.debug("Create Group [{}]", group.getId());

        List<?> eventRules = group.getEventRules() == null
                ? Collections.emptyList()
                : group.getEventRules().
                stream().
                map(groupEventRule -> groupEventRule.getEvent().name()).
                collect(Collectors.toList());
        Statement insert = QueryBuilder.insertInto(GROUPS_TABLE)
                .values(new String[]{"id", "name", "administrators", "event_rules", "created_at", "updated_at"},
                        new Object[]{group.getId(), group.getName(),
                        group.getAdministrators(), eventRules,
                        group.getCreatedAt(), group.getUpdatedAt()});

        session.execute(insert);

        return findById(group.getId()).orElse(null);
    }

    @Override
    public Group update(Group group) throws TechnicalException {
        LOGGER.debug("Update Group [{}]", group.getId());

        List<?> eventRules = group.getEventRules() == null
                ? Collections.emptyList()
                : group.getEventRules().
                stream().
                map(groupEventRule -> groupEventRule.getEvent().name()).
                collect(Collectors.toList());

        Statement update = QueryBuilder.update(GROUPS_TABLE)
                .with(set("name", group.getName()))
                .and(set("administrators", group.getAdministrators()))
                .and(set("updated_at", group.getUpdatedAt()))
                .and(set("event_rules", eventRules))
                .where(eq("id", group.getId()));

        session.execute(update);

        return findById(group.getId()).orElse(null);
    }

    @Override
    public void delete(String groupId) throws TechnicalException {
        LOGGER.debug("Delete Group [{}]", groupId);

        Statement delete = QueryBuilder.delete().from(GROUPS_TABLE).where(eq("id", groupId));

        session.execute(delete);
    }

    private Group groupFromRow(Row row) {
        if (row != null) {
            final Group group = new Group();
            group.setId(row.getString("id"));
            group.setName(row.getString("name"));
            group.setAdministrators(row.getList("administrators", String.class));
            group.setCreatedAt(row.getTimestamp("created_at"));
            group.setUpdatedAt(row.getTimestamp("updated_at"));
            List<String> eventRules = row.getList("event_rules", String.class);
            if (eventRules != null && !eventRules.isEmpty()) {
                group.setEventRules(eventRules.stream().
                        map(event -> new GroupEventRule(GroupEvent.valueOf(event))).
                        collect(Collectors.toList()));
            }

            return group;
        }
        return null;
    }
}
