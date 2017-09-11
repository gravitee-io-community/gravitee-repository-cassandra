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
import io.gravitee.repository.management.api.ApplicationRepository;
import io.gravitee.repository.management.model.Application;
import io.gravitee.repository.management.model.ApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraApplicationRepository implements ApplicationRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraApplicationRepository.class);

    private final String APPLICATIONS_TABLE = "applications";

    @Autowired
    private Session session;

    @Override
    public Set<Application> findAll(ApplicationStatus... statuses) throws TechnicalException {
        LOGGER.debug("Find all Applications");

        final Statement select = QueryBuilder.select().all().from(APPLICATIONS_TABLE);
        final ResultSet resultSet = session.execute(select);

        Set<Application> applications = resultSet.all().stream().
                map(this::applicationFromRow).
                collect(Collectors.toSet());
        if (statuses != null && statuses.length > 0) {
            List<ApplicationStatus> applicationStatuses = Arrays.asList(statuses);
            applications = applications.stream().
                    filter(app -> applicationStatuses.contains(app.getStatus())).
                    collect(Collectors.toSet());
        }

        LOGGER.debug("Found {} applications", applications.size());
        return applications;
    }

    @Override
    public Set<Application> findByIds(List<String> ids) throws TechnicalException {
        LOGGER.debug("Find Applications by ID list");

        // may be wrong : should loop through list and add resultsets to a list of resultset
        final Statement select = QueryBuilder.select().all().from(APPLICATIONS_TABLE).where(in("id", ids));
        final ResultSet resultSet = session.execute(select);

        final Set<Application> applications = resultSet.all().stream().map(this::applicationFromRow).collect(Collectors.toSet());

        LOGGER.debug("Found {} applications", applications.size());
        return applications;
    }

    @Override
    public Set<Application> findByGroups(List<String> groups, ApplicationStatus ... statuses) throws TechnicalException {
        LOGGER.debug("Find Applications by Group list");

        // may be wrong : should loop through list and add resultsets to a list of resultset
        final Statement select = QueryBuilder.select().all().from(APPLICATIONS_TABLE).allowFiltering()
                .where(in("group", groups));

        final ResultSet resultSet = session.execute(select);

        Set<Application> applications = resultSet.all().stream().map(this::applicationFromRow).collect(Collectors.toSet());
        if (statuses != null && statuses.length > 0) {
            List<ApplicationStatus> applicationStatuses = Arrays.asList(statuses);
            applications = applications.stream().
                    filter(app -> applicationStatuses.contains(app.getStatus())).
                    collect(Collectors.toSet());
        }
        return applications;
    }

    @Override
    public Optional<Application> findById(String applicationId) throws TechnicalException {
        LOGGER.debug("Find Application by ID [{}]", applicationId);

        final Statement select = QueryBuilder.select().all().from(APPLICATIONS_TABLE).where(eq("id", applicationId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(applicationFromRow(row));
    }

    @Override
    public Set<Application> findByName(String partialName) throws TechnicalException {
        LOGGER.debug("Find Application by partial name [{}]", partialName);

        final Statement select = QueryBuilder.select("id", "name").from(APPLICATIONS_TABLE);

        final ResultSet resultSet = session.execute(select);

        final List<String> applicationIds = resultSet.all().stream()
                .filter(row -> row.getString("name").toLowerCase().contains(partialName.toLowerCase()))
                .map(row -> row.getString("id"))
                .collect(Collectors.toList());

        return findByIds(applicationIds);
    }

    @Override
    public Application create(Application application) throws TechnicalException {
        LOGGER.debug("Create Application [{}]", application.getId());

        Statement insert = QueryBuilder.insertInto(APPLICATIONS_TABLE)
                .values(new String[]{"id", "name", "description", "type", "created_at", "updated_at", "group", "status"},
                        new Object[]{application.getId(), application.getName(), application.getDescription(),
                                application.getType(), application.getCreatedAt(), application.getUpdatedAt(),
                                application.getGroup(), application.getStatus().toString()});

        session.execute(insert);

        return findById(application.getId()).orElse(null);
    }

    @Override
    public Application update(Application application) throws TechnicalException {
        LOGGER.debug("Update Application [{}]", application.getId());

        Statement update = QueryBuilder.update(APPLICATIONS_TABLE)
                .with(set("name", application.getName()))
                .and(set("description", application.getDescription()))
                .and(set("type", application.getType()))
                .and(set("created_at", application.getCreatedAt()))
                .and(set("updated_at", application.getUpdatedAt()))
                .and(set("group", application.getGroup()))
                .and(set("status", application.getStatus().toString()))
                .where(eq("id", application.getId()));

        session.execute(update);

        return findById(application.getId()).orElse(null);
    }

    @Override
    public void delete(String applicationId) throws TechnicalException {
        LOGGER.debug("Delete Application [{}]", applicationId);

        Statement delete = QueryBuilder.delete().from(APPLICATIONS_TABLE).where(eq("id", applicationId));

        session.execute(delete);
    }

    private Application applicationFromRow(Row row) {
        if (row != null) {
            final Application application = new Application();
            application.setId(row.getString("id"));
            application.setName(row.getString("name"));
            application.setDescription(row.getString("description"));
            application.setType(row.getString("type"));
            application.setCreatedAt(row.getTimestamp("created_at"));
            application.setUpdatedAt(row.getTimestamp("updated_at"));
            application.setGroup(row.getString("group"));
            application.setStatus(ApplicationStatus.valueOf(row.getString("status")));
            return application;
        }
        return null;
    }

}
