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
import io.gravitee.repository.management.api.ApiRepository;
import io.gravitee.repository.management.model.Api;
import io.gravitee.repository.management.model.LifecycleState;
import io.gravitee.repository.management.model.Visibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraApiRepository implements ApiRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraApiRepository.class);

    private final String APIS_TABLE = "apis";

    @Autowired
    private Session session;

    @Override
    public Set<Api> findAll() throws TechnicalException {
        LOGGER.debug("Find all Apis");

        final Statement select = select().all().from(APIS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Api> findByVisibility(Visibility visibility) throws TechnicalException {
        LOGGER.debug("Find Apis by Visibility [{}]", visibility.toString());

        final Statement select = select().all().from(APIS_TABLE).allowFiltering().where(eq("visibility", visibility.toString()));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Api> findByIds(List<String> apiIds) throws TechnicalException {
        LOGGER.debug("Find Apis by ID list");

        final Statement select = select().all().from(APIS_TABLE).where(in("id", apiIds));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Api> findByGroups(List<String> groups) throws TechnicalException {
        LOGGER.debug("Find Apis by Group list");

        final Statement select = select().all().from(APIS_TABLE).where(in("id", groups));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<Api> findById(String apiId) throws TechnicalException {
        LOGGER.debug("Find Api by ID [{}]", apiId);

        final Statement select = select().all().from(APIS_TABLE).where(eq("id", apiId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(apiFromRow(row));
    }

    @Override
    public Api create(Api api) throws TechnicalException {
        LOGGER.debug("Create Api [{}]", api.getId());

        final Visibility apiVisibility = api.getVisibility();
        String visibility = null;
        if (apiVisibility != null) {
            visibility = apiVisibility.toString();
        }
        Statement insert = QueryBuilder.insertInto(APIS_TABLE)
                .values(new String[]{"id", "name", "description", "version", "definition", "deployed_at", "created_at",
                                "updated_at", "visibility", "lifecycle_state", "picture", "group", "views"},
                        new Object[]{api.getId(), api.getName(), api.getDescription(), api.getVersion(), api.getDefinition(),
                                api.getDeployedAt(), api.getCreatedAt(), api.getUpdatedAt(), visibility,
                                api.getLifecycleState().toString(), api.getPicture(), api.getGroup(), api.getViews()});

        session.execute(insert);

        return findById(api.getId()).orElse(null);
    }

    @Override
    public Api update(Api api) throws TechnicalException {
        LOGGER.debug("Update Api [{}]", api.getId());

        Statement update = QueryBuilder.update(APIS_TABLE)
                .with(set("name", api.getName()))
                .and(set("description", api.getDescription()))
                .and(set("version", api.getVersion()))
                .and(set("definition", api.getDefinition()))
                .and(set("deployed_at", api.getDeployedAt()))
                .and(set("updated_at", api.getUpdatedAt()))
                .and(set("visibility", api.getVisibility().toString()))
                .and(set("lifecycle_state", api.getLifecycleState().toString()))
                .and(set("picture", api.getPicture()))
                .and(set("group", api.getGroup()))
                .and(set("views", api.getViews()))
                .where(eq("id", api.getId()));

        session.execute(update);

        return findById(api.getId()).orElse(null);
    }

    @Override
    public void delete(String apiId) throws TechnicalException {
        LOGGER.debug("Delete Api [{}]", apiId);

        Statement delete = QueryBuilder.delete().from(APIS_TABLE).where(eq("id", apiId));

        session.execute(delete);
    }

    private Api apiFromRow(Row row) {
        if (row != null) {
            final String apiVisibility = row.getString("visibility");
            Visibility visibility = null;
            if (apiVisibility != null) {
                visibility = Visibility.valueOf(apiVisibility.toUpperCase());
            }

            final Api api = new Api();
            api.setId(row.getString("id"));
            api.setName(row.getString("name"));
            api.setDescription(row.getString("description"));
            api.setVersion(row.getString("version"));
            api.setDefinition(row.getString("definition"));
            api.setDeployedAt(row.getTimestamp("deployed_at"));
            api.setCreatedAt(row.getTimestamp("created_at"));
            api.setUpdatedAt(row.getTimestamp("updated_at"));
            api.setVisibility(visibility);
            api.setLifecycleState(LifecycleState.valueOf(row.getString("lifecycle_state").toUpperCase()));
            api.setPicture(row.getString("picture"));
            api.setGroup(row.getString("group"));
            api.setViews(row.getSet("views", String.class));
            return api;
        }
        return null;
    }
}
