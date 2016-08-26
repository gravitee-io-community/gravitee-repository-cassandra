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
import io.gravitee.repository.management.api.ViewRepository;
import io.gravitee.repository.management.model.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraViewRepository implements ViewRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraViewRepository.class);

    private final String VIEWS_TABLE = "views";

    @Autowired
    private Session session;

    @Override
    public Set<View> findAll() throws TechnicalException {
        LOGGER.debug("Find all Views");

        final Statement select = QueryBuilder.select().all().from(VIEWS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::viewFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<View> findById(String viewId) throws TechnicalException {
        LOGGER.debug("Find View by ID [{}]", viewId);

        Statement select = QueryBuilder.select().all().from(VIEWS_TABLE).where(eq("id", viewId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(viewFromRow(row));
    }

    @Override
    public View create(View view) throws TechnicalException {
        LOGGER.debug("Create View [{}]", view.getName());

        Statement insert = QueryBuilder.insertInto(VIEWS_TABLE)
                .values(new String[]{"id", "name", "description"},
                        new Object[]{view.getId(), view.getName(), view.getDescription()});

        session.execute(insert);

        return findById(view.getId()).orElse(null);
    }

    @Override
    public View update(View view) throws TechnicalException {
        LOGGER.debug("Update View [{}]", view.getName());

        Statement update = QueryBuilder.update(VIEWS_TABLE)
                .with(set("name", view.getName()))
                .and(set("description", view.getDescription()))
                .where(eq("id", view.getId()));

        session.execute(update);

        return findById(view.getId()).orElse(null);
    }

    @Override
    public void delete(String viewId) throws TechnicalException {
        LOGGER.debug("Delete view [{}]", viewId);

        Statement delete = QueryBuilder.delete().from(VIEWS_TABLE).where(eq("id", viewId));

        session.execute(delete);
    }

    private View viewFromRow(Row row) {
        if (row != null) {
            final View view = new View();
            view.setId(row.getString("id"));
            view.setName(row.getString("name"));
            view.setDescription(row.getString("description"));
            return view;
        }
        return null;
    }

}
