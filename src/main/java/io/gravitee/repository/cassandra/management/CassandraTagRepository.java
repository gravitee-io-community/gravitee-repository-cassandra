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
import io.gravitee.repository.management.api.TagRepository;
import io.gravitee.repository.management.model.Tag;
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
public class CassandraTagRepository implements TagRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraTagRepository.class);

    private final String TAGS_TABLE = "tags";

    @Autowired
    private Session session;

    @Override
    public Set<Tag> findAll() throws TechnicalException {
        LOGGER.debug("Find all Tags");

        final Statement select = QueryBuilder.select().all().from(TAGS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::tagFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<Tag> findById(String tagId) throws TechnicalException {
        LOGGER.debug("Find Tag by ID [{}]", tagId);

        Statement select = QueryBuilder.select().all().from(TAGS_TABLE).where(eq("id", tagId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(tagFromRow(row));
    }

    @Override
    public Tag create(Tag tag) throws TechnicalException {
        LOGGER.debug("Create Tag [{}]", tag.getName());

        Statement insert = QueryBuilder.insertInto(TAGS_TABLE)
                .values(new String[]{"id", "name", "description"},
                        new Object[]{tag.getId(), tag.getName(), tag.getDescription()});

        session.execute(insert);

        return findById(tag.getId()).orElse(null);
    }

    @Override
    public Tag update(Tag tag) throws TechnicalException {
        if (tag == null || tag.getName() == null) {
            throw new IllegalStateException("Tag to update must have a name");
        }

        if (!findById(tag.getId()).isPresent()) {
            throw new IllegalStateException(String.format("No tag found with name [%s]", tag.getId()));
        }

        LOGGER.debug("Update Tag [{}]", tag.getName());

        Statement update = QueryBuilder.update(TAGS_TABLE)
                .with(set("name", tag.getName()))
                .and(set("description", tag.getDescription()))
                .where(eq("id", tag.getId()));

        session.execute(update);

        return findById(tag.getId()).orElse(null);
    }

    @Override
    public void delete(String tagId) throws TechnicalException {
        LOGGER.debug("Delete Tag [{}]", tagId);

        Statement delete = QueryBuilder.delete().from(TAGS_TABLE).where(eq("id", tagId));

        session.execute(delete);
    }

    private Tag tagFromRow(Row row) {
        if (row != null) {
            final Tag tag = new Tag();
            tag.setId(row.getString("id"));
            tag.setName(row.getString("name"));
            tag.setDescription(row.getString("description"));
            return tag;
        }
        return null;
    }
}
