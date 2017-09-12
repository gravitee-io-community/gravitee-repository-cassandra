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
import io.gravitee.repository.management.api.MetadataRepository;
import io.gravitee.repository.management.model.Metadata;
import io.gravitee.repository.management.model.MetadataFormat;
import io.gravitee.repository.management.model.MetadataReferenceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraMetadataRepository implements MetadataRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraMetadataRepository.class);

    private final String METADATAS_TABLE = "metadata";

    @Autowired
    private Session session;

    @Override
    public Metadata create(final Metadata metadata) throws TechnicalException {
        LOGGER.debug("Create metadata [{}]", metadata.getName());

        Statement insert = QueryBuilder.insertInto(METADATAS_TABLE)
                .values(new String[]{"key", "name", "format", "value", "reference_id", "reference_type", "created_at", "updated_at"},
                        new Object[]{metadata.getKey(), metadata.getName(), metadata.getFormat().name(),
                                metadata.getValue(), metadata.getReferenceId(),
                                metadata.getReferenceType().name(), metadata.getCreatedAt(), metadata.getUpdatedAt()});

        session.execute(insert);

        return findById(metadata.getKey(), metadata.getReferenceId(), metadata.getReferenceType()).orElse(null);
    }

    @Override
    public Metadata update(final Metadata metadata) throws TechnicalException {
        if (metadata == null || metadata.getName() == null) {
            throw new IllegalStateException("Metadata to update must have a name");
        }

        if (!findById(metadata.getKey(), metadata.getReferenceId(), metadata.getReferenceType()).isPresent()) {
            throw new IllegalStateException(String.format("No metadata found with key [%s], reference id [%s] and type [%s]",
                    metadata.getKey(), metadata.getReferenceId(), metadata.getReferenceType()));
        }

        LOGGER.debug("Update metadata [{}]", metadata.getName());

        Statement update = QueryBuilder.update(METADATAS_TABLE)
                .with(set("name", metadata.getName()))
                .and(set("format", metadata.getFormat().name()))
                .and(set("value", metadata.getValue()))
                .and(set("created_at", metadata.getCreatedAt()))
                .and(set("updated_at", metadata.getUpdatedAt()))
                .where(eq("key", metadata.getKey()))
                .and(eq("reference_id", metadata.getReferenceId()))
                .and(eq("reference_type", metadata.getReferenceType().name()));

        session.execute(update);

        return findById(metadata.getKey(), metadata.getReferenceId(), metadata.getReferenceType()).orElse(null);
    }

    @Override
    public void delete(final String key, final String referenceId, final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Delete metadata [{}, {}, {}]", key, referenceId, referenceType);

        Statement delete = QueryBuilder.delete().from(METADATAS_TABLE).where(eq("key", key))
                .and(eq("reference_id", referenceId)).and(eq("reference_type", referenceType.name()));

        session.execute(delete);
    }

    @Override
    public Optional<Metadata> findById(final String key, final String referenceId, final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find metadata by ID [{}, {}, {}]", key, referenceId, referenceType);

        Statement select = QueryBuilder.select().all().from(METADATAS_TABLE).allowFiltering()
                .where(eq("key", key)).and(eq("reference_id", referenceId))
                .and(eq("reference_type", referenceType.name()));
        final Row row = session.execute(select).one();

        return Optional.ofNullable(metadataFromRow(row));
    }

    @Override
    public List<Metadata> findByKeyAndReferenceType(final String key, final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find all metadata by key and reference type");
        final Statement select = QueryBuilder.select().all().from(METADATAS_TABLE).allowFiltering()
                .where(eq("key", key)).and(eq("reference_type", referenceType.name()));
        final ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(this::metadataFromRow).collect(Collectors.toList());
    }

    @Override
    public List<Metadata> findByReferenceType(final MetadataReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find all metadata by reference type");
        final Statement select = QueryBuilder.select().all().from(METADATAS_TABLE).allowFiltering()
                .where(eq("reference_type", referenceType.name()));
        final ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(this::metadataFromRow).collect(Collectors.toList());
    }

    @Override
    public List<Metadata> findByReferenceTypeAndReferenceId(final MetadataReferenceType referenceType, final String referenceId) throws TechnicalException {
        LOGGER.debug("Find all metadata by reference type and reference id");
        final Statement select = QueryBuilder.select().all().from(METADATAS_TABLE).allowFiltering()
                .where(eq("reference_type", referenceType.name())).and(eq("reference_id", referenceId));
        final ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(this::metadataFromRow).collect(Collectors.toList());
    }

    private Metadata metadataFromRow(Row row) {
        if (row != null) {
            final Metadata metadata = new Metadata();
            metadata.setKey(row.getString("key"));
            metadata.setName(row.getString("name"));
            metadata.setValue(row.getString("value"));
            metadata.setFormat(MetadataFormat.valueOf(row.getString("format")));
            metadata.setReferenceId(row.getString("reference_id"));
            metadata.setReferenceType(MetadataReferenceType.valueOf(row.getString("reference_type")));
            metadata.setCreatedAt(row.getTimestamp("created_at"));
            metadata.setUpdatedAt(row.getTimestamp("updated_at"));
            return metadata;
        }
        return null;
    }
}
