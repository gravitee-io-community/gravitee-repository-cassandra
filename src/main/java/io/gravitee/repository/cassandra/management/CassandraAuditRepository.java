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
import com.datastax.driver.core.querybuilder.Select;
import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.AuditRepository;
import io.gravitee.repository.management.api.search.AuditCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Audit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * @author Nicolas GERAUD (nicolas.geraud at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraAuditRepository implements AuditRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraAuditRepository.class);

    private final String AUDITS_TABLE = "audits";

    @Autowired
    private Session session;

    @Override
    public Page<Audit> search(AuditCriteria filter, Pageable pageable) {
        Select.Where select = QueryBuilder.select().all().from(AUDITS_TABLE).allowFiltering().where();

        if (filter.getFrom() > 0) {
            select.and(gte("createdAt", filter.getFrom()));
        }

        if (filter.getTo() > 0) {
            select.and(lte("createdAt", filter.getTo()));
        }

        if (filter.getEvents() != null && !filter.getEvents().isEmpty()) {
            select.and(in("event", filter.getEvents()));
        }

        if (filter.getReferences() != null && !filter.getReferences().isEmpty()) {
            List<String> referenceTypes = filter.getReferences().keySet().stream().map(Audit.AuditReferenceType::name).collect(toList());
            List<String> referenceIds = filter.getReferences().values().stream().reduce((r1, r2) -> {r1.addAll(r2); return r1;}).get();
            select.and(in("referenceId", referenceIds)).and(in("referenceType", referenceTypes));
        }

        final List<Row> rows = session.execute(select).all();
        final int limit = pageable.pageNumber() * pageable.pageSize();
        List<Audit> audits = rows.
                stream().
                map(this::auditFromRow).
                sorted(comparing(Audit::getCreatedAt).reversed()).
                skip(limit - pageable.pageSize()).
                limit(limit).
                collect(toList());

        return new Page<>(audits,
                (pageable != null) ? pageable.pageNumber() : 0,
                (pageable != null) ? pageable.pageSize() : 0,
                rows.size());
    }

    @Override
    public Optional<Audit> findById(String id) throws TechnicalException {
        LOGGER.debug("Find Audit by ID [{}]", id);

        final Statement select = QueryBuilder.select().all().from(AUDITS_TABLE).where(eq("id", id));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(auditFromRow(row));
    }

    @Override
    public Audit create(Audit audit) throws TechnicalException {
        LOGGER.debug("Create Audit [{}]", audit.getUsername());

        Statement insert = QueryBuilder.insertInto(AUDITS_TABLE)
                .values(new String[]{"id", "referenceId", "referenceType", "username", "createdAt", "event", "patch", "properties"},
                        new Object[]{audit.getId(), audit.getReferenceId(), audit.getReferenceType().name(), audit.getUsername(),
                                audit.getCreatedAt(), audit.getEvent(), audit.getPatch(), audit.getProperties()});

        session.execute(insert);

        final Optional<Audit> createdAudit = findById(audit.getId());

        return createdAudit.orElse(null);

    }

    private Audit auditFromRow(Row row) {
        if (row!= null) {
            final Audit audit = new Audit();
            audit.setId(row.getString("id"));
            audit.setReferenceType(Audit.AuditReferenceType.valueOf(row.getString("referenceType")));
            audit.setReferenceId(row.getString("referenceId"));
            audit.setUsername(row.getString("username"));
            audit.setEvent(row.getString("event"));
            audit.setPatch(row.getString("patch"));
            audit.setProperties(row.getMap("properties", String.class, String.class));
            audit.setCreatedAt(row.getTimestamp("createdAt"));
            return audit;
        }
        return null;
    }
}
