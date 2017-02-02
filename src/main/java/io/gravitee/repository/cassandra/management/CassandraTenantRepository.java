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
import io.gravitee.repository.management.api.TenantRepository;
import io.gravitee.repository.management.model.Tenant;
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
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraTenantRepository implements TenantRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraTenantRepository.class);

    private final String TENANTS_TABLE = "tenants";

    @Autowired
    private Session session;

    @Override
    public Set<Tenant> findAll() throws TechnicalException {
        LOGGER.debug("Find all tenants");

        final Statement select = QueryBuilder.select().all().from(TENANTS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::tenantFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<Tenant> findById(String tenantId) throws TechnicalException {
        LOGGER.debug("Find Tenant by ID [{}]", tenantId);

        Statement select = QueryBuilder.select().all().from(TENANTS_TABLE).where(eq("id", tenantId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(tenantFromRow(row));
    }

    @Override
    public Tenant create(Tenant tenant) throws TechnicalException {
        LOGGER.debug("Create Tenant [{}]", tenant.getName());

        Statement insert = QueryBuilder.insertInto(TENANTS_TABLE)
                .values(new String[]{"id", "name", "description"},
                        new Object[]{tenant.getId(), tenant.getName(), tenant.getDescription()});

        session.execute(insert);

        return findById(tenant.getId()).orElse(null);
    }

    @Override
    public Tenant update(Tenant tenant) throws TechnicalException {
        LOGGER.debug("Update Tenant [{}]", tenant.getName());

        Statement update = QueryBuilder.update(TENANTS_TABLE)
                .with(set("name", tenant.getName()))
                .and(set("description", tenant.getDescription()))
                .where(eq("id", tenant.getId()));

        session.execute(update);

        return findById(tenant.getId()).orElse(null);
    }

    @Override
    public void delete(String tenantId) throws TechnicalException {
        LOGGER.debug("Delete tenant [{}]", tenantId);

        Statement delete = QueryBuilder.delete().from(TENANTS_TABLE).where(eq("id", tenantId));

        session.execute(delete);
    }

    private Tenant tenantFromRow(Row row) {
        if (row != null) {
            final Tenant tenant = new Tenant();
            tenant.setId(row.getString("id"));
            tenant.setName(row.getString("name"));
            tenant.setDescription(row.getString("description"));
            return tenant;
        }
        return null;
    }

}
