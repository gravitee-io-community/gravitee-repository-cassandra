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
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.ApiKeyRepository;
import io.gravitee.repository.management.api.search.ApiKeyCriteria;
import io.gravitee.repository.management.model.ApiKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraApiKeyRepository implements ApiKeyRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraApiKeyRepository.class);

    private final String APIKEYS_TABLE = "apikeys";

    @Autowired
    private Session session;

    @Override
    public Optional<ApiKey> findById(String key) throws TechnicalException {
        LOGGER.debug("Find ApiKey by key [{}]", key);

        Statement select = QueryBuilder.select().all().from(APIKEYS_TABLE).where(eq("key", key));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(apiKeyFromRow(row));
    }

    @Override
    public ApiKey create(ApiKey apiKey) throws TechnicalException {
        LOGGER.debug("Create ApiKey [{}]", apiKey.getKey());

        Statement insert = QueryBuilder.insertInto(APIKEYS_TABLE)
                .values(new String[]{"key", "subscription", "application", "plan", "expire_at",
                                "created_at", "updated_at", "revoked_at", "revoked"},
                        new Object[]{apiKey.getKey(), apiKey.getSubscription(), apiKey.getApplication(),
                                apiKey.getPlan(), apiKey.getExpireAt(), apiKey.getCreatedAt(),
                                apiKey.getUpdatedAt(), apiKey.getRevokedAt(), apiKey.isRevoked()});

        session.execute(insert);

        return findById(apiKey.getKey()).orElse(null);
    }

    @Override
    public ApiKey update(ApiKey apiKey) throws TechnicalException {
        if (apiKey == null || apiKey.getKey() == null) {
            throw new IllegalStateException("ApiKey to update must have an key");
        }

        if (!findById(apiKey.getKey()).isPresent()) {
            throw new IllegalStateException(String.format("No apiKey found with key [%s]", apiKey.getKey()));
        }

        LOGGER.debug("Update ApiKey [{}]", apiKey.getKey());

        Statement update = QueryBuilder.update(APIKEYS_TABLE)
                .with(set("subscription", apiKey.getSubscription()))
                .and(set("application", apiKey.getApplication()))
                .and(set("plan", apiKey.getPlan()))
                .and(set("expire_at", apiKey.getExpireAt()))
                .and(set("created_at", apiKey.getCreatedAt()))
                .and(set("updated_at", apiKey.getUpdatedAt()))
                .and(set("revoked_at", apiKey.getRevokedAt()))
                .and(set("revoked", apiKey.isRevoked()))
                .where(eq("key", apiKey.getKey()));

        session.execute(update);

        return findById(apiKey.getKey()).orElse(null);
    }

    @Override
    public Set<ApiKey> findBySubscription(String subscription) throws TechnicalException {
        LOGGER.debug("Find ApiKey by Subscription [{}]", subscription);

        Statement select = QueryBuilder.select().all().from(APIKEYS_TABLE).allowFiltering().where(eq("subscription", subscription));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiKeyFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<ApiKey> findByPlan(String plan) throws TechnicalException {
        LOGGER.debug("Find ApiKey by Plan [{}]", plan);

        final Statement select = QueryBuilder.select().all().from(APIKEYS_TABLE).allowFiltering().where(eq("plan", plan));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::apiKeyFromRow).collect(Collectors.toSet());
    }

    @Override
    public List<ApiKey> findByCriteria(ApiKeyCriteria filter) throws TechnicalException {
        final Select.Where query = QueryBuilder.select().all().from(APIKEYS_TABLE).allowFiltering().where();

        if (! filter.isIncludeRevoked()) {
            query.and(eq("revoked", false));
        }

        final Collection<String> plans = filter.getPlans();
        if (plans != null && !plans.isEmpty()) {
            query.and(in("plan", new ArrayList<>(plans)));
        }

        // set range query
        if (filter.getFrom() != 0) {
            query.and(gte("updated_at", new Date(filter.getFrom())));
        }
        if (filter.getTo() != 0) {
            query.and(lt("updated_at", new Date(filter.getTo())));
        }

        final ResultSet resultSet = session.execute(query);

        return resultSet.all().stream().map(this::apiKeyFromRow).collect(Collectors.toList());
    }

    private ApiKey apiKeyFromRow(Row row) {
        if (row != null) {
            final ApiKey apiKey = new ApiKey();
            apiKey.setKey(row.getString("key"));
            apiKey.setSubscription(row.getString("subscription"));
            apiKey.setApplication(row.getString("application"));
            apiKey.setPlan(row.getString("plan"));
            apiKey.setExpireAt(row.getTimestamp("expire_at"));
            apiKey.setCreatedAt(row.getTimestamp("created_at"));
            apiKey.setUpdatedAt(row.getTimestamp("updated_at"));
            apiKey.setRevokedAt(row.getTimestamp("revoked_at"));
            apiKey.setRevoked(row.getBool("revoked"));
            return apiKey;
        }
        return null;
    }


}
