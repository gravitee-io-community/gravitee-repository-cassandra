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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.SubscriptionRepository;
import io.gravitee.repository.management.model.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 */
@Repository
public class CassandraSubscriptionRepository implements SubscriptionRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraSubscriptionRepository.class);

    private final String SUBSCRIPTIONS_TABLE = "subscriptions";

    @Autowired
    private Session session;

    @Override
    public Set<Subscription> findByPlan(String plan) throws TechnicalException {
        LOGGER.debug("Find Subscription by plan [{}]", plan);

        final Statement select = QueryBuilder.select().all().from(SUBSCRIPTIONS_TABLE).allowFiltering()
                .where(eq("plan", plan));

        final List<Row> all = session.execute(select).all();

        return all.stream().map(this::subscriptionFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Subscription> findByApplication(String application) throws TechnicalException {
        LOGGER.debug("Find Subscription by application [{}]", application);

        final Statement select = QueryBuilder.select().all().from(SUBSCRIPTIONS_TABLE).allowFiltering()
                .where(eq("application", application));

        final List<Row> all = session.execute(select).all();

        return all.stream().map(this::subscriptionFromRow).collect(Collectors.toSet());
    }

    @Override
    public Optional<Subscription> findById(final String subscriptionId) throws TechnicalException {
        LOGGER.debug("Find Subscription by ID [{}]", subscriptionId);

        Statement select = QueryBuilder.select().all().from(SUBSCRIPTIONS_TABLE).where(eq("id", subscriptionId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(subscriptionFromRow(row));
    }

    @Override
    public Subscription create(Subscription subscription) throws TechnicalException {
        LOGGER.debug("Create Subscription for the plan/application [{}/{}]", subscription.getPlan(),
                subscription.getApplication());

        final Subscription.Status subStatus = subscription.getStatus();
        String status = null;
        if (subStatus != null) {
            status = subStatus.toString();
        }

        Statement insert = QueryBuilder.insertInto(SUBSCRIPTIONS_TABLE)
                .values(new String[]{"id", "created_at", "updated_at", "processed_at", "starting_at", "ending_at",
                                "processed_by", "subscribed_by", "plan", "reason", "status", "application", "closed_at"},
                        new Object[]{subscription.getId(), subscription.getCreatedAt(), subscription.getUpdatedAt(),
                                subscription.getProcessedAt(), subscription.getStartingAt(), subscription.getEndingAt(),
                                subscription.getProcessedBy(), subscription.getSubscribedBy(), subscription.getPlan(),
                                subscription.getReason(), status, subscription.getApplication(), subscription.getClosedAt()});

        session.execute(insert);

        return findById(subscription.getId()).orElse(null);
    }

    @Override
    public Subscription update(Subscription subscription) throws TechnicalException {
        if (subscription == null || subscription.getId() == null) {
            throw new IllegalStateException("Subscription to update must have an id");
        }

        if (!findById(subscription.getId()).isPresent()) {
            throw new IllegalStateException(String.format("No subscription found with id [%s]", subscription.getId()));
        }

        LOGGER.debug("Update Subscription for the plan/application [{}/{}]", subscription.getPlan(),
                subscription.getApplication());

        final Subscription.Status subStatus = subscription.getStatus();
        String status = null;
        if (subStatus != null) {
            status = subStatus.toString();
        }

        Statement update = QueryBuilder.update(SUBSCRIPTIONS_TABLE)
                .with(set("created_at", subscription.getCreatedAt()))
                .and(set("updated_at", subscription.getUpdatedAt()))
                .and(set("processed_at", subscription.getProcessedAt()))
                .and(set("starting_at", subscription.getStartingAt()))
                .and(set("ending_at", subscription.getEndingAt()))
                .and(set("processed_by", subscription.getProcessedBy()))
                .and(set("subscribed_by", subscription.getSubscribedBy()))
                .and(set("plan", subscription.getPlan()))
                .and(set("reason", subscription.getReason()))
                .and(set("status", status))
                .and(set("application", subscription.getApplication()))
                .and(set("closed_at", subscription.getClosedAt()))
                .where(eq("id", subscription.getId()));

        session.execute(update);

        return findById(subscription.getId()).orElse(null);
    }

    @Override
    public void delete(final String subscriptionId) throws TechnicalException {
        LOGGER.debug("Delete subscription [{}]", subscriptionId);

        Statement delete = QueryBuilder.delete().from(SUBSCRIPTIONS_TABLE).where(eq("id", subscriptionId));

        session.execute(delete);
    }

    private Subscription subscriptionFromRow(Row row) {
        if (row != null) {
            final Subscription subscription = new Subscription();
            subscription.setId(row.getString("id"));
            subscription.setCreatedAt(row.getTimestamp("created_at"));
            subscription.setUpdatedAt(row.getTimestamp("updated_at"));
            subscription.setStartingAt(row.getTimestamp("starting_at"));
            subscription.setEndingAt(row.getTimestamp("ending_at"));
            subscription.setProcessedAt(row.getTimestamp("processed_at"));
            subscription.setProcessedBy(row.getString("processed_by"));
            subscription.setSubscribedBy(row.getString("subscribed_by"));
            subscription.setApplication(row.getString("application"));
            subscription.setPlan(row.getString("plan"));
            subscription.setReason(row.getString("reason"));
            subscription.setClosedAt(row.getTimestamp("closed_at"));
            final String status = row.getString("status");
            if (status != null) {
                subscription.setStatus(Subscription.Status.valueOf(status));
            }
            return subscription;
        }
        return null;
    }
}
