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
import io.gravitee.repository.management.api.PlanRepository;
import io.gravitee.repository.management.model.Api;
import io.gravitee.repository.management.model.LifecycleState;
import io.gravitee.repository.management.model.Plan;
import io.gravitee.repository.management.model.Visibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraPlanRepository implements PlanRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraPlanRepository.class);

    private final String PLANS_TABLE = "plans";

    @Autowired
    private Session session;

    @Override
    public Optional<Plan> findById(String planId) throws TechnicalException {
        LOGGER.debug("Find Plan by ID [{}]", planId);

        final Statement select = QueryBuilder.select().all().from(PLANS_TABLE).where(eq("id", planId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(planFromRow(row));
    }

    @Override
    public Plan create(Plan plan) throws TechnicalException {
        LOGGER.debug("Create Plan {}", plan.getName());

        final Statement insert = QueryBuilder.insertInto(PLANS_TABLE)
                .values(new String[]{"id", "name", "description", "validation", "type", "plan_order", "apis",
                                "created_at", "updated_at", "definition", "characteristics"},
                        new Object[]{plan.getId(), plan.getName(), plan.getDescription(),
                                plan.getValidation() == null ? Plan.PlanValidationType.MANUAL.toString() : plan.getValidation().toString(),
                                plan.getType() == null ? Plan.PlanType.API.toString() : plan.getType().toString(),
                                plan.getOrder(), plan.getApis(), plan.getCreatedAt(), plan.getUpdatedAt(), plan.getDefinition(),
                                plan.getCharacteristics()});

        session.execute(insert);

        return findById(plan.getId()).orElse(null);
    }

    @Override
    public Plan update(Plan plan) throws TechnicalException {
        LOGGER.debug("Update Plan {}", plan.getName());

        Statement update = QueryBuilder.update(PLANS_TABLE)
                .with(set("name", plan.getName()))
                .and(set("description", plan.getDescription()))
                .and(set("validation", plan.getValidation().toString()))
                .and(set("type", plan.getType().toString()))
                .and(set("plan_order", plan.getOrder()))
                .and(set("apis", plan.getApis()))
                .and(set("updated_at", plan.getUpdatedAt()))
                .and(set("definition", plan.getDefinition()))
                .and(set("characteristics", plan.getCharacteristics()))
                .where(eq("id", plan.getId()));

        session.execute(update);

        return findById(plan.getId()).orElse(null);
    }

    @Override
    public void delete(String planId) throws TechnicalException {
        LOGGER.debug("Delete Plan [{}]", planId);

        Statement delete = QueryBuilder.delete().from(PLANS_TABLE).where(eq("id", planId));

        session.execute(delete);
    }

    @Override
    public Set<Plan> findByApi(String apiId) throws TechnicalException {
        LOGGER.debug("Find Plans by Api ID [{}]", apiId);

        final Statement select = QueryBuilder.select().all().from(PLANS_TABLE);

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream()
                .filter(row -> row.getSet("apis", String.class).contains(apiId))
                .map(this::planFromRow)
                .collect(Collectors.toSet());
    }

    private Plan planFromRow(Row row) {
        if (row != null) {
            final Plan plan = new Plan();
            plan.setId(row.getString("id"));
            plan.setName(row.getString("name"));
            plan.setDescription(row.getString("description"));
            plan.setValidation(Plan.PlanValidationType.valueOf(row.getString("validation").toUpperCase()));
            plan.setType(Plan.PlanType.valueOf(row.getString("type").toUpperCase()));
            plan.setOrder(row.getInt("plan_order"));
            plan.setApis(row.getSet("apis", String.class));
            plan.setCreatedAt(row.getTimestamp("created_at"));
            plan.setUpdatedAt(row.getTimestamp("updated_at"));
            plan.setDefinition(row.getString("definition"));
            plan.setCharacteristics(row.getList("characteristics", String.class));
            return plan;
        }
        return null;
    }

}
