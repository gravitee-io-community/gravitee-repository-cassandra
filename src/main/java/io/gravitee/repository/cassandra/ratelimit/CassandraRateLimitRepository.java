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

package io.gravitee.repository.cassandra.ratelimit;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.gravitee.repository.ratelimit.api.RateLimitRepository;
import io.gravitee.repository.ratelimit.model.RateLimit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.Iterator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraRateLimitRepository implements RateLimitRepository {

    @Autowired
    private Session session;

    private final String RATELIMITS_TABLE = "ratelimits";

    @Override
    public RateLimit get(String rateId) {
        final Statement select = QueryBuilder.select().all().from(RATELIMITS_TABLE).where(eq("id", rateId));

        final Row row = session.execute(select).one();

        return rateLimitFromRow(row);
    }

    @Override
    public void save(RateLimit rateLimit) {
        Statement insert = QueryBuilder.insertInto(RATELIMITS_TABLE)
                .values(new String[]{"key", "last_request", "counter", "reset_time", "created_at", "updated_at", "async"},
                        new Object[]{rateLimit.getKey(), rateLimit.getLastRequest(), rateLimit.getCounter(),
                                rateLimit.getResetTime(), new Timestamp(rateLimit.getCreatedAt()), new Timestamp(rateLimit.getUpdatedAt()),
                                rateLimit.isAsync()});

        session.execute(insert);
    }

    @Override
    public Iterator<RateLimit> findAsyncAfter(long timestamp) {
        final Statement select = QueryBuilder.select().all().from(RATELIMITS_TABLE).allowFiltering()
                .where(eq("async", true))
                .and(gte("updated_at", timestamp));

        final Iterator<Row> rows = session.execute(select).iterator();

        return new Iterator<RateLimit>() {

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public RateLimit next() {
                return rateLimitFromRow(rows.next());
            }
        };
    }

    private RateLimit rateLimitFromRow(Row row) {
        if (row != null) {
            final RateLimit rateLimit = new RateLimit(row.getString("key"));
            rateLimit.setLastRequest(row.getLong("last_request"));
            rateLimit.setCounter(row.getLong("counter"));
            rateLimit.setResetTime(row.getLong("reset_time"));
            rateLimit.setCreatedAt(row.getTimestamp("created_at").getTime());
            rateLimit.setUpdatedAt(row.getTimestamp("updated_at").getTime());
            rateLimit.setAsync(row.getBool("async"));
            return rateLimit;
        }
        return null;
    }
}
