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
import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.RatingRepository;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraRatingRepository implements RatingRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRatingRepository.class);

    private static final String RATINGS_TABLE = "ratings";

    @Autowired
    private Session session;

    @Override
    public Page<Rating> findByApiPageable(String api, Pageable pageable) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}]", api);
        final Statement statement = QueryBuilder.select().all().from(RATINGS_TABLE).allowFiltering().
                where(eq("api", api));
        final List<Row> rows = session.execute(statement).all();

        final int limit = pageable.pageNumber() * pageable.pageSize();
        final List<Rating> ratings = rows.
                stream().
                map(this::convert).
                sorted(comparing(Rating::getCreatedAt).reversed()).
                skip(limit - pageable.pageSize()).
                limit(limit).
                collect(toList());


        return new Page<>(ratings, pageable.pageNumber(), ratings.size(), rows.size());
    }

    @Override
    public List<Rating> findByApi(String api) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}]", api);
        Statement select = QueryBuilder.select().all().from(RATINGS_TABLE).allowFiltering().where(eq("api", api));
        final List<Row> rows = session.execute(select).all();
        return rows.stream().map(this::convert).collect(toList());
    }

    @Override
    public Optional<Rating> findByApiAndUser(String api, String user) throws TechnicalException {
        LOGGER.debug("Find Rating by api [{}] and user [{}]", api, user);
        Statement select = QueryBuilder.select().all().from(RATINGS_TABLE).allowFiltering().where(eq("api", api)).and(eq("user", user));
        final Row row = session.execute(select).one();
        return Optional.ofNullable(convert(row));
    }

    @Override
    public Optional<Rating> findById(String ratingId) throws TechnicalException {
        LOGGER.debug("Find Rating by ID [{}]", ratingId);
        Statement select = QueryBuilder.select().all().from(RATINGS_TABLE).where(eq("id", ratingId));
        final Row row = session.execute(select).one();
        return Optional.ofNullable(convert(row));
    }

    @Override
    public Rating create(Rating rating) throws TechnicalException {
        LOGGER.debug("Create Rating [{}]", rating.getId());
        Statement insert = QueryBuilder.insertInto(RATINGS_TABLE)
                .values(new String[]{"id", "api", "user", "rate", "title", "comment", "created_at", "updated_at"},
                        new Object[]{rating.getId(), rating.getApi(), rating.getUser(), rating.getRate(), rating.getTitle(),
                                rating.getComment(), rating.getCreatedAt(), rating.getUpdatedAt()});
        session.execute(insert);
        return findById(rating.getId()).orElse(null);
    }

    @Override
    public Rating update(Rating rating) throws TechnicalException {
        if (rating == null || rating.getId() == null) {
            throw new IllegalStateException("Rating to update must have a name");
        }
        if (!findById(rating.getId()).isPresent()) {
            throw new IllegalStateException(String.format("No rating found with name [%s]", rating.getId()));
        }
        LOGGER.debug("Update Rating [{}]", rating.getId());
        Statement update = QueryBuilder.update(RATINGS_TABLE)
                .with(set("api", rating.getApi()))
                .and(set("user", rating.getUser()))
                .and(set("rate", rating.getRate()))
                .and(set("title", rating.getTitle()))
                .and(set("comment", rating.getComment()))
                .and(set("created_at", rating.getCreatedAt()))
                .and(set("updated_at", rating.getUpdatedAt()))
                .where(eq("id", rating.getId()));
        session.execute(update);
        return findById(rating.getId()).orElse(null);
    }

    @Override
    public void delete(String ratingId) throws TechnicalException {
        LOGGER.debug("Delete Rating [{}]", ratingId);
        Statement delete = QueryBuilder.delete().from(RATINGS_TABLE).where(eq("id", ratingId));
        session.execute(delete);
    }

    private Rating convert(Row row) {
        if (row != null) {
            final Rating rating = new Rating();
            rating.setId(row.getString("id"));
            rating.setApi(row.getString("api"));
            rating.setUser(row.getString("user"));
            rating.setRate(row.getByte("rate"));
            rating.setTitle(row.getString("title"));
            rating.setComment(row.getString("comment"));
            rating.setCreatedAt(row.getTimestamp("created_at"));
            rating.setUpdatedAt(row.getTimestamp("updated_at"));
            return rating;
        }
        return null;
    }
}
