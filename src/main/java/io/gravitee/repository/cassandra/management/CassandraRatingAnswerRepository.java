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
import io.gravitee.repository.management.api.RatingAnswerRepository;
import io.gravitee.repository.management.model.RatingAnswer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static java.util.stream.Collectors.toList;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraRatingAnswerRepository implements RatingAnswerRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRatingAnswerRepository.class);

    private static final String RATING_ANSWERS_TABLE = "ratinganswers";

    @Autowired
    private Session session;

    @Override
    public RatingAnswer create(RatingAnswer ratingAnswer) throws TechnicalException {
        LOGGER.debug("Create Rating answer [{}]", ratingAnswer.getId());
        Statement insert = QueryBuilder.insertInto(RATING_ANSWERS_TABLE)
                .values(new String[]{"id", "rating", "user", "comment", "created_at", "updated_at"},
                        new Object[]{ratingAnswer.getId(), ratingAnswer.getRating(), ratingAnswer.getUser(),
                                ratingAnswer.getComment(), ratingAnswer.getCreatedAt(), ratingAnswer.getUpdatedAt()});
        session.execute(insert);
        return findById(ratingAnswer.getId()).orElse(null);
    }

    @Override
    public List<RatingAnswer> findByRating(String rating) throws TechnicalException {
        LOGGER.debug("Find Rating by rating [{}]", rating);
        Statement select = QueryBuilder.select().all().from(RATING_ANSWERS_TABLE).allowFiltering().where(eq("rating", rating));
        final List<Row> rows = session.execute(select).all();
        return rows.stream().map(this::convert).collect(toList());
    }

    @Override
    public Optional<RatingAnswer> findById(String id) throws TechnicalException {
        LOGGER.debug("Find Rating by ID [{}]", id);
        Statement select = QueryBuilder.select().all().from(RATING_ANSWERS_TABLE).where(eq("id", id));
        final Row row = session.execute(select).one();
        return Optional.ofNullable(convert(row));
    }

    @Override
    public RatingAnswer update(RatingAnswer ratingAnswer) throws TechnicalException {
        if (ratingAnswer == null || ratingAnswer.getId() == null) {
            throw new IllegalStateException("Rating answer to update must have a name");
        }
        if (!findById(ratingAnswer.getId()).isPresent()) {
            throw new IllegalStateException(String.format("No rating answer found with name [%s]", ratingAnswer.getId()));
        }
        LOGGER.debug("Update Rating [{}]", ratingAnswer.getId());
        Statement update = QueryBuilder.update(RATING_ANSWERS_TABLE)
                .with(set("rating", ratingAnswer.getRating()))
                .and(set("user", ratingAnswer.getUser()))
                .and(set("comment", ratingAnswer.getComment()))
                .and(set("created_at", ratingAnswer.getCreatedAt()))
                .and(set("updated_at", ratingAnswer.getUpdatedAt()))
                .where(eq("id", ratingAnswer.getId()));
        session.execute(update);
        return findById(ratingAnswer.getId()).orElse(null);
    }

    @Override
    public void delete(String id) throws TechnicalException {
        LOGGER.debug("Delete Rating answer [{}]", id);
        Statement delete = QueryBuilder.delete().from(RATING_ANSWERS_TABLE).where(eq("id", id));
        session.execute(delete);
    }

    private RatingAnswer convert(Row row) {
        if (row != null) {
            final RatingAnswer ratingAnswer = new RatingAnswer();
            ratingAnswer.setId(row.getString("id"));
            ratingAnswer.setRating(row.getString("rating"));
            ratingAnswer.setUser(row.getString("user"));
            ratingAnswer.setComment(row.getString("comment"));
            ratingAnswer.setCreatedAt(row.getTimestamp("created_at"));
            ratingAnswer.setUpdatedAt(row.getTimestamp("updated_at"));
            return ratingAnswer;
        }
        return null;
    }
}
