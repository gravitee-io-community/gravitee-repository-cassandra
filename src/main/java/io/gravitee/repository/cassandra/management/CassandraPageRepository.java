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
import io.gravitee.repository.management.api.PageRepository;
import io.gravitee.repository.management.model.Page;
import io.gravitee.repository.management.model.PageConfiguration;
import io.gravitee.repository.management.model.PageSource;
import io.gravitee.repository.management.model.PageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 * @author Nicolas GERAUD (nicolas.geraud at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraPageRepository implements PageRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraPageRepository.class);

    private final String PAGES_TABLE = "pages";

    @Autowired
    private Session session;

    @Override
    public Collection<Page> findApiPageByApiId(String apiId) throws TechnicalException {
        LOGGER.debug("Find Pages by Api ID [{}]", apiId);

        final Statement select = QueryBuilder.select().all().from(PAGES_TABLE).allowFiltering().where(eq("api", apiId));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::pageFromRow).collect(Collectors.toSet());
    }

    @Override
    public Integer findMaxApiPageOrderByApiId(String apiId) throws TechnicalException {
        LOGGER.debug("Find max Page order by Api ID [{}]", apiId);

        final Statement select = QueryBuilder.select().all().from(PAGES_TABLE).allowFiltering()
                .where(eq("api", apiId));

        final List<Row> all = session.execute(select).all();

        return all.stream()
                .map(this::pageFromRow)
                .map(Page::getOrder)
                .reduce(Math::max).orElse(null);
    }

    @Override
    public Optional<Page> findById(String pageId) throws TechnicalException {
        LOGGER.debug("Find Page by ID [{}]", pageId);

        final Statement select = QueryBuilder.select().all().from(PAGES_TABLE).where(eq("id", pageId));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(pageFromRow(row));
    }

    @Override
    public Page create(Page page) throws TechnicalException {
        LOGGER.debug("Create Page {}", page.getName());

        String sourceType = null;
        String configuration = null;
        String tryItURL = null;
        Boolean tryIt = null;
        String pageType = null;

        final PageSource source = page.getSource();
        if (source != null) {
            sourceType = source.getType();
            configuration = source.getConfiguration();
        }

        final PageType type = page.getType();
        if (type != null) {
            pageType = type.toString();
        }

        final PageConfiguration pageConfiguration = page.getConfiguration();
        if (pageConfiguration != null) {
            tryItURL = pageConfiguration.getTryItURL();
            tryIt = pageConfiguration.isTryIt();
        }

        Statement insert = QueryBuilder.insertInto(PAGES_TABLE)
                .values(new String[]{"id", "type", "name", "content", "last_contributor", "page_order", "published",
                                "source_type", "source_configuration", "configuration_tryiturl", "configuration_tryit", "api",
                                "created_at", "updated_at", "homepage"},
                        new Object[]{page.getId(), pageType, page.getName(), page.getContent(), page.getLastContributor(),
                                page.getOrder(), page.isPublished(), sourceType, configuration,
                                tryItURL, tryIt, page.getApi(),
                                page.getCreatedAt(), page.getUpdatedAt(), page.isHomepage()});

        session.execute(insert);

        return findById(page.getId()).orElse(null);
    }

    @Override
    public Page update(Page page) throws TechnicalException {
        if(page == null){
            throw new IllegalArgumentException("Page must not be null");
        }

        if(!findById(page.getId()).isPresent()){
            throw new IllegalArgumentException(String.format("No page found with id [%s]", page.getId()));
        }

        LOGGER.debug("Update Page {}", page.getName());

        String sourceType = null;
        String configuration = null;
        String tryItURL = null;
        Boolean tryIt = null;
        String pageType = null;

        final PageSource source = page.getSource();
        if (source != null) {
            sourceType = source.getType();
            configuration = source.getConfiguration();
        }

        final PageType type = page.getType();
        if (type != null) {
            pageType = type.toString();
        }

        final PageConfiguration pageConfiguration = page.getConfiguration();
        if (pageConfiguration != null) {
            tryItURL = pageConfiguration.getTryItURL();
            tryIt = pageConfiguration.isTryIt();
        }

        final Statement update = QueryBuilder.update(PAGES_TABLE)
                .with(set("name", page.getName()))
                .and(set("type", pageType))
                .and(set("content", page.getContent()))
                .and(set("last_contributor", page.getLastContributor()))
                .and(set("page_order", page.getOrder()))
                .and(set("published", page.isPublished()))
                .and(set("source_type", sourceType))
                .and(set("source_configuration", configuration))
                .and(set("configuration_tryiturl", tryItURL))
                .and(set("configuration_tryit", tryIt))
                .and(set("api", page.getApi()))
                .and(set("updated_at", page.getUpdatedAt()))
                .and(set("homepage", page.isHomepage()))
                .where(eq("id", page.getId()));

        session.execute(update);

        return findById(page.getId()).orElse(null);
    }

    @Override
    public void delete(String pageId) throws TechnicalException {
        LOGGER.debug("Delete Page [{}]", pageId);

        Statement delete = QueryBuilder.delete().from(PAGES_TABLE).where(eq("id", pageId));

        session.execute(delete);
    }

    @Override
    public Collection<Page> findApiPageByApiIdAndHomepage(String apiId, boolean isHomepage) throws TechnicalException {
        LOGGER.debug("Find Page by ApiId and homepage [{}, {}]", apiId, isHomepage);

        final Statement select = QueryBuilder.
                select().
                all().
                from(PAGES_TABLE).
                allowFiltering().
                where(eq("api", apiId)).
                and(eq("homepage", isHomepage));

        List<Row> rowList = session.execute(select).all();

        return rowList.stream().map(this::pageFromRow).collect(Collectors.toSet());
    }

    private Page pageFromRow(Row row) {
        if (row!= null) {
            final Page page = new Page();
            page.setId(row.getString("id"));
            page.setName(row.getString("name"));
            final String type = row.getString("type");
            if (type != null) {
                page.setType(PageType.valueOf(type.toUpperCase()));
            }
            page.setContent(row.getString("content"));
            page.setLastContributor(row.getString("last_contributor"));
            page.setOrder(row.getInt("page_order"));
            page.setPublished(row.getBool("published"));
            page.setHomepage(row.getBool("homepage"));

            final String sourceType = row.getString("source_type");
            final String sourceConfiguration = row.getString("source_configuration");
            if (sourceConfiguration != null || sourceType != null) {
                final PageSource pageSource = new PageSource();
                pageSource.setType(sourceType);
                pageSource.setConfiguration(sourceConfiguration);
                page.setSource(pageSource);
            }

            final PageConfiguration pageConfiguration = new PageConfiguration();
            pageConfiguration.setTryItURL(row.getString("configuration_tryiturl"));
            pageConfiguration.setTryIt(row.getBool("configuration_tryit"));
            page.setConfiguration(pageConfiguration);

            page.setApi(row.getString("api"));
            page.setCreatedAt(row.getTimestamp("created_at"));
            page.setUpdatedAt(row.getTimestamp("updated_at"));
            return page;
        }
        return null;
    }
}
