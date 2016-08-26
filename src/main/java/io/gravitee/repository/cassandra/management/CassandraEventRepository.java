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

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.EventRepository;
import io.gravitee.repository.management.api.search.EventCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.model.Event;
import io.gravitee.repository.management.model.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
@Repository
public class CassandraEventRepository implements EventRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraEventRepository.class);

    private final String EVENTS_TABLE = "events";

    @Autowired
    private Session session;


    @Override
    public Page<Event> search(EventCriteria filter, Pageable pageable) {
        LOGGER.debug("Search Events with Paging");
        List<Event> allEvents = searchEvent(filter);
        List<Event> events = new ArrayList<>();

        if (pageable != null) {
            int start = pageable.pageNumber() * pageable.pageSize();
            int end = start + pageable.pageSize();
            for (int i = start; i < end && i < allEvents.size(); ++i) {
                events.add(allEvents.get(i));
            }
        } else {
            events = allEvents;
        }

        return new Page<>(events,
                (pageable != null) ? pageable.pageNumber() : 0,
                (pageable != null) ? pageable.pageSize() : 0,
                allEvents.size());
    }

    @Override
    public List<Event> search(EventCriteria filter) {
        LOGGER.debug("Search Events with Paging");
        return searchEvent(filter);
    }

    @Override
    public Optional<Event> findById(String eventId) throws TechnicalException {
        LOGGER.debug("Find Event by ID [{}]", eventId);

        final Statement select = QueryBuilder.select().all().from(EVENTS_TABLE).allowFiltering().where(eq("id", eventId));
        final ResultSet resultSet = session.execute(select);

        final Row row = resultSet.one();
        Event event = null;
        if (row != null) {
            event = eventFromRow(row);
        }

        return Optional.ofNullable(event);
    }

    @Override
    public Event create(Event event) throws TechnicalException {
        LOGGER.debug("Create Event [{}]", event.getId());

        Statement insert = QueryBuilder.insertInto(EVENTS_TABLE)
                .values(new String[]{"id", "type", "payload", "parent_id", "properties_api_id", "properties_origin",
                                "properties_username", "created_at", "updated_at"},
                        new Object[]{event.getId(),
                                event.getType().toString(),
                                event.getPayload(),
                                event.getParentId(),
                                event.getProperties() == null ? "" : event.getProperties().get("api_id"),
                                event.getProperties() == null ? "" : event.getProperties().get("origin"),
                                event.getProperties() == null ? "" : event.getProperties().get("username"),
                                event.getCreatedAt(),
                                event.getUpdatedAt()});

        session.execute(insert);

        final Optional<Event> createdEvent = findById(event.getId());

        if (createdEvent.isPresent()) {
            LOGGER.debug("Event created");
            return createdEvent.get();
        }

        return null;
    }

    @Override
    public Event update(Event event) throws TechnicalException {
        LOGGER.debug("Update Event [{}]", event.getId());

        Statement update = QueryBuilder.update(EVENTS_TABLE)
                .with(set("type", event.getType().toString()))
                .and(set("payload", event.getPayload()))
                .and(set("parent_id", event.getParentId()))
                .and(set("properties_api_id", event.getProperties().get("api_id")))
                .and(set("properties_origin", event.getProperties().get("origin")))
                .and(set("properties_username", event.getProperties().get("username")))
                .and(set("updated_at", event.getUpdatedAt()))
                .where(eq("id", event.getId()));

        session.execute(update);

        final Optional<Event> updatedEvent = findById(event.getId());

        if (updatedEvent.isPresent()) {
            LOGGER.debug("Event updated");
            return updatedEvent.get();
        }

        return null;
    }

    @Override
    public void delete(String eventId) throws TechnicalException {
        LOGGER.debug("Delete Event [{}]", eventId);

        Statement delete = QueryBuilder.delete().from(EVENTS_TABLE).where(eq("id", eventId));

        session.execute(delete);
    }

    private List<Event> searchEvent(EventCriteria filter) {
        LOGGER.debug("Entering search()");

        // Get event ids by type of event
        List<String> types = filter.getTypes().stream()
                .map(Enum::toString)
                .collect(Collectors.toList());
        List<String> idsType = null;
        if (types != null && !types.isEmpty()) {
            idsType = new ArrayList<>();
            for (String type: types) {
                Statement st = QueryBuilder.select("id").from(EVENTS_TABLE)
                        .allowFiltering()
                        .where(eq("type", type));
                final List<String> ids = session.execute(st).all().stream()
                        .map(row -> row.getString("id"))
                        .collect(Collectors.toList());
                idsType.addAll(ids);
            }
        }

        // Get event ids by event properties
        List<String> idsProperties = null;
        Map<String, Object> properties = filter.getProperties();
        if (properties != null && !properties.isEmpty()) {
            idsProperties = new ArrayList<>();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Collection) {
                    for (String property: (Collection<String>) value) {
                        final List<String> ids = session.execute(QueryBuilder.select("id").from(EVENTS_TABLE)
                                .allowFiltering()
                                .where(eq("properties_" + key, property)))
                                .all().stream().map(row -> row.getString("id")).collect(Collectors.toList());
                        idsProperties.addAll(ids);
                    }
                } else {
                    final List<String> ids = session.execute(QueryBuilder.select("id").from(EVENTS_TABLE)
                            .allowFiltering()
                            .where(eq("properties_" + key, value)))
                            .all().stream().map(row -> row.getString("id")).collect(Collectors.toList());
                    idsProperties.addAll(ids);
                }
            }
        }

        // Get event ids by update range
        List<String> idsDate = null;
        if (filter.getFrom() != 0L && filter.getTo() != 0L) {
            idsDate = session.execute(QueryBuilder.select("id").from(EVENTS_TABLE)
                    .allowFiltering()
                    .where(gte("updated_at", new Date(filter.getFrom())))
                    .and(lt("updated_at", new Date(filter.getTo()))))
                    .all().stream().map(row -> row.getString("id")).collect(Collectors.toList());
        }


        // Keep ids common to all filters, discard the others
        // TODO : Refactor this beast !
        List<String> ids;
        if (idsType != null && idsProperties != null && idsDate != null) { idsType.retainAll(idsProperties);idsType.retainAll(idsDate);ids = idsType; }
        else if (idsProperties != null && idsDate != null) { idsProperties.retainAll(idsDate); ids = idsProperties; }
        else if (idsType != null && idsProperties != null) { idsType.retainAll(idsProperties); ids = idsType; }
        else if (idsType != null && idsDate != null) { idsType.retainAll(idsDate);ids = idsType; }
        else if (idsType != null) { ids = idsType; }
        else if (idsProperties != null) { ids = idsProperties; }
        else if (idsDate != null) { ids = idsDate; }
        else { ids = session.execute(QueryBuilder.select("id").from(EVENTS_TABLE)).all().stream().map(row -> row.getString("id")).collect(Collectors.toList()); }

        // Retrieve events corresponding to id list built above
        List<Event> allEvents = new ArrayList<>();
        ResultSet resultSet = session.execute(QueryBuilder.select().all().from(EVENTS_TABLE).where(in("id", ids)));
        resultSet.forEach(row -> allEvents.add(eventFromRow(row)));
        allEvents.sort((e1, e2) -> e2.getUpdatedAt().compareTo(e1.getUpdatedAt()));

        return allEvents;
    }

    private Event eventFromRow(Row row) {
        if (row != null) {
            final Event event = new Event();
            event.setId(row.getString("id"));
            event.setType(EventType.valueOf(row.getString("type").toUpperCase()));
            event.setPayload(row.getString("payload"));
            event.setParentId(row.getString("parent_id"));
            event.setProperties(new HashMap<>(Stream.of(
                    new SimpleEntry<>("api_id", Optional.ofNullable(row.getString("properties_api_id")).orElse("")),
                    new SimpleEntry<>("origin", Optional.ofNullable(row.getString("properties_origin")).orElse("")),
                    new SimpleEntry<>("username", Optional.ofNullable(row.getString("properties_username")).orElse("")))
                    .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))));
            event.setCreatedAt(row.getTimestamp("created_at"));
            event.setUpdatedAt(row.getTimestamp("updated_at"));
            return event;
        }
        return null;
    }
}