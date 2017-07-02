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
import io.gravitee.repository.management.api.MembershipRepository;
import io.gravitee.repository.management.api.RoleRepository;
import io.gravitee.repository.management.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * @author Nicolas GERAUD (nicolas.geraud at graviteesource.com)
 * @author GraviteeSource Team
 */
@Repository
public class CassandraRoleRepository implements RoleRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraRoleRepository.class);

    private final String ROLES_TABLE = "roles";

    @Autowired
    private Session session;

    @Override
    public Optional<Role> findById(RoleScope scope, String name) throws TechnicalException {
        LOGGER.debug("Find Role by ID [{}, {}]", scope, name);

        final Statement select = QueryBuilder.select().
                all().
                from(ROLES_TABLE).
                allowFiltering().
                where(eq("scope", scope.getId())).
                and(eq("name", name));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(roleFromRow(row));
    }

    @Override
    public Role create(Role role) throws TechnicalException {
        LOGGER.debug("Create Role [{},{}]", role.getScope(), role.getName());

        List<Integer> perms = new ArrayList<>(role.getPermissions().length);
        for (int perm : role.getPermissions()) {
            perms.add(perm);
        }

        Statement insert = QueryBuilder.insertInto(ROLES_TABLE)
                .values(new String[]{"scope", "name", "description",
                                "default", "system", "permissions",
                                "created_at", "updated_at"},
                        new Object[]{role.getScope().getId(), role.getName(), role.getDescription(),
                                role.isDefaultRole(), role.isSystem(), perms,
                               role.getCreatedAt(), role.getUpdatedAt()});

        session.execute(insert);

        return findById(role.getScope(), role.getName()).orElse(null);
    }

    @Override
    public Role update(Role role) throws TechnicalException {
        LOGGER.debug("Update Role [{}, {}]", role.getScope(), role.getName());

        List<Integer> perms = new ArrayList<>(role.getPermissions().length);
        for (int perm : role.getPermissions()) {
            perms.add(perm);
        }

        Statement update = QueryBuilder.
                update(ROLES_TABLE).
                with(set("description", role.getDescription())).
                and(set("permissions", perms)).
                and(set("default", role.isDefaultRole())).
                and(set("system", role.isSystem())).
                and(set("created_at", role.getCreatedAt())).
                and(set("updated_at", role.getUpdatedAt())).
                where(eq("scope", role.getScope().getId())).
                and(eq("name", role.getName()));

        session.execute(update);

        return findById(role.getScope(), role.getName()).orElse(null);
    }

    @Override
    public void delete(RoleScope scope, String name) throws TechnicalException {
        LOGGER.debug("Delete Role [{}, {}]", scope, name);

        Statement delete = QueryBuilder.
                delete().
                from(ROLES_TABLE).
                where(eq("scope", scope.getId())).
                and(eq("name", name));

        session.execute(delete);
    }

    @Override
    public Set<Role> findByScope(RoleScope scope) throws TechnicalException {
        LOGGER.debug("Find Role by Scope [{}]", scope);

        final Statement select = QueryBuilder.
                select().
                all().
                from(ROLES_TABLE).
                allowFiltering().
                where(eq("scope", scope.getId()));

        return session.execute(select).
                all().
                stream().
                map(this::roleFromRow).
                collect(Collectors.toSet());
    }

    @Override
    public Set<Role> findAll() throws TechnicalException {
        LOGGER.debug("Find all Roles");

        final Statement select = QueryBuilder.
                select().
                all().
                from(ROLES_TABLE);

        return session.execute(select).
                all().
                stream().
                map(this::roleFromRow).
                collect(Collectors.toSet());
    }

    private Role roleFromRow(Row row) {
        if (row != null) {
            final Role role = new Role();
            role.setName(row.getString("name"));
            role.setScope(RoleScope.valueOf(row.getInt("scope")));
            role.setDescription(row.getString("description"));
            role.setDefaultRole(row.getBool("default"));
            role.setSystem(row.getBool("system"));
            List<Integer> permissions = row.getList("permissions", Integer.class);
            if (permissions != null) {
                int[] ints = new int[permissions.size()];
                for (int i = permissions.size() - 1; i >= 0; i--) {
                    ints[i] = permissions.get(i);
                }
                role.setPermissions(ints);
            }
            role.setCreatedAt(row.getTimestamp("created_at"));
            role.setUpdatedAt(row.getTimestamp("updated_at"));
            return role;
        }
        return null;
    }
}
