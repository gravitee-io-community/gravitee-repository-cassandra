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
import io.gravitee.repository.management.model.Membership;
import io.gravitee.repository.management.model.MembershipReferenceType;
import io.gravitee.repository.management.model.RoleScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * @author LeansysTeam (leansys dot fr)
 */
@Repository
public class CassandraMembershipRepository implements MembershipRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraMembershipRepository.class);

    private final String MEMBERSHIPS_TABLE = "memberships";

    @Autowired
    private Session session;

    @Override
    public Membership create(Membership membership) throws TechnicalException {
        String membershipType = convertRoleToType(membership.getRoleScope(), membership.getRoleName());
        LOGGER.debug("Create Membership {}", membershipType);

        Statement insert = QueryBuilder.insertInto(MEMBERSHIPS_TABLE)
                .values(new String[]{"user_id", "reference_id", "reference_type", "type", "created_at", "updated_at"},
                        new Object[]{membership.getUserId(), membership.getReferenceId(), membership.getReferenceType().toString(),
                                membershipType, membership.getCreatedAt(), membership.getUpdatedAt()});

        session.execute(insert);

        return findById(membership.getUserId(), membership.getReferenceType(), membership.getReferenceId()).orElse(null);
    }

    @Override
    public Membership update(Membership membership) throws TechnicalException {
        String membershipType = convertRoleToType(membership.getRoleScope(), membership.getRoleName());
        LOGGER.debug("Update Membership {}", membershipType);

        Statement update = QueryBuilder.update(MEMBERSHIPS_TABLE)
                .with(set("type", membershipType))
                .and(set("created_at", membership.getCreatedAt()))
                .and(set("updated_at", membership.getUpdatedAt()))
                .where(eq("user_id", membership.getUserId()))
                .and(eq("reference_id", membership.getReferenceId()))
                .and(eq("reference_type", membership.getReferenceType().toString()));

        session.execute(update);

        return findById(membership.getUserId(), membership.getReferenceType(), membership.getReferenceId()).orElse(null);
    }

    @Override
    public void delete(Membership membership) throws TechnicalException {
        LOGGER.debug("Delete Membership with User ID [{}] & Reference ID [{}]", membership.getUserId(), membership.getReferenceId());

        Statement delete = QueryBuilder.delete().from(MEMBERSHIPS_TABLE)
                .where(eq("user_id", membership.getUserId()))
                .and(eq("reference_id", membership.getReferenceId()))
                .and(eq("reference_type", membership.getReferenceType().toString()));

        session.execute(delete);
    }

    @Override
    public Optional<Membership> findById(String userId, MembershipReferenceType referenceType, String referenceId) throws TechnicalException {
        LOGGER.debug("Find Membership by ID [{}]-[{}]-[{}]", userId, referenceType, referenceId);

        if (userId == null) {
            return Optional.empty();
        }

        final Statement select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE)
                .where(eq("user_id", userId))
                .and(eq("reference_id", referenceId))
                .and(eq("reference_type", referenceType.toString()));

        final Row row = session.execute(select).one();

        return Optional.ofNullable(membershipFromRow(row));
    }

    @Override
    public Set<Membership> findByReferenceAndRole(MembershipReferenceType referenceType, String referenceId, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by Reference & MembershipType [{}]-[{}]-[{}]", referenceType, referenceId, membershipType);

        Statement select;
        if (membershipType == null) {
            select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                    .where(eq("reference_id", referenceId))
                    .and(eq("reference_type", referenceType.toString()));
        } else {
            select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                    .where(eq("reference_id", referenceId))
                    .and(eq("reference_type", referenceType.toString()))
                    .and(eq("type", membershipType));
        }

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::membershipFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByReferencesAndRole(MembershipReferenceType referenceType, List<String> referenceIds, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by References & MembershipType [{}]-[{}]", referenceType, membershipType);

        Statement select;
        if (membershipType == null) {
            select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                    .where(in("reference_id", referenceIds))
                    .and(eq("reference_type", referenceType.toString()));
        } else {
            select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                    .where(in("reference_id", referenceIds))
                    .and(eq("reference_type", referenceType.toString()))
                    .and(eq("type", membershipType));
        }

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::membershipFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByUserAndReferenceType(String userId, MembershipReferenceType referenceType) throws TechnicalException {
        LOGGER.debug("Find Membership by User & Reference [{}]-[{}]", userId, referenceType);

        final Statement select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                .where(eq("user_id", userId))
                .and(eq("reference_type", referenceType.toString()));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::membershipFromRow).collect(Collectors.toSet());
    }

    @Override
    public Set<Membership> findByUserAndReferenceTypeAndRole(String userId, MembershipReferenceType referenceType, RoleScope roleScope, String roleName) throws TechnicalException {
        String membershipType = convertRoleToType(roleScope, roleName);
        LOGGER.debug("Find Membership by User, Reference & MembershipType [{}]-[{}]-[{}]", userId, referenceType, membershipType);

        final Statement select = QueryBuilder.select().all().from(MEMBERSHIPS_TABLE).allowFiltering()
                .where(eq("user_id", userId))
                .and(eq("reference_type", referenceType.toString()))
                .and(eq("type", membershipType));

        final ResultSet resultSet = session.execute(select);

        return resultSet.all().stream().map(this::membershipFromRow).collect(Collectors.toSet());
    }

    private Membership membershipFromRow(Row row) {
        if (row != null) {
            final Membership membership = new Membership();
            membership.setUserId(row.getString("user_id"));
            membership.setReferenceId(row.getString("reference_id"));
            membership.setReferenceType(MembershipReferenceType.valueOf(row.getString("reference_type").toUpperCase()));
            String[] scopeAndName = convertTypeToRole(row.getString("type"));
            membership.setRoleScope(Integer.valueOf(scopeAndName[0]));
            membership.setRoleName(scopeAndName[1]);
            membership.setCreatedAt(row.getTimestamp("created_at"));
            membership.setUpdatedAt(row.getTimestamp("updated_at"));
            return membership;
        }
        return null;
    }

    private String convertRoleToType(RoleScope roleScope, String roleName) {
        if (roleName == null) {
            return null;
        }
        return convertRoleToType(roleScope.getId(), roleName);
    }

    private String convertRoleToType(int roleScope, String roleName) {
        return roleScope + ":" + roleName;
    }

    private String[] convertTypeToRole(String type) {
        if(type == null) {
            return null;
        }
        String[] role = type.split(":");
        if (role .length != 2) {
            return null;
        }
        return role;
    }
}
