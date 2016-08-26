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
package io.gravitee.repository.cassandra;

import io.gravitee.repository.Repository;
import io.gravitee.repository.Scope;
import io.gravitee.repository.cassandra.management.ManagementRepositoryConfiguration;
import io.gravitee.repository.cassandra.ratelimit.RateLimitRepositoryConfiguration;

/**
 * @author Adel Abdelhak (adel.abdelhak@leansys.fr)
 */
public class CassandraRepository implements Repository {

    /**
     * @return the type of repository
     */
    @Override
    public String type() {
        return "cassandra";
    }

    /**
     * @return Scopes handled by this repository
     */
    @Override
    public Scope[] scopes() {
        return new Scope[]{ Scope.MANAGEMENT, Scope.RATE_LIMIT };
    }

    /**
     * Get the correct configuration class for specified scope
     * @param scope current scope
     * @return configuration class for current scope
     */
    @Override
    public Class<?> configuration(Scope scope) {
        switch (scope) {
            case MANAGEMENT:
                return ManagementRepositoryConfiguration.class;
            case RATE_LIMIT:
                return RateLimitRepositoryConfiguration.class;
        }
        return null;
    }
}
