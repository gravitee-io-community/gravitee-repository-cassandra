CREATE KEYSPACE IF NOT EXISTS gravitee WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

CREATE TABLE IF NOT EXISTS gravitee.tenants (id text PRIMARY KEY, name text, description text);
CREATE TABLE IF NOT EXISTS gravitee.views (id text PRIMARY KEY, name text, description text);
CREATE TABLE IF NOT EXISTS gravitee.tags (id text PRIMARY KEY, name text, description text);
CREATE TABLE IF NOT EXISTS gravitee.apikeys (id text PRIMARY KEY, subscription text, application text, plan text, expire_at timestamp, created_at timestamp, updated_at timestamp, revoked_at timestamp, revoked boolean);
CREATE TABLE IF NOT EXISTS gravitee.apis (id text PRIMARY KEY, name text, description text, version text, definition text, deployed_at timestamp, created_at timestamp, updated_at timestamp, visibility text, lifecycle_state text, picture text, groups set<text>, views set<text>, labels list<text>);
CREATE TABLE IF NOT EXISTS gravitee.applications (id text PRIMARY KEY, name text, description text, type text, created_at timestamp, updated_at timestamp, groups set<text>, status text);
CREATE TABLE IF NOT EXISTS gravitee.events (id text PRIMARY KEY, type text, payload text, parent_id text, properties map<text, text>, created_at timestamp, updated_at timestamp);
CREATE TABLE IF NOT EXISTS gravitee.groups (id text PRIMARY KEY, name text, administrators list<text>, event_rules list<text>, created_at timestamp, updated_at timestamp);
CREATE TABLE IF NOT EXISTS gravitee.memberships (user_id text, reference_id text, reference_type text, roles set<text>, created_at timestamp, updated_at timestamp, PRIMARY KEY (user_id, reference_id, reference_type));
CREATE TABLE IF NOT EXISTS gravitee.pages (id text PRIMARY KEY, name text, type text, content text, last_contributor text, page_order int, published boolean, source_type text, source_configuration text, configuration_tryiturl text, configuration_tryit boolean, api text, excludedGroups list<text>, created_at timestamp, updated_at timestamp, homepage boolean);
CREATE TABLE IF NOT EXISTS gravitee.plans (id text PRIMARY KEY, name text, description text, validation text, type text, status text, plan_order int, apis set<text>, created_at timestamp, updated_at timestamp, definition text, characteristics list<text>, security text, excludedGroups list<text>, published_at timestamp, closed_at timestamp);
CREATE TABLE IF NOT EXISTS gravitee.users (username text PRIMARY KEY, source text, source_id text, password text, email text, firstname text, lastname text, roles set<text>, created_at timestamp, updated_at timestamp, last_connection_at timestamp, picture text);
CREATE TABLE IF NOT EXISTS gravitee.subscriptions (id text PRIMARY KEY, plan text, application text, reason text, status text, created_at timestamp, updated_at timestamp, processed_at timestamp, starting_at timestamp, ending_at timestamp, processed_by text, subscribed_by text, closed_at timestamp);
CREATE TABLE IF NOT EXISTS gravitee.metadata (key text, reference_id text, reference_type text, name text, format text, value text, created_at timestamp, updated_at timestamp, PRIMARY KEY (key, reference_id, reference_type));
CREATE TABLE IF NOT EXISTS gravitee.roles (scope int, name text, description text, default boolean, system boolean, permissions list<int>, created_at timestamp, updated_at timestamp, PRIMARY KEY (scope, name));

CREATE TABLE IF NOT EXISTS gravitee.ratelimits (id text PRIMARY KEY, last_request bigint, counter bigint, reset_time bigint, created_at bigint, updated_at bigint, async boolean);
