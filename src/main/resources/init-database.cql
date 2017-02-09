CREATE KEYSPACE IF NOT EXISTS managementgravitee WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
CREATE KEYSPACE IF NOT EXISTS ratelimitgravitee WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

CREATE TABLE IF NOT EXISTS managementgravitee.views (id text PRIMARY KEY, name text, description text);
CREATE TABLE IF NOT EXISTS managementgravitee.tags (id text PRIMARY KEY, name text, description text);
CREATE TABLE IF NOT EXISTS managementgravitee.apikeys (key text PRIMARY KEY, subscription text, application text, plan text, expire_at timestamp, created_at timestamp, updated_at timestamp, revoked_at timestamp, revoked boolean);
CREATE TABLE IF NOT EXISTS managementgravitee.apis (id text PRIMARY KEY, name text, description text, version text, definition text, deployed_at timestamp, created_at timestamp, updated_at timestamp, visibility text, lifecycle_state text, picture text, group text, views set<text>);
CREATE TABLE IF NOT EXISTS managementgravitee.applications (id text PRIMARY KEY, name text, description text, type text, created_at timestamp, updated_at timestamp, group text);
CREATE TABLE IF NOT EXISTS managementgravitee.events (id text PRIMARY KEY, type text, payload text, parent_id text, properties map<text, text>, created_at timestamp, updated_at timestamp);
CREATE TABLE IF NOT EXISTS managementgravitee.groups (id text PRIMARY KEY, type text, name text, administrators list<text>, created_at timestamp, updated_at timestamp);
CREATE TABLE IF NOT EXISTS managementgravitee.memberships (user_id text, reference_id text, reference_type text, type text, created_at timestamp, updated_at timestamp, PRIMARY KEY (user_id, reference_id, reference_type));
CREATE TABLE IF NOT EXISTS managementgravitee.pages (id text PRIMARY KEY, name text, type text, content text, last_contributor text, page_order int, published boolean, source_type text, source_configuration text, configuration_tryiturl text, configuration_tryit boolean, api text, created_at timestamp, updated_at timestamp);
CREATE TABLE IF NOT EXISTS managementgravitee.plans (id text PRIMARY KEY, name text, description text, validation text, type text, status text, plan_order int, apis set<text>, created_at timestamp, updated_at timestamp, definition text, characteristics list<text>, security text, published_at timestamp, closed_at timestamp);
CREATE TABLE IF NOT EXISTS managementgravitee.users (username text PRIMARY KEY, source text, source_id text, password text, email text, firstname text, lastname text, roles set<text>, created_at timestamp, updated_at timestamp, last_connection_at timestamp, picture text);
CREATE TABLE IF NOT EXISTS managementgravitee.subscriptions (id text PRIMARY KEY, plan text, application text, reason text, status text, created_at timestamp, updated_at timestamp, processed_at timestamp, starting_at timestamp, ending_at timestamp, processed_by text, subscribed_by text);

CREATE TABLE IF NOT EXISTS ratelimitgravitee.ratelimits (key text PRIMARY KEY, last_request bigint, counter bigint, reset_time bigint, created_at bigint, updated_at bigint, async boolean);
