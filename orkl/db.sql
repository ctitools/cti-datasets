-- Database creation and setup for the ORKL.eu dataset.

CREATE DATABASE orkl;
CONNECT TO orkl;

-- schema.sql

-- Create the main entries table
CREATE TABLE entries (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP,
    sha1_hash VARCHAR(40) NOT NULL,
    title TEXT NOT NULL,
    authors TEXT NOT NULL,
    file_creation_date TIMESTAMP NOT NULL,
    file_modification_date TIMESTAMP NOT NULL,
    file_size BIGINT NOT NULL,
    plain_text TEXT NOT NULL,
    language VARCHAR(10) NOT NULL,
    ts_created_at BIGINT NOT NULL,
    ts_updated_at BIGINT NOT NULL,
    ts_creation_date BIGINT NOT NULL,
    ts_modification_date BIGINT NOT NULL,
    files_pdf TEXT NOT NULL,
    files_text TEXT NOT NULL,
    files_img TEXT NOT NULL
);

-- Create the sources table
CREATE TABLE sources (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    description TEXT NOT NULL,
    reports TEXT
);

-- Create the entries_sources join table
CREATE TABLE entries_sources (
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    source_id UUID REFERENCES sources(id) ON DELETE CASCADE,
    PRIMARY KEY (entry_id, source_id)
);

-- Create the references table
CREATE TABLE references (
    id SERIAL PRIMARY KEY,
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    reference TEXT NOT NULL
);

-- Create the report_names table
CREATE TABLE report_names (
    id SERIAL PRIMARY KEY,
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    report_name TEXT NOT NULL
);

-- Create the threat_actors table
CREATE TABLE threat_actors (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP,
    main_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    tools TEXT[],
    source_id TEXT NOT NULL,
    reports TEXT
);

-- Create the threat_actors_aliases table
CREATE TABLE threat_actors_aliases (
    threat_actor_id UUID REFERENCES threat_actors(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    PRIMARY KEY (threat_actor_id, alias)
);

-- Create the entries_threat_actors join table
CREATE TABLE entries_threat_actors (
    entry_id UUID REFERENCES entries(id) ON DELETE CASCADE,
    threat_actor_id UUID REFERENCES threat_actors(id) ON DELETE CASCADE,
    PRIMARY KEY (entry_id, threat_actor_id)
);
