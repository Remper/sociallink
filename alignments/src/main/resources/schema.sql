-- Table: public.user_sg

-- DROP TABLE public.user_sg;

CREATE TABLE public.user_sg
(
    uid bigint NOT NULL,
    followees bigint[] NOT NULL,
    weights real[] NOT NULL,
    CONSTRAINT user_sg_pkey PRIMARY KEY (uid)
)
WITH (
    OIDS = FALSE
);

-- Table: public.user_index

-- DROP TABLE public.user_index;

CREATE TABLE public.user_index
(
    fullname character varying COLLATE pg_catalog."default" NOT NULL,
    uid bigint NOT NULL,
    freq integer NOT NULL,
    CONSTRAINT user_index_pkey PRIMARY KEY (fullname, uid)
)
WITH (
    OIDS = FALSE
);

-- Index: fullname

-- DROP INDEX public.fullname;

CREATE INDEX fullname
    ON public.user_index USING btree
    (fullname COLLATE pg_catalog."default");

-- Index: fulltext_fullname

-- DROP INDEX public.fulltext_fullname;

CREATE INDEX fulltext_fullname
    ON public.user_index USING gin
    (to_tsvector('english_fullname'::regconfig, fullname::text));

-- Index: uid

-- DROP INDEX public.uid;

CREATE INDEX uid
    ON public.user_index USING btree
    (uid);

-- Table: public.user_objects

-- DROP TABLE public.user_objects;

CREATE TABLE public.user_objects
(
    uid bigint NOT NULL,
    object json NOT NULL,
    CONSTRAINT user_objects_pkey PRIMARY KEY (uid)
)
WITH (
    OIDS = FALSE
);

-- Table: public.user_text

-- DROP TABLE public.user_text;

CREATE TABLE public.user_text
(
  uid bigint NOT NULL,
  text text NOT NULL,
  CONSTRAINT user_text_pkey PRIMARY KEY (uid)
)
WITH (
    OIDS = FALSE
);

-- Table: public.kb_index

-- DROP TABLE public.kb_index;

CREATE TABLE public.kb_index
(
    kbid integer NOT NULL,
    uri text NOT NULL,
    CONSTRAINT kb_index_pkey PRIMARY KEY (kbid)
)
WITH (
    OIDS = FALSE
);

-- Index: uid

-- DROP INDEX public.uid;

CREATE INDEX kb_index_uri
    ON public.kb_index USING btree
    (uri);

-- Table: public.alignments

-- DROP TABLE public.alignments;

CREATE TABLE public.alignments (
    resource_id character varying(255) NOT NULL,
    uid bigint NOT NULL,
    score real NOT NULL,
    is_alignment boolean NOT NULL,
    version smallint NOT NULL,
    CONSTRAINT alignments_pkey PRIMARY KEY (resource_id, uid)
);

CREATE INDEX alignments_version_idx ON public.alignments USING btree (version);