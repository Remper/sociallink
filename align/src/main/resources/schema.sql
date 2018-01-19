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

-- Index: uid_obj

-- DROP INDEX public.uid_obj;

CREATE INDEX uid_obj
    ON public.user_objects USING btree
    (uid);

-- Table: public.user_text

-- DROP TABLE public.user_text;

CREATE TABLE public.user_text
(
  uid bigint NOT NULL,
  lsa real[] NOT NULL,
  CONSTRAINT user_text_pkey PRIMARY KEY (uid)
)
WITH (
  OIDS = FALSE
);

-- Index: uid_text

-- DROP INDEX public.uid_text;

CREATE INDEX uid_text
  ON public.user_text USING btree
  (uid);