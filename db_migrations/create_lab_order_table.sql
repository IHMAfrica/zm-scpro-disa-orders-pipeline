-- Table: crt.lab_order
-- Lab order messages from healthcare facilities to DISA lab (with lab_code)

-- DROP TABLE IF EXISTS crt.lab_order;

CREATE TABLE IF NOT EXISTS crt.lab_order
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    mfl_code text COLLATE pg_catalog."default" NOT NULL,
    order_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    test_id text COLLATE pg_catalog."default" NOT NULL,
    order_date date NOT NULL DEFAULT CURRENT_DATE,
    order_time time without time zone NOT NULL DEFAULT CURRENT_TIME,
    message_ref_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
    sending_application character varying(25) COLLATE pg_catalog."default",
    lab_code character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT lab_order_pkey PRIMARY KEY (id),
    CONSTRAINT lab_order_mfl_code_fk FOREIGN KEY (mfl_code)
        REFERENCES ref.facilities (mfl_code) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT lab_order_lab_code_fk FOREIGN KEY (lab_code)
        REFERENCES ref.lab_prefixes (lab_code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE RESTRICT
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS crt.lab_order
    OWNER to hie_manager_user;

-- Index: idx_lab_order_facility_date
CREATE INDEX IF NOT EXISTS idx_lab_order_facility_date
    ON crt.lab_order USING btree
    (mfl_code COLLATE pg_catalog."default" ASC NULLS LAST, order_date DESC NULLS FIRST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_facility_test
CREATE INDEX IF NOT EXISTS idx_lab_order_facility_test
    ON crt.lab_order USING btree
    (mfl_code COLLATE pg_catalog."default" ASC NULLS LAST, test_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_message_ref_id (UNIQUE)
CREATE UNIQUE INDEX IF NOT EXISTS idx_lab_order_message_ref_id
    ON crt.lab_order USING btree
    (message_ref_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_mfl_code
CREATE INDEX IF NOT EXISTS idx_lab_order_mfl_code
    ON crt.lab_order USING btree
    (mfl_code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_order_date
CREATE INDEX IF NOT EXISTS idx_lab_order_order_date
    ON crt.lab_order USING btree
    (order_date DESC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_order_id
CREATE INDEX IF NOT EXISTS idx_lab_order_order_id
    ON crt.lab_order USING btree
    (order_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_sending_facility
CREATE INDEX IF NOT EXISTS idx_lab_order_sending_facility
    ON crt.lab_order USING btree
    (sending_application COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_test_id
CREATE INDEX IF NOT EXISTS idx_lab_order_test_id
    ON crt.lab_order USING btree
    (test_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_lab_order_lab_code (NEW)
CREATE INDEX IF NOT EXISTS idx_lab_order_lab_code
    ON crt.lab_order USING btree
    (lab_code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
