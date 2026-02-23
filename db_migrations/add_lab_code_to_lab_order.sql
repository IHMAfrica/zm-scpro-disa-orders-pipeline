-- Add lab_code column to existing crt.lab_order table

-- Add the lab_code column (nullable initially to avoid issues with existing rows)
ALTER TABLE crt.lab_order
    ADD COLUMN IF NOT EXISTS lab_code character varying(10) COLLATE pg_catalog."default";

-- Add foreign key constraint to ref.lab_prefixes
ALTER TABLE crt.lab_order
    ADD CONSTRAINT lab_order_lab_code_fk FOREIGN KEY (lab_code)
        REFERENCES ref.lab_prefixes (lab_code) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE RESTRICT;

-- Create index on lab_code
CREATE INDEX IF NOT EXISTS idx_lab_order_lab_code
    ON crt.lab_order USING btree
    (lab_code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Set lab_code to NOT NULL constraint (optional - only if all future records will have lab_code)
-- If you have existing rows without lab_code, comment out the line below or backfill first
-- ALTER TABLE crt.lab_order
--     ALTER COLUMN lab_code SET NOT NULL;
