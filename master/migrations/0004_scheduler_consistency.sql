ALTER TABLE requests
    MODIFY COLUMN sequence BIGINT UNSIGNED NOT NULL,
    DROP INDEX requests_sequence,
    ADD UNIQUE KEY requests_sequence (namespace, sequence);

CREATE TABLE queue_sequences (
    namespace VARCHAR(128) NOT NULL,
    value BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (namespace)
) ENGINE = InnoDB;

INSERT INTO queue_sequences (namespace, value)
SELECT namespace, MAX(sequence)
FROM requests
GROUP BY namespace;

ALTER TABLE operations
    ADD COLUMN completed BOOLEAN NOT NULL DEFAULT TRUE;
