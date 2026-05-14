BEGIN;

CREATE TABLE IF NOT EXISTS student_library_log (
    log_id SERIAL PRIMARY KEY,
    student_id INT NOT NULL REFERENCES student(student_id) ON DELETE CASCADE,
    student_code_snapshot VARCHAR(100) NOT NULL,
    student_name_snapshot VARCHAR(255),
    grade_snapshot VARCHAR(50),
    section_snapshot VARCHAR(100),
    time_in TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_out TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    recorded_by INT REFERENCES librarian(librarian_id),
    CONSTRAINT student_library_log_time_order_check
        CHECK (time_out IS NULL OR time_out >= time_in)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_student_library_log_one_open
ON student_library_log(student_id)
WHERE time_out IS NULL;

CREATE INDEX IF NOT EXISTS idx_student_library_log_time_in_desc
ON student_library_log(time_in DESC);

CREATE INDEX IF NOT EXISTS idx_student_library_log_student_time
ON student_library_log(student_id, time_in DESC);

CREATE INDEX IF NOT EXISTS idx_student_library_log_grade_section
ON student_library_log(grade_snapshot, section_snapshot);

COMMIT;
