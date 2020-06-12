CREATE TABLE FAILSAFE_TASK (
    ID VARCHAR(36) NOT NULL,
    PARAMETER TEXT,
    NAME VARCHAR(200) NOT NULL,
    PLANNED_EXECUTION_TIME TIMESTAMP,
    LOCK_TIME TIMESTAMP,
    FAIL_TIME TIMESTAMP,
    EXCEPTION_MESSAGE VARCHAR(1000),
    STACK_TRACE TEXT,
    VERSION INT DEFAULT 0,
    CREATED_DATE TIMESTAMP,
    PRIMARY KEY (ID,NAME)
);

CREATE INDEX idx_failsafe_task_name ON FAILSAFE_TASK(NAME);
CREATE INDEX idx_failsafe_task_timestamp ON FAILSAFE_TASK(FAIL_TIME, LOCK_TIME, CREATED_DATE);