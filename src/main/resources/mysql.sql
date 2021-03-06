CREATE TABLE FAILSAFE_TASK (
    ID VARCHAR(36) NOT NULL,
    PARAMETER MEDIUMTEXT,
    NAME VARCHAR(200) NOT NULL,
    PLANNED_EXECUTION_TIME DATETIME(3),
    LOCK_TIME DATETIME(3),
    FAIL_TIME DATETIME(3),
    EXCEPTION_MESSAGE VARCHAR(1000),
    STACK_TRACE MEDIUMTEXT,
    RETRY_COUNT INT DEFAULT 0,
    VERSION INT DEFAULT 0,
    CREATED_DATE DATETIME(3),
    PRIMARY KEY (ID,NAME)
);