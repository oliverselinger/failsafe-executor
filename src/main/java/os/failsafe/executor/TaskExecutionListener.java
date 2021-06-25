package os.failsafe.executor;

public interface TaskExecutionListener {

    /**
     * Called, before a task is persisted in the database.
     *
     * @param name       the name of the task that should be executed
     * @param id         the id of the task used as unique constraint in database
     * @param parameter  the parameter that should be passed to the function
     */
    void persisting(String name, String id, String parameter);

    /**
     * Called, before a task is retried.
     *
     * @param name      the name of the task that should be executed
     * @param id        the id of the task used as unique constraint in database
     * @param parameter the parameter that should be passed to the function
     */
    void retrying(String name, String id, String parameter);

    /**
     * Called, after a task got executed without throwing an exception.
     *
     * @param name      the name of the task that should be executed
     * @param id        the id of the task used as unique constraint in database
     * @param parameter the parameter that should be passed to the function
     */
    void succeeded(String name, String id, String parameter);

    /**
     * Called, after a task execution raised an exception and therefore resulted in a failure.
     *
     * @param name      the name of the task that should be executed
     * @param id        the id of the task used as unique constraint in database
     * @param parameter the parameter that should be passed to the function
     * @param exception the thrown exception
     */
    void failed(String name, String id, String parameter, Exception exception);
}
