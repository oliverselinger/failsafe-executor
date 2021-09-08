package os.failsafe.executor;

import java.sql.Connection;

/**
 * This is a functional interface which represents a lambda, accepting a connection and a parameter.
 */
public interface TransactionalTaskFunction<T> {
    void accept(Connection connection, T param) throws Exception;
}
