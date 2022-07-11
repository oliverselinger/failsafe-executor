package os.failsafe.executor;

public class Sort {

    public String field;
    public Direction direction;

    public Sort(Field field, Direction d) {
        this.field = field.name();
        this.direction = d;
    }

    @Override
    public String toString() {
        return field + ' ' + direction.toString();
    }

    public enum Direction {
        ASC, DESC;

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }
    }

    public enum Field {
        ID, FAIL_TIME, CREATED_DATE, NAME, PARAMETER, RETRY_COUNT
    }

}
