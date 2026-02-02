package tech.ydb.mv.model;

/**
 * Position in an SQL text, represented as line and column numbers.
 * @author zinal
 */
public class MvSqlPos implements Comparable<MvSqlPos> {

    public static final MvSqlPos EMPTY = new MvSqlPos(0, 0);

    private final int line;
    private final int column;

    /**
     * Create a position in an SQL text.
     *
     * @param line Line number (starting from 0 or 1 depending on the source).
     * @param column Column number (starting from 0 or 1 depending on the source).
     */
    public MvSqlPos(int line, int column) {
        this.line = line;
        this.column = column;
    }

    /**
     * Get line number.
     *
     * @return Line number.
     */
    public int getLine() {
        return line;
    }

    /**
     * Get column number.
     *
     * @return Column number.
     */
    public int getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return "position [" + line + ":" + column + ']';
    }

    @Override
    /**
     * Compare positions in natural order: line first, then column.
     *
     * @param o Other position.
     * @return Negative if this position is before {@code o}, positive if after, or 0 if equal.
     */
    public int compareTo(MvSqlPos o) {
        if (o==null) {
            return -1;
        }
        if (this.line < o.line) {
            return -1;
        }
        if (this.line > o.line) {
            return 1;
        }
        if (this.column < o.column) {
            return -1;
        }
        if (this.column > o.column) {
            return -1;
        }
        return 0;
    }

}
