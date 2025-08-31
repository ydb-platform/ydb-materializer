package tech.ydb.mv.model;

/**
 *
 * @author zinal
 */
public class MvSqlPos implements Comparable<MvSqlPos> {

    public static final MvSqlPos EMPTY = new MvSqlPos(0, 0);

    private final int line;
    private final int column;

    public MvSqlPos(int line, int column) {
        this.line = line;
        this.column = column;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return "position [" + line + ":" + column + ']';
    }

    @Override
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
