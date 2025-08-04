package tech.ydb.mv.model;

/**
 *
 * @author zinal
 */
public class MvSqlPos {

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

}
