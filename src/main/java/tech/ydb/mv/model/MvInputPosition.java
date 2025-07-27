package tech.ydb.mv.model;

/**
 *
 * @author zinal
 */
public class MvInputPosition {
    
    private final int line;
    private final int column;

    public MvInputPosition(int line, int column) {
        this.line = line;
        this.column = column;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }
    
}
