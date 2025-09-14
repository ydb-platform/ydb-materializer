package tech.ydb.mv.data;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author zinal
 */
public class MvRowFilter {

    private final ArrayList<Block> blocks = new ArrayList<>();

    public ArrayList<Block> getBlocks() {
        return blocks;
    }

    public boolean matches(Comparable<?>[] row) {
        for (Block block : blocks) {
            if (! block.matches(row)) {
                return false;
            }
        }
        return true;
    }

    public static class Block {
        private final HashSet<MvTuple> tuples = new HashSet<>();
        private final int startPos;
        private final int length;

        public Block(int startPos, int length) {
            this.startPos = startPos;
            this.length = length;
        }

        public HashSet<MvTuple> getTuples() {
            return tuples;
        }

        public int getStartPos() {
            return startPos;
        }

        public int getLength() {
            return length;
        }

        public boolean matches(Comparable<?>[] row) {
            Comparable<?>[] part = new Comparable<?>[length];
            for (int i = 0; i < length; ++i) {
                part[i] = row[startPos + i];
            }
            return tuples.contains(new MvTuple(part));
        }
    }

}
