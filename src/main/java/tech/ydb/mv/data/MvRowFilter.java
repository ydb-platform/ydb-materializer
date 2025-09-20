package tech.ydb.mv.data;

import java.util.ArrayList;
import java.util.HashSet;

import tech.ydb.mv.model.MvTarget;

/**
 * Row filter for a single target's scan.
 *
 * @author zinal
 */
public class MvRowFilter {

    private final MvTarget target;
    private MvTarget transformation;
    private final ArrayList<Block> blocks = new ArrayList<>();

    public MvRowFilter(MvTarget target) {
        this.target = target;
    }

    public ArrayList<Block> getBlocks() {
        return blocks;
    }

    public MvTarget getTarget() {
        return target;
    }

    public MvTarget getTransformation() {
        return transformation;
    }

    public void setTransformation(MvTarget transformation) {
        this.transformation = transformation;
    }

    public boolean matches(Comparable<?>[] row) {
        for (Block block : blocks) {
            if (! block.matches(row)) {
                return false;
            }
        }
        return true;
    }

    public boolean isEmpty() {
        if (blocks.isEmpty()) {
            return true;
        }
        int countNonEmpty = blocks.stream()
                .mapToInt(b -> b.isEmpty() ? 0 : 1)
                .sum();
        return (countNonEmpty == 0);
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

        public boolean isEmpty() {
            return tuples.isEmpty();
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
