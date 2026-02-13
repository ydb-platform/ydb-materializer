package tech.ydb.mv.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import tech.ydb.mv.model.MvViewExpr;

/**
 * Row filter for a single target's scan.
 *
 * @author zinal
 */
public class MvRowFilter {

    private final MvViewExpr target;
    private MvViewExpr transformation;
    private final ArrayList<Block> blocks = new ArrayList<>();

    /**
     * Create filter for a specific view part.
     *
     * @param target View part for which the filter is built.
     */
    public MvRowFilter(MvViewExpr target) {
        this.target = target;
    }

    public ArrayList<Block> getBlocks() {
        return blocks;
    }

    public MvViewExpr getTarget() {
        return target;
    }

    public MvViewExpr getTransformation() {
        return transformation;
    }

    public void setTransformation(MvViewExpr transformation) {
        this.transformation = transformation;
    }

    /**
     * Test whether a given row matches any of the filter blocks.
     *
     * @param row Row values in the transformation output order.
     * @return {@code true} if any block matches the row.
     */
    public boolean matches(Comparable<?>[] row) {
        for (Block block : blocks) {
            if (block.matches(row)) {
                // any matching block means that the row matches
                return true;
            }
        }
        return false;
    }

    /**
     * @return {@code true} if there are no blocks or all blocks are empty.
     */
    public boolean isEmpty() {
        if (blocks.isEmpty()) {
            return true;
        }
        int countNonEmpty = blocks.stream()
                .mapToInt(b -> b.isEmpty() ? 0 : 1)
                .sum();
        return (countNonEmpty == 0);
    }

    /**
     * Add a filter block defined by a contiguous subrange of the columns.
     *
     * @param startPos Start position of the subrange (0-based).
     * @param length Length of the subrange.
     * @param tuples Set of tuples to match against.
     */
    public void addBlock(int startPos, int length, Collection<? extends MvTuple> tuples) {
        var block = new Block(startPos, length);
        for (var tuple : tuples) {
            block.tuples.add(new MvTuple(tuple.values));
        }
        blocks.add(block);
    }

    @Override
    public String toString() {
        return "MvRowFilter{" + "target=" + target + ", transformation=" + transformation + ", blocks=" + blocks + '}';
    }

    /**
     * A single block of tuple matches for a row subrange.
     */
    public static class Block {

        private final HashSet<MvTuple> tuples = new HashSet<>();
        private final int startPos;
        private final int length;

        /**
         * Create a block.
         *
         * @param startPos Start position of the subrange (0-based).
         * @param length Length of the subrange.
         */
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

        /**
         * Test whether the row matches any tuple in this block.
         *
         * @param row Row values in the transformation output order.
         * @return {@code true} if the extracted subrange matches one of the
         * tuples.
         */
        public boolean matches(Comparable<?>[] row) {
            Comparable<?>[] part = new Comparable<?>[length];
            for (int i = 0; i < length; ++i) {
                part[i] = row[startPos + i];
            }
            return tuples.contains(new MvTuple(part));
        }

        @Override
        public String toString() {
            return "Block{" + startPos + "/" + length + ": " + tuples + '}';
        }
    }

}
