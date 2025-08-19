package tech.ydb.mv.apply;

/**
 * The apply worker is an active object (thread) with the input queue to process.
 * It handles the changes from each MV being handled on the current application instance.
 *
 * @author zinal
 */
public class MvApplyWorker implements Runnable {

    private final MvApplyManager owner;
    private final int number;
    private final Thread thread;

    public MvApplyWorker(MvApplyManager owner, int number) {
        this.owner = owner;
        this.number = number;
        this.thread = new Thread();
    }

    @Override
    public void run() {

    }

}
