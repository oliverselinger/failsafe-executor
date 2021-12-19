package os.failsafe.executor.utils;

import java.util.concurrent.Phaser;

public class BlockingRunnable implements Runnable {

    Phaser setupPhaser;
    Phaser finishPhaser;

    public BlockingRunnable() {
        this(2);
    }

    public BlockingRunnable(int parties) {
        setupPhaser = new Phaser(parties);
        finishPhaser = new Phaser(parties);
    }

    @Override
    public void run() {
        setupPhaser.arriveAndAwaitAdvance();
        finishPhaser.arriveAndAwaitAdvance();
    }

    public void waitForSetup() {
        setupPhaser.arriveAndAwaitAdvance();
    }

    public void release() {
        finishPhaser.arriveAndAwaitAdvance();
    }

    public void waitForSetupAndRelease() {
        waitForSetup();
        release();
    }
}
