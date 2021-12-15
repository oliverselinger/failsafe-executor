package os.failsafe.executor.utils;

import java.util.concurrent.Phaser;

public class BlockingRunnable implements Runnable {
    public volatile boolean blocking = false;
    Phaser phaser;

    public BlockingRunnable() {
        phaser = new Phaser(2);
    }

    @Override
    public void run() {
        blocking = true;
        phaser.arriveAndAwaitAdvance();
    }

    public void release() {
        phaser.arrive();
        blocking = false;
    }
}
