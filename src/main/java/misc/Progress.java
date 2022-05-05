package misc;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class Progress {

    int width = 20;
    AtomicInteger progress = new AtomicInteger(0);
    int max;
    long time;
    long start;

    public Progress(int max) {
        this.max = max;
        start();
    }

    public Progress(int max, int width){
        this.max = max;
        this.width = width;
        start();
    }

    private void start(){
        start = System.nanoTime();
        tic();
        render();
    }

    private void render(){


        clear();
        renderPercent();
        renderBar();
        renderCount();
        renderTime();

        if (progress.get() == max) System.out.println();
    }

    private void tic(){
        time = System.nanoTime();
    }

    private long toc(){
        return System.nanoTime() - time;
    }

    private void renderPercent(){
        float p = progress.get() / (float)max * 100;
        System.out.printf("%3.0f%% ", p);
        System.out.print("|");
    }

    private void renderTime(){
        Duration duration = Duration.ofNanos(System.nanoTime() - start);
        Duration rem = Duration.ofNanos(toc() * (max - progress.get()));
        tic();
        if (progress.get() != max) {
            System.out.printf("(%d:%d:%d / %d:%d:%d)", duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart(), rem.toHoursPart(), rem.toMinutesPart(), rem.toSecondsPart());

        } else {
            System.out.printf("(%d:%d:%d)", duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart());

        }
    }

    private void clear(){
        System.out.print("\r");
    }

    private void renderBar(){
        if (progress.get() != max){
            System.out.print("\u001B[34m");
        } else {
            System.out.print("\u001B[32m");
        }


        for (int i = 0; i < width; i++) {
            if (i / (float)width < progress.get() / (float)max){
                System.out.print("█");
            } else {
                System.out.print(" ");
            }
        }
        System.out.print("\u001B[0m");
        System.out.print("| ");
    }

    private void renderCount(){
        int ds = (int)Math.floor(Math.log10(max)) + 1;
        System.out.printf(" %" + ds + "d/" + max + " ", progress.get());
    }
    public void step(){
        progress.incrementAndGet();
        render();
    }

}
