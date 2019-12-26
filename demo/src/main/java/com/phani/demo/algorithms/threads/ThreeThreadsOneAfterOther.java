package com.phani.demo.algorithms.threads;

import java.util.concurrent.locks.ReentrantLock;

public class ThreeThreadsOneAfterOther {

    ReentrantLock lock1 = new ReentrantLock();
    ReentrantLock lock2 = new ReentrantLock();
    ReentrantLock lock3 = new ReentrantLock();

    public static void main(String[] args) {

        ThreeThreadsOneAfterOther three = new ThreeThreadsOneAfterOther();
        three.runThreads();


    }

    void runThreads(){
        Runnable task1 = () -> {
            for(int i=0;i<10;i++){
                System.out.print(0);
                lock2.unlock();
                lock1.lock();
            }
        };

        Runnable task2 = () -> {
            lock2.lock();
            for(int i=1;i<20;i=i+2){
                System.out.print(i);
                lock3.unlock();
                lock2.lock();
            }
        };

        Runnable task3 = () -> {
            lock3.lock();
            for(int i=2;i<20;i=i+2){
                System.out.print(i);
                lock1.unlock();
                lock3.lock();
            }
        };



        task1.run();
        task2.run();
        task3.run();
    }
}
