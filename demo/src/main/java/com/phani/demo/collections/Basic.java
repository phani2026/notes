package com.phani.demo.collections;

import scala.collection.mutable.HashTable;

import java.util.*;
import java.util.concurrent.*;

public class Basic {
    public static void main(String[] args) {



        //List
        List<Object> arrayList = new ArrayList<>();
        List<Object> linkedList = new LinkedList<>();
        List<Object> vector = new Vector<>();
        List<Object> stack = new Stack<>();
        // ** Concurrent
        List<Object> copyOnWriteArrayList = new CopyOnWriteArrayList<>();


        //Queue
        Queue<Object> priorityQueue = new PriorityQueue<>();
        Queue<Object> arrayDeQue = new ArrayDeque<>();
        // ** Concurrent
        Queue<Object> linkedBlockingQueue = new LinkedBlockingQueue<>();
        Queue<Object> linkedBlockingDeQue = new LinkedBlockingDeque<>();
        Queue<Object> synchronousQueue = new SynchronousQueue<>();
        Queue<Object> arrayBlockingQueue = new ArrayBlockingQueue<>(1);
        Queue<Object> priorityBlockingQueue = new PriorityBlockingQueue<>();
        Queue<DelayObject> delayQueue = new DelayQueue<DelayObject>();
        Queue<Object> concurrentLinkedQueue = new ConcurrentLinkedQueue<>();
        Queue<Object> concurrentLinkedDeQue = new ConcurrentLinkedDeque<>();




        //Set
        Set<Object> hashSet = new HashSet<>();
        Set<Object> linkedHashSet = new LinkedHashSet<>();
        Set<Object> treeSet = new TreeSet<>();
        // ** Concurrent
        Set<Object> concurrentSkipListSet = new ConcurrentSkipListSet<>();
        Set<Object> copyOnWriteArraySet = new CopyOnWriteArraySet<>();






        //Map
        Map<Object, Object> hashMap = new HashMap<>();
        Map<Object, Object> treeMap = new TreeMap<>();

        // ** Concurrent
        Map<Object, Object> concurrentHashMap = new ConcurrentHashMap<>();




    }


}
