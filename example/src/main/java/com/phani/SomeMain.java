package com.phani;

import java.util.HashSet;

public class SomeMain {

    public static void main(String[] args) {

        HashSet<String> hashSet = new HashSet<>();
        String str = "hi";
        hashSet.add(str);
        hashSet.add(str);

        System.out.println(hashSet.size());
    }
}
