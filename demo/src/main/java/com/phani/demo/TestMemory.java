package com.phani.demo;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestMemory {

    public static void main(String[] args) {



        File ldir = new File("/tmp/test/topic1/");
        File rdir = new File("/tmp/test/topic1/");
        System.out.println(ldir.list());

        Set<String> directoryListing = new HashSet<>(Arrays.stream(ldir.list()).collect(Collectors.toSet()));
        directoryListing.addAll(new HashSet<>(Arrays.stream(rdir.list()).collect(Collectors.toSet())));
        for(String s:directoryListing){
            System.out.println(s);
        }

        System.exit(0);

        Map<String, String[]> map = new HashMap<>();

//        String x = "1239991239";
//        IntStream chars = x.chars();
//        //System.out.println(chars.sum());
//        long t1 = System.currentTimeMillis();
//        System.out.println(String.valueOf(x).chars().map(Character::getNumericValue).sum());
//        System.out.println(System.currentTimeMillis()-t1);
//
//
//        //System.out.println(x%10000000+"");
//
//        //System.out.println(x.hashCode());
//        System.exit(0);

        for(int i=0;i<1000000000;i++){
            String[] arr = {"abcdef", "asdfgedf"};
            String key = "key"+i;
            map.put(key,arr);
            System.out.println(generateHashCode(key.hashCode()));
            if(i%1000000==0){
                System.out.println(i);
            }
        }

    }

    public static int generateHashCode(int code) {

        return new HashCodeBuilder(17, 19).
                append(code).
                toHashCode();
    }
}
