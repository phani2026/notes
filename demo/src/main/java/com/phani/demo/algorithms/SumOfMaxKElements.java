package com.phani.demo.algorithms;


import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SumOfMaxKElements {

    /**
     * with sorting
     *
     * @param integerList
     * @return
     */
    public static Integer findSumOfMaxKElementsOfArray(List<Integer> integerList, int k){

        //
        Collections.sort(integerList,Collections.reverseOrder());
        int sum = 0;
        for(int i=0;i<k || i<integerList.size();i++){
            sum=sum+integerList.get(i);
        }
        System.out.println(integerList);
        System.out.println(sum);
        return sum;
    }

    /**
     * without sorting
     *
     * @param integerList
     * @return
     */
    public static Integer findSumOfMaxKElementsOfArray(int k, List<Integer> integerList){

        //

        int sum = 0;
        for(int i=0;i<k || i<integerList.size();i++){
            sum=sum+integerList.get(i);
        }
        System.out.println(integerList);
        System.out.println(sum);
        return sum;
    }

    public static void main(String[] args) {

        List<Integer> list = new ArrayList<>();

        list.add(1);
        list.add(10);
        list.add(11);
        list.add(31);
        list.add(2);
        list.add(23);
        list.add(-9);
        list.add(6);

        findSumOfMaxKElementsOfArray(list,4);
        findSumOfMaxKElementsOfArray(4, list);

    }
}
