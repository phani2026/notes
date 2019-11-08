package com.phani.demo.algorithms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class SumOfTwoDistinctNumbers {

    //Find the set of two distinct numbers who's sum is equal to given number in an array in less than o(n) time
    //Find the set of two distinct numbers who's Diff is equal to given number in an array in less than o(n) time

    public static void main(String[] args) {
//        Integer[] arr = {6,10,20,30,9,3,12,24,10};
//        SumOfTwoDistinctNumbers.calculateSUM(arr,36);

//        Integer[] arr = {6,10,20,30,9,3,12,24,10};
//        SumOfTwoDistinctNumbers.calculateDiff(arr,6);

        Integer[] arr = {0,1,7,4,2,3,5};
        SumOfTwoDistinctNumbers.calculateDiff(arr,2);


    }


    public static void calculateSUM(Integer[] array, int sum)
    {

        Set<Integer> resultSet = new HashSet<>();
        for(Integer num:array)
        {
            if(resultSet.contains(sum - num))
            {
                System.out.println("Result: "+(sum - num)+", "+num);
            }
            else {
                resultSet.add(num);
            }
        }
    }


    public static void calculateDiff(Integer[] array, int sum)
    {

        ArrayList<Integer> resultSet = new ArrayList<>();
        for (Integer num : array) {
            if (resultSet.contains(num - sum)) {

                System.out.println("Result: " + (num - sum) + ", " + num);
                if(resultSet.contains(num+sum))
                    System.out.println("Result: " + (num + sum) + ", " + num);
            }
            resultSet.add(num);
//            resultSet.add(0-num);
        }
        System.out.println(resultSet);
    }
}
