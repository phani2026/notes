package com.phani.demo.algorithms;

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.regex.*;
import java.util.stream.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;



class Result {

    /*
     * Complete the 'minimumMoves' function below.
     *
     * The function is expected to return an INTEGER.
     * The function accepts following parameters:
     *  1. INTEGER_ARRAY a
     *  2. INTEGER_ARRAY m
     */

    public static int minimumMoves(List<Integer> a, List<Integer> m) {
        // Write your code here
        int moves = 0;
        for(int i=0;i<a.size();i++){

            int length = 0;
            int l1 = a.get(i).toString().length();
            int l2 = m.get(i).toString().length();
            if(l1>l2){
                length = l1;

            }else if(l2>l1){
                length = l2;
            }


            char[] num1 = a.get(i).toString().toCharArray();
            char[] num2 = m.get(i).toString().toCharArray();



            for(int j=0;j<length;j++){

                if(num1[j]==num2[j]){
                    continue;
                }else{
                    int n1 = Integer.parseInt(""+num1[j]);
                    int n2 = Integer.parseInt(""+num2[j]);
                    if(n1>n2){
                        moves = moves + (n1-n2);
                    }else{
                        moves = moves + (n2-n1);
                    }

                }
            }

        }
        return moves;
    }

}

public class Solution {
    public static void main(String[] args) throws IOException {
        List<Integer> m = new ArrayList<>();
        List<Integer> a = new ArrayList<>();

        a.add(1234);

        m.add(2345);


        int result = Result.minimumMoves(a, m);
        System.out.println(result);


    }
}
