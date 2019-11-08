package com.phani.demo.algorithms;

public class BinaryGap {

    public static void main(String[] args) {

        String number = Integer.toBinaryString(100);

        char[] binaryNumber = number.toCharArray();

        int gap = 0;
        boolean one = false;
        boolean zero = false;
        for(char c:binaryNumber){
            if(c=='1')
            {
                if(zero && one){
                    gap++;
                    zero=false;
                }
                one = true;
            }
            else {
                zero=true;
            }
        }

        System.out.println("Number: "+number +" Gap: "+gap);
    }
}
