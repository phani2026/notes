package com.phani.demo.algorithms;

public class LongestCommonSubSequence
{
    //https://blog.usejournal.com/top-50-dynamic-programming-practice-problems-4208fed71aa3



    char[] a1 = "asdfgdvc".toCharArray();
    char[] a2 = "sdfcvded".toCharArray();

    public static void main(String[] args) {

        LongestCommonSubSequence seq = new LongestCommonSubSequence();
        int subseq = seq.recLCS(0,0);
        System.out.println(subseq);
    }


    //Recursion
    private int recLCS(int i, int j)
    {
        if(a1.length<=i || a2.length<=j){
            return 0;
        }
        else if(a1[i]==a2[j]){
            return 1 + recLCS(i+1,j+1);
        }else{
            return Math.max(recLCS(i+1,j),recLCS(i,j+1));
        }
    }

    public static int LCSLength(String X, String Y)
    {
        int m = X.length(), n = Y.length();

        // lookup table stores solution to already computed sub-problems
        // i.e. T[i][j] stores the length of LCS of substring
        // X[0..i-1] and Y[0..j-1]
        int[][] T = new int[m + 1][n + 1];

        // fill the lookup table in bottom-up manner
        for (int i = 1; i <= m; i++)
        {
            for (int j = 1; j <= n; j++)
            {
                // if current character of X and Y matches
                if (X.charAt(i - 1) == Y.charAt(j - 1)) {
                    T[i][j] = T[i - 1][j - 1] + 1;
                }
                // else if current character of X and Y don't match,
                else {
                    T[i][j] = Integer.max(T[i - 1][j], T[i][j - 1]);
                }
            }
        }

        // LCS will be last entry in the lookup table
        return T[m][n];
    }
}
