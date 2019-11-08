package com.phani.demo.algorithms;

/**
 *
 * Given a M x N binary matrix, find the size of largest square sub-matrix of 1’s present in it.
 *
 * https://www.techiedelight.com/find-size-largest-square-sub-matrix-1s-present-given-binary-matrix/
 *
 * https://blog.usejournal.com/top-50-dynamic-programming-practice-problems-4208fed71aa3
 */

public class LargestSquare {



    public static int minimum (int x, int y, int z) {
        return Integer.min(Integer.min(x, y), z);
    }

    // Function to find the size of largest square sub-matrix of 1's
    // present in the given binary matrix
    public static int findLargestSquare(int[][] M)
    {
        // T[i][j] stores the size of maximum square
        // sub-matrix ending at M[i][j]
        int[][] T = new int[M.length][M[0].length];

        // max stores the size of largest square sub-matrix of 1's
        int max = 0;

        // fill in bottom-up manner
        for (int i = 0; i < M.length; i++)
        {
            for (int j = 0; j < M[0].length; j++)
            {
                T[i][j] = M[i][j];

                // if we are not at the first row or first column and
                // current cell has value 1
                if (i > 0 && j > 0 && M[i][j] == 1)
                {
                    // largest square sub-matrix ending at M[i][j] will be
                    // 1 plus minimum of largest square sub-matrix
                    // ending at M[i][j-1], M[i-1][j] and M[i-1][j-1]

                    T[i][j] = minimum(T[i][j - 1], T[i - 1][j],
                            T[i - 1][j - 1]) + 1;
                }

                // update maximum size found so far
                if (max < T[i][j]) {
                    max = T[i][j];
                }
            }
        }

        // return size of largest square matrix
        return max;
    }

    public static void main(String[] args)
    {
        // input boolean matrix
        int mat[][] =
                {
                        { 0, 0, 1, 0, 1, 1 },
                        { 0, 1, 1, 1, 0, 0 },
                        { 0, 0, 1, 1, 1, 1 },
                        { 1, 1, 0, 1, 1, 1 },
                        { 1, 1, 1, 1, 1, 1 },
                        { 1, 1, 0, 1, 1, 1 },
                        { 1, 0, 1, 1, 1, 1 },
                        { 1, 1, 1, 0, 1, 1 }
                };

        System.out.print("The size of largest square sub-matrix of 1's is " +
                findLargestSquare(mat));
    }
}
