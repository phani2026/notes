package com.phani.demo.algorithms;

import java.util.*;

public class SortStringArrayWithDynamicOrder {

    public static void main(String[] args) {

        SortStringArrayWithDynamicOrder order = new SortStringArrayWithDynamicOrder();

        List<String> input = Arrays.asList("ab", "bi","ai","ah","h","i","a");
        order.sort(input);

    }



    private List<String> sort(List<String> input){


        Collections.sort(input,new CustomStringComparator("baehoi"));
        System.out.println(input);
        return input;
    }

    public class CustomStringComparator implements Comparator<String>
    {
        private Map<String,Integer> charValMap = new HashMap();

        public CustomStringComparator(String orderString) {
            //Add characters to map
            int i = 1;
            for(char c:orderString.toCharArray())
            {
                charValMap.put(c+"",i++);
            }
            System.out.println(charValMap);
        }

        private Integer getValue(char a){
            return charValMap.getOrDefault(a+"",charValMap.size()+2);
        }

        @Override
        public int compare( String o1, String o2 )
        {
            char[] a1 = o1.toCharArray();
            char[] a2 = o2.toCharArray();
            int length = Math.min(o1.length(), o2.length());
            for(int i=0; i<length; i++)
            {
                Integer val1 = getValue(a1[i]);
                Integer val2 = getValue(a2[i]);
                int result = val1.compareTo(val2);
                if(result==0){
                    continue;
                }else{
                    return result;
                }
            }
            return 0;
        }
    }
}
