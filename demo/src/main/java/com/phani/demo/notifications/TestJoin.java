package com.phani.demo.notifications;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class TestJoin {

    private Map<String, SchemaColumnDetail> schema1;

    private Map<String, SchemaColumnDetail> schema2;

    private Map<String, SchemaColumnDetail> outputSchema;

    private Map<Integer, String> outputSchemaOrderToKey;

    private Map<String, DataJoinColumn> inputColumnMapOnOutputName;

    public static void main(String[] args) {




    }



    private void doJoinOnTheData(Map<String, List<List<String>>> leftData, Map<String, List<List<String>>> rightData) {
        List<String[]> outputData = new ArrayList<>();

        if(leftData.size()>0){
            for(Map.Entry<String, List<List<String>>> data:leftData.entrySet())
            {
                List<List<String>> leftList = data.getValue();
                ListIterator<List<String>> rightIter = null;
                if(rightData.containsKey(data.getKey())){
                    rightIter = rightData.get(data.getKey()).listIterator();
                }
                for(List<String> left:leftList){
                    List<String> right = null;
                    if(rightIter.hasNext()){
                        right = rightIter.next();
                    }
                    do {
                        String[] outputRow = new String[outputSchema.size()];
                        for( int i = 0; i < outputRow.length; i++ )
                        {
                            if( i < schema1.size() )
                            {
                                String outputColName = outputSchemaOrderToKey.get(i + 1);
                                DataJoinColumn dataJoinCol = inputColumnMapOnOutputName.get(outputColName);
                                if( dataJoinCol != null )
                                {
                                    outputColName = dataJoinCol.getFirstColumnName();
                                }
                                outputRow[i] = left.get(schema1.get(outputColName).getOrder() - 1);
                            }
                            else if(right!=null)
                            {
                                String outputColName = outputSchemaOrderToKey.get(i + 1);
                                DataJoinColumn dataJoinCol = inputColumnMapOnOutputName.get(outputColName);
                                if( dataJoinCol != null )
                                {
                                    outputColName = dataJoinCol.getSecondColumnName();
                                }

                                outputRow[i] = right.get(schema2.get(outputColName).getOrder() - 1);
                            }
                        }
                        outputData.add(outputRow);
                    }while ( rightIter.hasNext() );
                }
            }
        }else{
            for(Map.Entry<String, List<List<String>>> data:rightData.entrySet()){
                for(List<String> right:data.getValue()){
                    String[] outputRow = new String[outputSchema.size()];
                    for( int i = 0; i < outputRow.length; i++ )
                    {
                        String outputColName = outputSchemaOrderToKey.get(i + 1);
                        DataJoinColumn dataJoinCol = inputColumnMapOnOutputName.get(outputColName);
                        if( dataJoinCol != null )
                        {
                            outputColName = dataJoinCol.getSecondColumnName();
                        }
                        outputRow[i] = right.get(schema2.get(outputColName).getOrder() - 1);
                    }
                    outputData.add(outputRow);
                }
            }
        }


        //write data to output
        if( !outputData.isEmpty() )
        {
            System.out.println(outputData);
            outputData.clear();
        }
    }
}
