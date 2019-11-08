package com.phani.demo.algorithms.linkedlist;

public class DeleteEveryOtherNodeInCircularLinkedList {


    private static Node rootNode = null;

    public static void main(String[] args) {



        Node n11 = new Node("11");
        Node n10 = new Node("10",n11);
        Node n9 = new Node("9",n10);
        Node n8 = new Node("8",n9);
        Node n7 = new Node("7",n8);
        Node n6 = new Node("6",n7);
        Node n5 = new Node("5",n6);
        Node n4 = new Node("4",n5);
        Node n3 = new Node("3",n4);
        Node n2 = new Node("2",n3);
        Node n1 = new Node("1",n2);
        rootNode  = n1;
        n11.setNextNode(n1);


        deleteAlternativeNodes(n1,0);
        System.out.println(printLinkedList(n1));



    }

    private static String printLinkedList(Node n1) {
        if(n1.getNextNode()==null || n1.equals(rootNode)){
            return n1.getValue();
        }
        return (n1.getValue()+"::"+printLinkedList(n1.getNextNode()));
    }


    /**
     *
     * 1->2->3->4
     * to delete 2 copy value of 3 to two and point Node to 4
     * @param node
     * @param n
     */
    private static void deleteAlternativeNodes(Node node, int n) {
        if(node.equals(rootNode) && n>0){
            return;
        }
        if(n%2==0){
            //System.out.println(node.getValue());
            deleteAlternativeNodes(node.getNextNode(),n+1);
        }else{
            System.out.println(node.getValue());
            if(node==null || node.getNextNode()==null){
                return;
            }
            node.setValue(node.getNextNode().getValue());
            node.setNextNode(node.getNextNode().getNextNode());
            deleteAlternativeNodes(node,n+1);
        }
    }
}
