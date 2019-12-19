package com.phani.demo.algorithms.tree;

import java.util.Stack;

/**
 *
 * Traversal using recursive and iterative
 */

public class Traversal {


    Node root;

    Traversal()
    {
        root = null;
    }

    void printPreOrderRec(Node node){
        if (node == null)
            return;

        System.out.print(node.key+ " ");
        printPostOrderRec(node.left);
        printPostOrderRec(node.right);

    }

    void printPreOrderIter(Node node){
        if (node == null)
            return;

        Stack<Node> nodeStack = new Stack<Node>();
        nodeStack.push(node);

        while (!nodeStack.isEmpty()){
            Node currentNode = nodeStack.peek();
            System.out.print(currentNode.key + " ");
            nodeStack.pop();



            if(currentNode.right !=null)
                nodeStack.push(currentNode.right);

            if(currentNode.left !=null)
                nodeStack.push(currentNode.left);


        }

    }

    void printInOrderRec(Node node){
        if (node == null)
            return;

        printPostOrderRec(node.left);
        System.out.print(node.key+ " ");
        printPostOrderRec(node.right);


    }

    void printInOrderIter(Node node){
        if (node == null)
            return;

        Stack<Node> nodeStack = new Stack<Node>();
        Node curr = node;

        while (curr != null || nodeStack.size() > 0)
        {

            /* Reach the left most Node of the curr Node */
            while (curr !=  null)
            {
                nodeStack.push(curr);
                curr = curr.left;
            }

            /* Current must be NULL at this point */
            curr = nodeStack.pop();

            System.out.print(curr.key + " ");

            /* we have visited the node and its
               left subtree.  Now, it's right
               subtree's turn */
            curr = curr.right;
        }

    }

    void printPostOrderRec(Node node){
        if (node == null)
            return;

        printPostOrderRec(node.left);
        printPostOrderRec(node.right);
        System.out.print(node.key+ " ");

    }

    void printPostOrderIter(Node node){
        if (node == null)
            return;

        // Create two stacks
        Stack<Node> s1 = new Stack<>();
        Stack<Node> s2 = new Stack<>();

        if (root == null)
            return;

        // push root to first stack
        s1.push(root);

        // Run while first stack is not empty
        while (!s1.isEmpty()) {
            // Pop an item from s1 and push it to s2
            Node temp = s1.pop();
            s2.push(temp);

            // Push left and right children of
            // removed item to s1
            if (temp.left != null)
                s1.push(temp.left);
            if (temp.right != null)
                s1.push(temp.right);
        }

        // Print all elements of second stack
        while (!s2.isEmpty()) {
            Node temp = s2.pop();
            System.out.print(temp.key + " ");
        }

    }



    public static void main(String[] args) {
        Traversal tree = new Traversal();
        tree.root = new Node(1);
        tree.root.left = new Node(2);
        tree.root.right = new Node(3);
        tree.root.left.left = new Node(4);
        tree.root.left.right = new Node(5);

        tree.printPostOrderIter(tree.root);
        System.out.println("");
        tree.printPostOrderRec(tree.root);
    }


}
