package com.phani.demo.algorithms.tree;

import java.util.ArrayDeque;
import java.util.Queue;
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

    void leftViewUtil(Node node, int level)
    {
        int max_level = 0;
        // Base Case
        if (node == null)
            return;

        // If this is the first node of its level
        if (max_level < level) {
            System.out.print(" " + node.key);
            max_level = level;
        }

        // Recur for left and right subtrees
        leftViewUtil(node.left, level + 1);
        leftViewUtil(node.right, level + 1);
    }

    void printLevelOrder(Node root)
    {
        int h = height(root);
        int i;
        for (i=1; i<=h; i++)
            printGivenLevel(root, i);
    }

    int height(Node root)
    {
        if (root == null)
            return 0;
        else
        {
            /* compute  height of each subtree */
            int lheight = height(root.left);
            int rheight = height(root.right);

            /* use the larger one */
            if (lheight > rheight)
                return(lheight+1);
            else return(rheight+1);
        }
    }

    void printGivenLevel (Node root ,int level)
    {
        if (root == null)
            return;
        if (level == 1)
            System.out.println(root.key + " ");
        else if (level > 1)
        {
            printGivenLevel(root.left, level-1);
            printGivenLevel(root.right, level-1);
        }
    }

    public void leftView(Node root)
    {
        if (root == null) {
            return;
        }

        // create an empty queue and enqueue root node
        Queue<Node> queue = new ArrayDeque<>();
        queue.add(root);

        // pointer to store current node
        Node curr;

        // run till queue is not empty
        while (!queue.isEmpty())
        {
            // calculate number of nodes in current level
            int size = queue.size();
            int i = 0;

            // process every node of current level and enqueue their
            // non-empty left and right child to queue
            while (i++ < size) {
                curr = queue.poll();

                // if this is first node of current level, print it
                if (i == 1) {
                    System.out.print(curr.key + " ");
                }

                if (curr.left != null) {
                    queue.add(curr.left);
                }

                if (curr.right != null) {
                    queue.add(curr.right);
                }
            }
        }
    }

    public void rightView(Node root)
    {
        if (root == null) {
            return;
        }

        // create an empty queue and enqueue root node
        Queue<Node> queue = new ArrayDeque<>();
        queue.add(root);

        // pointer to store current node
        Node curr;

        // run till queue is not empty
        while (!queue.isEmpty())
        {
            // calculate number of nodes in current level
            int size = queue.size();
            int i = 0;

            // process every node of current level and enqueue their
            // non-empty left and right child to queue
            while (i++ < size) {
                curr = queue.poll();

                // if this is first node of current level, print it
                if (i == size) {
                    System.out.print(curr.key + " ");
                }

                if (curr.left != null) {
                    queue.add(curr.left);
                }

                if (curr.right != null) {
                    queue.add(curr.right);
                }
            }
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

        System.out.println("");

        tree.leftViewUtil(tree.root, 1);

        System.out.println("");

        tree.printLevelOrder(tree.root);

        System.out.println("");

        tree.leftView(tree.root);

        System.out.println("");

        tree.rightView(tree.root);


    }


}
