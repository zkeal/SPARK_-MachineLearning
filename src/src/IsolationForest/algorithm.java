package src.IsolationForest;

import apple.laf.JRSUIUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.*;

public class algorithm {
    protected static int sum = 0;

    public ArrayList<Double> calculate_IFtree(ArrayList<Double> data_list,double filter) throws HiveException
    {
        //select a random attribute
        int size = data_list.size();
        if (size!=0)
        {
            Tree_node root = new Tree_node(0);
            root = random_split(data_list,root,0);
            return Isolate_value(root,size,filter);
        }
        return null;
    }



    public Tree_node random_split(ArrayList<Double> data_tree,Tree_node tree_node,int tree_high) throws HiveException
    {
        try {
            sum = sum +tree_high;//convinent for get E(h(x))
            if(data_tree.size()>1)
            {
                ArrayList<Double> left_tree = new ArrayList <Double>();
                ArrayList<Double> right_tree = new ArrayList <Double>();
                double min = data_tree.get(0);
                double max = data_tree.get(data_tree.size()-1);
                if(max!=min)
                {
                    double result = Math.random();
                    int Int_Index = (int)(result*(data_tree.size()));
                    for(Double temp_node:data_tree)
                    {
                        if(temp_node<data_tree.get(Int_Index))
                        {
                            left_tree.add(temp_node);
                        }
                        else
                        {
                            right_tree.add(temp_node);
                        }
                    }
                    tree_node.setValue(data_tree.get(Int_Index));
                    data_tree.clear();
                    if(left_tree.size() == right_tree.size())
                    {
                        return tree_node;
                    }
                    tree_node.left_tree = new Tree_node(tree_high+1);
                    tree_node.right_tree = new Tree_node(tree_high+1);
                    tree_node.left_tree= random_split(left_tree,tree_node.left_tree,tree_high+1);
                    tree_node.right_tree = random_split(right_tree,tree_node.right_tree,tree_high+1);
                }
            }
            return tree_node;
        }catch (Exception e)
        {
            throw  new HiveException("calculate_IFtree failed");
        }
    }

//      public int get_SumHight(Tree_node root)
//      {
//          int sum=-1;
//          Tree_node temp = root;
//          Stack<Tree_node> collection = new Stack <Tree_node>();
//          while (temp!=null || collection.size()!=0)
//          {
//             if(sum==-1)
//             {
//                 sum=0;
//                 collection.push(temp.left_tree);
//                 collection.push(temp.right_tree);
//                 continue;
//             }else
//             {
//                 temp = collection.pop();
//             }
//             if(temp==null)
//             {
//                 continue;
//             }
//             sum = sum + temp.getHigh();
//             collection.push(temp.left_tree);
//             collection.push(temp.right_tree);
//          }
//          return sum;
//      }

    public ArrayList<Double> Isolate_value(Tree_node root,int max_count,double Dividing)
    {
        int count_now  = 0;
        double temp_sum = 0;
        ArrayList<Double> result_list = new ArrayList <Double>();
        Tree_node temp_node = root;
        Stack<Tree_node> stack = new Stack <Tree_node>();
        while (temp_node.value !=null || stack.size()!=0)
        {
            if(temp_node.value==null)
            {
                temp_node=stack.pop();
                continue;
            }
            if(temp_node.left_tree!=null)
            {
                stack.push(temp_node.left_tree);
            }
            if(temp_node.right_tree!=null)
            {
                stack.push(temp_node.right_tree);
            }
            count_now++;
            temp_sum = temp_sum+ temp_node.getValue();
            double temp_avg = temp_sum/count_now;
            double result  = Math.pow(2,-temp_avg/get_EulerConstant(count_now,max_count));
            if(result>Dividing && result < 1-Dividing)
            {
                result_list.add(temp_node.getValue());
            }
            temp_node=stack.pop();
        }
        return result_list;
    }

    public double get_EulerConstant(int index,int max_count)
    {
        if(index<2)
        {
            return 1;
        }else if(index==2)
        {
            return 1;
        }
        else {
            return 2*Math.log(index-1)-2*(index-1)/max_count;
        }
    }

    public static class Tree_node
    {
        int high;
        Double value;
        Tree_node right_tree;
        Tree_node left_tree;

        public Tree_node()
        {
            high=0;
            right_tree = null;
            left_tree = null;
        }

        public Tree_node(Tree_node data_node)
        {
            this.high = data_node.high;
        }

        public Tree_node(int high)
        {
            this.high = high;
        }

        public int getHigh() {
            return high;
        }

        public void setHigh(int high) {
            this.high = high;
        }

        public Tree_node getRight_tree() {
            return right_tree;
        }

        public void setRight_tree(Tree_node right_tree) {
            this.right_tree = right_tree;
        }

        public Tree_node getLeft_tree() {
            return left_tree;
        }

        public void setLeft_tree(Tree_node left_tree) {
            this.left_tree = left_tree;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }
    }

}
