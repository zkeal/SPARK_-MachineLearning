package test;

import Jama.Matrix;
import org.junit.Test;

import src.Equations;
import src.Svm_Tool;
import src.ToolInterface;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.*;

public class Svm_ToolTest {
    static ToolInterface unit_test = new Svm_Tool();
    static HashMap<String,ArrayList> train_data =  get_test_train_data(100,5);

    @Test
    public void getparameters() throws Exception {
        Object test = "threat:6,cccc:4,crack:6,waf:6,special:6";
        assertEquals(unit_test.getparameters(1,test),get_arry());
        System.out.print("pass");
    }

    @Test
    public void getKernel() throws Exception {
    }

    @Test
    public void getinit_matrix() throws Exception {

    }

    @Test
    public void getDemension() throws Exception {
        HashMap<String,ArrayList> test = new HashMap<String, ArrayList>();
        test.put("10.1.1.1",get_arry());
        assertEquals(unit_test.getDemension(test),5);
        System.out.print("pass");
    }

    @Test
    public void SMO() throws Exception {
        Matrix trai_data = unit_test.getTrain_data(unit_test.getDemension(train_data),train_data);
        Matrix label = unit_test.getLabel(train_data);
        Matrix aerph = unit_test.getinit_matrix(unit_test.getDemension(train_data));
        Equations result = unit_test.SMO(aerph,label,trai_data,0.1,1000);
        System.out.print(result.toString());
    }

    @Test
    public void getLabel() throws Exception {
        Matrix label =unit_test.getLabel(train_data);
        for(int i=0;i<label.getRowDimension();i++)
        {
            if(label.get(i,0)==1.0 ||label.get(i,0)==-1.0)
            {
                continue;
            }else {
                System.out.print("unpass");
            }
        }
        System.out.print("pass");
    }

    @Test
    public void getTrain_data() throws Exception {

    }

    @Test
    public void get_inner_product() throws Exception {
    }

    @Test
    public void getRandom_index() throws Exception {
    }

    @Test
    public void getEi() throws Exception {
    }

    public static ArrayList<Double> get_arry()
    {
        ArrayList<Double> test_result = new ArrayList<Double>();
        test_result.add(1.0);
        test_result.add(6.0);
        test_result.add(4.0);
        test_result.add(6.0);
        test_result.add(6.0);
        test_result.add(6.0);
        return test_result;
    }

    //get random of train data
    public static HashMap<String,ArrayList>  get_test_train_data(int row,int col){
        HashMap<String,ArrayList> test = new HashMap<String, ArrayList>();
        for(int i=0;i<row;i++)
        {
            Random seed  = new Random();
            ArrayList<Double> temp = new ArrayList<Double>();
            double value = seed.nextDouble()*5;
            if(i<row/2)
            {
                temp.add(1.0);
                value = value+5;
            }else {
                temp.add(-1.0);
            }
            for(int j=1;j<col+1;j++)
            {
                temp.add(value-2);
            }
            test.put(i+"",temp);
        }
        return test;
    }
}
