package test;

import org.junit.Test;
import src.Apriori.algorithm;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;

public class algorithmTest {

    @Test
    public void get_Maxfrequent() {
        try {
            algorithm test = new algorithm();
            String rresult = test.get_Maxfrequent(get_testdata(),0.5,4);
            System.out.print(rresult);
        }catch (Exception e)
        {
            System.out.print("unpass");
        }
    }

    @Test
    public void get_Maxfrequent1() {
    }

    @Test
    public void get_newStoreList() {
    }

    @Test
    public void find_value() {
    }

    public HashMap<String,ArrayList<String>> get_testdata()
    {
        HashMap<String,ArrayList<String>> test = new HashMap<String, ArrayList<String>>();
        ArrayList<String> value1 = new ArrayList<String>();
        ArrayList<String> value2 = new ArrayList<String>();
        ArrayList<String> value3 = new ArrayList<String>();
        ArrayList<String> value4 = new ArrayList<String>();
        value1.add("1");
        value1.add("3");
        value1.add("4");
        value2.add("2");
        value2.add("3");
        value2.add("5");
        value3.add("1");
        value3.add("2");
        value3.add("3");
        value3.add("5");
        value4.add("2");
        value4.add("5");
        test.put("item1",value1);
        test.put("item2",value2);
        test.put("item3",value3);
        test.put("item4",value4);
        return test;
    }
}