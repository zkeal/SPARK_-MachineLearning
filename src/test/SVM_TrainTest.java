package test;

import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.junit.Test;
import src.TR_Svm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static test.Svm_ToolTest.get_test_train_data;

public class SVM_TrainTest {
    public  TR_Svm.SvmLine unit_test = new TR_Svm.SvmLine();
    @Test
    public void iterate(){
        Object[] value = {"123.1.1.1","threat_file:6,cactus:4,aegis_crack:6,waf:6,special:6",Integer.valueOf("1"),Double.valueOf("0.2"),Integer.valueOf("100"),"Linear"};
        HashMap<String,ArrayList> value1 =  get_test_train_data(100,5);
        try {
            GenericUDAFEvaluator.AggregationBuffer test = unit_test.getNewAggregationBuffer();
            TR_Svm.SvmLine.EventEntity ttt =(TR_Svm.SvmLine.EventEntity)test;
            ttt.setFeatureMap(value1);
            unit_test.iterate(test,value);
            unit_test.terminatePartial(test);
            unit_test.merge(test,value1);
            unit_test.terminate(ttt);
        }catch (Exception e)
        {
            e.printStackTrace();
            System.out.print("unpass");
        }
        System.out.print("pass");
    }
}


