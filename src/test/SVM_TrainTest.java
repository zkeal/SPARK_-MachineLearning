package test;

import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.junit.Test;
import src.SVM_TRAIN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class SVM_TrainTest {
    public static SVM_TRAIN.SvmLine unit_test = new SVM_TRAIN.SvmLine();
    @Test
    public void iterate(){
        Object[] value = {"123.1.1.1","threat_file:6,cactus:4,aegis_crack:6,waf:6,special:6",Integer.valueOf("1"),Double.valueOf("0.2"),Integer.valueOf("100"),"Linear"};
        Map<String,ArrayList> value1 = new HashMap<String, ArrayList>();
        ArrayList ent = new ArrayList();
        ent.add(Double.valueOf(1));
        ent.add(Double.valueOf(6));
        ent.add(Double.valueOf(4));
        ent.add(Double.valueOf(6));
        ent.add(Double.valueOf(6));
        ent.add(Double.valueOf(6));
        value1.put("123123",ent);
        try {
            GenericUDAFEvaluator.AggregationBuffer test = unit_test.getNewAggregationBuffer();
            unit_test.iterate(test,value);
            unit_test.terminatePartial(test);
            unit_test.merge(test,value1);
            unit_test.terminate(test);
        }catch (Exception e)
        {
            e.printStackTrace();
            System.out.print("unpass");
        }
        System.out.print("pass");
    }
}
