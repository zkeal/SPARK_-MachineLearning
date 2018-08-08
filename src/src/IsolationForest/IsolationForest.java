package src.IsolationForest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IsolationForest extends AbstractGenericUDAFResolver{
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws org.apache.hadoop.hive.ql.parse.SemanticException
    {
        if(parameters.length<2)
        {
            throw new UDFArgumentTypeException(parameters.length-1,"Exactly two more argument is expected");
        }
        return new Forest();
    }

    public static class Forest extends GenericUDAFEvaluator{
        protected PrimitiveObjectInspector intput_num;
        protected StandardListObjectInspector internalMergeIO;
        protected ObjectInspector output;

        public static algorithm algorithm = new algorithm();

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
        {
            try{
                super.init(m,parameters);
                if(m==Mode.PARTIAL1)
                {
                    intput_num = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(intput_num));
                }
                else{
                    if(parameters[0] instanceof StandardListObjectInspector && m==Mode.PARTIAL2){
                        internalMergeIO = (StandardListObjectInspector)parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(internalMergeIO);
                    }
                    else
                    {
                        output = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                        return output;
                    }
                }
            }catch (Exception e)
            {
                throw new HiveException("init fail");
            }
        }

        public static class DataEntity implements AggregationBuffer{
            ArrayList<Double> data_set;
            int max_count_per_tree;
            double fileter;
            ArrayList<Double> result;

            public DataEntity()
            {
                data_set = new ArrayList <Double>();
                result = new ArrayList <Double>();
                max_count_per_tree=0;
                fileter = 0.2;
            }

            public ArrayList <Double> getResult() {
                return result;
            }

            public void setResult(ArrayList <Double> result) {
                this.result = result;
            }

            public ArrayList <Double> getData_set() {
                return data_set;
            }

            public void setData_set(ArrayList <Double> data_set) {
                this.data_set = data_set;
            }

            public int getMax_count_per_tree() {
                return max_count_per_tree;
            }

            public void setMax_count_per_tree(int max_count_per_tree) {
                this.max_count_per_tree = max_count_per_tree;
            }

            public double getFileter() {
                return fileter;
            }

            public void setFileter(double fileter) {
                this.fileter = fileter;
            }
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            DataEntity dataEntity = new DataEntity();
            return dataEntity;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((DataEntity)aggregationBuffer).data_set.clear();
            ((DataEntity)aggregationBuffer).result.clear();
            ((DataEntity)aggregationBuffer).setMax_count_per_tree(0);
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            try{
                int parameter_size = objects.length-2;
                DataEntity dataEntity = (DataEntity)aggregationBuffer;
                dataEntity.setMax_count_per_tree(parameter_size);
                dataEntity.setFileter(objects[0]==null?0.2:Double.valueOf(objects[0].toString()));
                dataEntity.setMax_count_per_tree(objects[1]==null?0:Integer.getInteger(objects[1].toString()));
                dataEntity.data_set.add(objects[2]==null?0:Double.valueOf(objects[2].toString()));
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            DataEntity dataEntity = (DataEntity)aggregationBuffer;
            ArrayList<Double> data_set = Lists.newArrayList(dataEntity.data_set);
            return data_set;
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            //build ITree in this stage and get isolated parameters to final stage
            DataEntity dataEntity = (DataEntity)aggregationBuffer;
            ArrayList<Double> Entity_list = dataEntity.getData_set();
            if(dataEntity.getData_set().size()>dataEntity.max_count_per_tree)
            {
                dataEntity.setResult(algorithm.calculate_IFtree(dataEntity.getData_set(),dataEntity.getFileter()));
            }
            else
            {
                ArrayList<Double> parameter_List = (ArrayList<Double>)internalMergeIO.getList(o);
                parameter_List.addAll(Entity_list);
                dataEntity.setData_set(parameter_List);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            DataEntity dataEntity = (DataEntity)aggregationBuffer;
            Gson gson = new Gson();
            return  gson.toJson(dataEntity);
        }
    }
}
