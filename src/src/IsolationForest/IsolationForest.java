package src.IsolationForest;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;

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
            ArrayList<Integer> data_set;
            int max_count_per_tree;
            int hight_limit;
            int parameter_length;

            DataEntity(){
                data_set = new ArrayList<Integer>();
                max_count_per_tree=0;
                hight_limit = 0;
            }

            public int getParameter_length() {
                return parameter_length;
            }

            public void setParameter_length(int parameter_length) {
                this.parameter_length = parameter_length;
            }

            public ArrayList<Integer> getData_set() {
                return data_set;
            }

            public void setData_set(ArrayList<Integer> data_set) {
                this.data_set = data_set;
            }

            public int getMax_count_per_tree() {
                return max_count_per_tree;
            }

            public void setMax_count_per_tree(int max_count_per_tree) {
                this.max_count_per_tree = max_count_per_tree;
            }

            public int getHight_limit() {
                return hight_limit;
            }

            public void setHight_limit(int hight_limit) {
                this.hight_limit = hight_limit;
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
            ((DataEntity)aggregationBuffer).setHight_limit(0);
            ((DataEntity)aggregationBuffer).setMax_count_per_tree(0);
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            try{
                int parameter_size = objects.length-2;
                DataEntity dataEntity = (DataEntity)aggregationBuffer;
                dataEntity.setParameter_length(parameter_size);
                dataEntity.setMax_count_per_tree(objects[0]==null?0:Integer.getInteger(objects[0].toString()));
                dataEntity.setHight_limit(objects[1]==null?0:Integer.getInteger(objects[1].toString()));
                for(int i=2;i<objects.length;i++)
                {
                    dataEntity.data_set.add(objects[i]==null?0:Integer.getInteger(objects[i].toString()));
                }
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            DataEntity dataEntity = (DataEntity)aggregationBuffer;
            if(dataEntity.data_set.size() >dataEntity.getMax_count_per_tree())
            {
                return null;
            }
            else
            {
                return null;
            }
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            return null;
        }
    }
}
