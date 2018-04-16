package src.NV_exp;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.*;

public class Anomaly extends AbstractGenericUDAFResolver{
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws org.apache.hadoop.hive.ql.parse.SemanticException
    {
        if(parameters.length<2)
        {
            throw new UDFArgumentTypeException(parameters.length-1,"MIN two argument is expected");
        }
        return new anomaly();
    }

    public static class anomaly extends GenericUDAFEvaluator{
        protected PrimitiveObjectInspector sampleid;
        protected StandardListObjectInspector internalMergeIO;
        protected ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
        {
            try{
                super.init(m,parameters);
                if(m==Mode.PARTIAL1)
                {
                    sampleid = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(sampleid));
                }
                else{
                    if(parameters[0] instanceof StandardListObjectInspector && m==Mode.PARTIAL2){
                        internalMergeIO = (StandardListObjectInspector)parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(internalMergeIO);
                    }
                    else
                    {
                        outputOI = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                        return outputOI;
                    }
                }
            }catch (Exception e)
            {
                throw new HiveException("init fail");
            }
        }

        public static class EventEntity implements AggregationBuffer{
            public int line_size;
            ArrayList<String> data_set;
            String Flag;

            public EventEntity(){
                line_size=0;
                data_set = new ArrayList<String>();
            }

            public int getLine_size() {
                return line_size;
            }

            public void setLine_size(int line_size) {
                this.line_size = line_size;
            }


            public ArrayList<String> getData_set() {
                return data_set;
            }

            public void setData_set(ArrayList<String> data_set) {
                this.data_set = data_set;
            }

            public String getFlag() {
                return Flag;
            }

            public void setFlag(String flag) {
                Flag = flag;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EventEntity ret = new EventEntity();
            return ret;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((EventEntity)aggregationBuffer).data_set.clear();
            ((EventEntity)aggregationBuffer).line_size=0;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            try {
                int Lin_size = objects.length-1;
                String FLAG = objects[0]==null?"":objects[0].toString();
                EventEntity eventEntity = (EventEntity)aggregationBuffer;
                eventEntity.setLine_size(Lin_size);
                eventEntity.setFlag(FLAG);
                for(int i=1;i<objects.length;i++)
                {
                    String temp = objects[i]==null?"0.0":objects[0].toString();
                    eventEntity.data_set.add(temp);
                }
            }catch (Exception e)
            {
                if(sampleid==null)
                {
                    throw new HiveException("sampleid failed in init");
                }
                if(internalMergeIO==null)
                {
                    throw new HiveException("internalMergeIO failed in init");
                }
                else
                {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            try{
                EventEntity samples = (EventEntity) aggregationBuffer;
                ArrayList<String> ret = Lists.newArrayList(samples.data_set);
                return ret;
            }catch (Exception e)
            {
                throw new HiveException("terminatePartial"+e.getMessage());
            }
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            try {
                EventEntity eventEntity = (EventEntity) aggregationBuffer;
                List<String> sammple = (List<String>) internalMergeIO.getList(o);
                for(String temp:sammple)
                {
                    eventEntity.data_set.add(temp);
                }
            }catch (Exception e)
            {
                throw new HiveException("merge"+e.getMessage());
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            StringBuffer SB_RE = new StringBuffer();
            EventEntity entity = (EventEntity)aggregationBuffer;
            ArrayList<Double> result= getDoubleList(entity.data_set);
            int line_size = entity.getLine_size();
            String FLAG = entity.getFlag();
            double[] value_array =new double[line_size+1];
            Double temp_0 = 1.0;
            Double sum_value = 0.0;
            int index=0;
            while (index<entity.data_set.size())
            {
                int iner_index = index%(line_size+1);
                if(iner_index==0)
                {
                    if(index!=0)
                    {
                        value_array[0]=temp_0+value_array[0];
                    }
                    temp_0=result.get(index);
                }
                if(iner_index!=0)
                {
                    value_array[iner_index] = result.get(index)*result.get(index)+value_array[iner_index];
                    temp_0=temp_0*result.get(index);
                }
                sum_value=sum_value+result.get(index);
                index++;
            }
            Double Average = sum_value/entity.data_set.size();
            Double Exp=1.0;
            for(int i=1;i<value_array.length;i++)
            {
                Exp=Math.sqrt(value_array[i])*Exp;
            }
            if(FLAG.equals("up"))
            {
                return SB_RE.append(Average+3*value_array[0]/Exp).toString();
            }
            else {
                return SB_RE.append(Average-3*value_array[0]/Exp).toString();
            }
        }

        public ArrayList<Double> getDoubleList(ArrayList<String> stringlist)
        {
            ArrayList<Double> result =new ArrayList<Double>();
            for(String temp:stringlist)
            {
                result.add(Double.valueOf(temp));
            }
            return result;
        }


        public static void main(String[] args)
        {
            StringBuffer SB_RE = new StringBuffer();
            EventEntity entity = new EventEntity();
            entity.setFlag("sdsd");
            entity.setLine_size(1);
            entity.data_set.add("2");
            entity.data_set.add("428");
            entity.data_set.add("19355");
            entity.data_set.add("153");
            entity.data_set.add("121");
            ArrayList<Double> result =new ArrayList<Double>();
            for(String temp:entity.data_set)
            {
                result.add(Double.valueOf(temp));
            }
            int line_size = entity.getLine_size();
            String FLAG = entity.getFlag();
            double[] value_array =new double[line_size+1];
            Double temp_0 = 1.0;
            Double sum_value = 0.0;
            int index=0;
            while (index<entity.data_set.size())
            {
                int iner_index = index%(line_size+1);
                if(iner_index==0)
                {
                    if(index!=0)
                    {
                        value_array[0]=temp_0+value_array[0];
                    }
                    temp_0=result.get(index);
                }
                if(iner_index!=0)
                {
                    value_array[iner_index] = result.get(index)*result.get(index)+value_array[iner_index];
                    temp_0=temp_0*result.get(index);
                }
                sum_value=sum_value+result.get(index);
                index++;
            }
            Double Average = sum_value/entity.data_set.size();
            Double Exp=1.0;
            for(int i=1;i<value_array.length;i++)
            {
                Exp=Math.sqrt(value_array[i])*Exp;
            }
            if(FLAG.equals("up"))
            {
                System.out.print(SB_RE.append(Average+3*value_array[0]/Exp).toString());
            }
            else {
                System.out.print(SB_RE.append(Average-2*value_array[0]/Exp).toString());
            }
        }
    }

}
