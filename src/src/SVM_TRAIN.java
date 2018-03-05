package src;

import Jama.Matrix;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.*;

public class SVM_TRAIN extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException
    {
        if(parameters.length!=6)
        {
            throw new UDFArgumentTypeException(parameters.length-1,"Exactly Six argument is expected");
        }
        return new SvmLine();
    }

    public static class SvmLine extends GenericUDAFEvaluator{
        protected PrimitiveObjectInspector SampleId;
        protected PrimitiveObjectInspector InputFeatures;
        protected StandardMapObjectInspector internalMergeIO;
        protected static ToolInterface Tool = new Svm_Tool();

        @Override
        public ObjectInspector init(Mode m,ObjectInspector[] parameters) throws HiveException
        {
            try{
                super.init(m,parameters);
                if(m==Mode.PARTIAL1)
                {
                    SampleId = (PrimitiveObjectInspector) parameters[0];
                    InputFeatures = (PrimitiveObjectInspector) parameters[1];
                    return ObjectInspectorFactory.getStandardMapObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(SampleId),ObjectInspectorFactory.getStandardListObjectInspector(InputFeatures));
                }
                else{
                    if(parameters[0] instanceof StandardMapObjectInspector && m==Mode.PARTIAL2){
                        internalMergeIO = (StandardMapObjectInspector)parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(internalMergeIO);
                    }
                    else
                    {
                        SampleId = (PrimitiveObjectInspector) parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(SampleId);
                    }
                }
            }catch (Exception e)
            {
                throw new HiveException("init fail");
            }
        }

        public static class EventEntity implements AggregationBuffer
        {
            String SampleId;
            HashMap<String,ArrayList> FeatureMap;
            Double flag;
            Double tolrance;
            int MaxCouter;
            String Kernel;
            public EventEntity()
            {
                this.SampleId="";
                this.FeatureMap=new HashMap<String,ArrayList>();
            }

            public String getSampleId() {
                return SampleId;
            }

            public void setSampleId(String sampleId) {
                SampleId = sampleId;
            }

            public HashMap<String, ArrayList> getFeatureMap() {
                return FeatureMap;
            }

            public void setFeatureMap(HashMap<String, ArrayList> featureMap) {
                FeatureMap = featureMap;
            }

            public Double getFlag() {
                return flag;
            }

            public void setFlag(Double flag) {
                this.flag = flag;
            }

            public Double getTolrance() {
                return tolrance;
            }

            public void setTolrance(Double tolrance) {
                this.tolrance = tolrance;
            }

            public int getMaxCouter() {
                return MaxCouter;
            }

            public void setMaxCouter(int maxCouter) {
                MaxCouter = maxCouter;
            }

            public String getKernel() {
                return Kernel;
            }

            public void setKernel(String kernel) {
                Kernel = kernel;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EventEntity ret = new EventEntity();
            return ret;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((EventEntity)aggregationBuffer).FeatureMap.clear();
            ((EventEntity)aggregationBuffer).SampleId=null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            try {
                if(objects==null || objects.length!=6)
                {
                    return;
                }
                ArrayList<Double> value = new ArrayList<Double>();
                String SampleId = objects[0]==null?"":objects[0].toString();
                Double flag = Double.parseDouble(objects[2]==null?"":objects[2].toString());
                Double tolerance = Double.parseDouble(objects[3]==null?"":objects[3].toString());
                int MaxCounter = Integer.parseInt(objects[4]==null?"":objects[4].toString());
                String kernel = Tool.getKernel(objects[5]==null?"":objects[5].toString());
                EventEntity event = (EventEntity)aggregationBuffer;
                event.setFlag(flag);
                event.setTolrance(tolerance);
                event.setMaxCouter(MaxCounter);
                event.setKernel(kernel);
                event.setSampleId(SampleId);
                event.FeatureMap.put(SampleId,Tool.getparameters(flag,objects[1]));
            }catch (Exception e)
            {
                if(SampleId==null)
                {
                    throw new HiveException("InputK_event failed in init");
                }
                if(InputFeatures==null)
                {
                    throw new HiveException("Inputv_event failed in init");
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
                Map<String,ArrayList> ret = Maps.newHashMap(samples.FeatureMap);
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
                Map<String,ArrayList> sammple = (Map<String,ArrayList>)o;
                Iterator iter = sammple.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Object key = entry.getKey();
                    ArrayList<Double> value = (ArrayList<Double>) entry.getValue();
                    if(eventEntity.FeatureMap.containsKey(key))
                    {
                        ArrayList<Double> Map_value = eventEntity.getFeatureMap().get(key);
                        int count=0;
                        for(Double temp: value)
                        {
                            Double aggre_D=Map_value.get(count);
                            Map_value.set(count,aggre_D+temp);
                        }
                    }
                    else
                    {
                        eventEntity.FeatureMap.put(key.toString(),value);
                    }
                }
            }catch (Exception e)
            {
                throw new HiveFatalException("terminatePartial"+e.getMessage());
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            EventEntity data_set = (EventEntity)aggregationBuffer;
            int demension = Tool.get_Arraysize(data_set.getFeatureMap())-1;
            if(demension>0)
            {
                Matrix train_data = Tool.getTrain_data(demension,data_set.getFeatureMap());
                Matrix label = Tool.getLabel(data_set.getFeatureMap());
                Equations result = Tool.SMO(Tool.getinit_matrix(data_set.getFeatureMap().size()),label,train_data,data_set.tolrance,data_set.MaxCouter);
                return result.toString();
            }
            return null;
        }
    }

    public static void main(String[] args)
    {

    }

}
