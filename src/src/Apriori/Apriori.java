package src.Apriori;

import com.google.common.collect.Maps;
import groovyjarjarantlr.SemanticException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Apriori extends AbstractGenericUDAFResolver{
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws org.apache.hadoop.hive.ql.parse.SemanticException
    {
        if(parameters.length!=3)
        {
            throw new UDFArgumentTypeException(parameters.length-1,"Exactly Three argument is expected");
        }
        return new apriori();
    }

    public static class apriori extends GenericUDAFEvaluator{
        protected PrimitiveObjectInspector sampleid;
        protected StandardMapObjectInspector internalMergeIO;
        public static algorithm algorithm_excutor = new algorithm();

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException
        {
            try{
                super.init(m,parameters);
                if(m==Mode.PARTIAL1)
                {
                    sampleid = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardMapObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(sampleid),ObjectInspectorFactory.getStandardListObjectInspector(sampleid));
                }
                else{
                    if(parameters[0] instanceof StandardMapObjectInspector && m==Mode.PARTIAL2){
                        internalMergeIO = (StandardMapObjectInspector)parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(internalMergeIO);
                    }
                    else
                    {
                        sampleid = (PrimitiveObjectInspector) parameters[0];
                        return ObjectInspectorUtils.getStandardObjectInspector(sampleid);
                    }
                }
            }catch (Exception e)
            {
                throw new HiveException("init fail");
            }
        }

        public static class EventEntity implements AggregationBuffer{
            String ID;
            HashMap<String,ArrayList<String>> record;
            Double min;

            public EventEntity()
            {
                this.ID = "";
                this.record = new HashMap<String, ArrayList<String>>();
            }

            public String getID() {
                return ID;
            }

            public void setID(String ID) {
                this.ID = ID;
            }

            public HashMap<String, ArrayList<String>> getRecord() {
                return record;
            }

            public void setRecord(HashMap<String, ArrayList<String>> record) {
                this.record = record;
            }

            public Double getMin() {
                return min;
            }

            public void setMin(Double min) {
                this.min = min;
            }

            public void setMin(String min)
            {
                this.min = Double.valueOf(min);
            }

            public void setRecord(String ID,String values)
            {
                if(ID.isEmpty() || values.isEmpty())
                {
                    return;
                }
                ArrayList<String> value = new ArrayList<String>();
                if(values.contains(","))
                {
                    for(String temp:values.split(","))
                    {
                        value.add(temp);
                    }
                    this.record.put(ID,value);
                }else {
                    value.add(values);
                    this.record.put(ID,value);
                }
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EventEntity ret = new EventEntity();
            return ret;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            ((EventEntity)aggregationBuffer).record.clear();
            ((EventEntity)aggregationBuffer).ID=null;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            try {
                if(objects==null || objects.length!=2)
                {
                    return;
                }
                ArrayList<String> value = new ArrayList<String>();
                String user_name = objects[0]==null?"":objects[0].toString();
                String values = objects[1]==null?"":objects[1].toString();
                String min = objects[2]==null?"":objects[2].toString();
                EventEntity eventEntity = (EventEntity)aggregationBuffer;
                eventEntity.setID(user_name);
                eventEntity.setRecord(user_name,values);
                eventEntity.setMin(min);
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
                HashMap<String,ArrayList<String>> ret = Maps.newHashMap(samples.record);
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
                Map<String,ArrayList<String>> sammple = (Map<String,ArrayList<String>>)internalMergeIO.getMap(o);
                //Map<String,ArrayList> sammple = (Map<String,ArrayList>)o;
                Iterator iter = sammple.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Object key = entry.getKey();
                    ArrayList<String> value = (ArrayList<String>) entry.getValue();
                    eventEntity.record.put(key.toString(),value);
                }
            }catch (Exception e)
            {
                throw new HiveException("merge"+e.getMessage());
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            EventEntity data_set = (EventEntity)aggregationBuffer;
            String result = algorithm_excutor.get_Maxfrequent(data_set.record,data_set.min,data_set.record.size());
            return result;
        }
    }

}
