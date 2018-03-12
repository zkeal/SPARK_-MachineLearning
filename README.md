# SPARK_SVM
a SVM Based on  Hive(UDAF)

##Introduce

This is a UDAF Based on HiveUDF.This function is a distributed implementation of SVM(Support Vector Machine).If you want to get a series of Machine Learing methods in Spark.I'm sure you should use MLlib.(http://spark.apache.org/mllib/).<br>

###SVM:<br>  
https://en.wikipedia.org/wiki/Support_vector_machine  <br>  
![image](https://github.com/zkeal/SPARK_SVM/blob/master/src/ScreenShot/introduce.jpg)
Support Vector Machine in the field of machine learning is a supervised learning model, which is usually used for pattern recognition, classification and regression analysis.It implements classification by building a hyperplane.And SMO is choosed to get the Lagrange parameters(https://en.wikipedia.org/wiki/Sequential_minimal_optimization) in this UDAF.<br>  

Any BUG or advice,contact me freely : zkeal@outlook.com <br>  

##How to use 

1.you can use the sourcecode to compile and build a jar,or use the jar directly.<br>  

2.upload it to your spark and register it as a UDAF.<br>  
![image](https://github.com/zkeal/SPARK_SVM/blob/master/src/ScreenShot/example1.png)

add it in beeline,and create a udf.<br>  

```Bash
add jar /xxx/svm.jar;
CREATE TEMPORARY FUNCTION example_svm AS 'src.TR_Svm';
```
3.the jar includes train part and predict park,use it diffriently .<br>  

```Bash
select example_svm(id,vector,flag,0.1,300,"Linear") from your_data_source_table;
create table train_result as select example_svm(ip,vector,flag,0.1,300,"Linear") as result from your_data_source_table;
```
and you can get a result like this.<br>  
![image](https://github.com/zkeal/SPARK_SVM/blob/master/src/ScreenShot/example2.PNG)

then  use the result to redict<br>  
```Bash
CREATE TEMPORARY FUNCTION predict_svm AS 'src.PredictSvm';
select predict_svm(pre.data,t.result) from your_predict_source pre join(select result from train_result)t;
```
you can get a double value in the End.<br>  

#NB:some explain about the parameter.<br>  
example_svm(id,vector,flag,0.1,300,"Linear") <br>  
id:The primary key for each row of data.<br>  
vector: used for describe the features. vector are required as a string like ->"feature_A:2,feature_B:3:feature_C:4",and resouce table look like below.<br>  

................................................................<br>  
id |                   vector                |   flag           <br>  
1  | feature_A:2,feature_B:3:feature_C:4     |    1             <br>  
2  | feature_A:3,feature_B:2:feature_C:1     |    1             <br>  
3  | feature_A:0,feature_B:-1:feature_C:-1   |   -1             <br>  
................................................................<br>  

0.1: it means tolerence .<br>  
300: it means max counter.<br>  
"Linear": it means kernel .Now it's only support Linear kernel,but I will update it.<br>  



