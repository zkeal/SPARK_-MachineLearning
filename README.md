# SPARK_SVM
a SVM Based on  Hive(UDAF)

Introduce
This is a UDAF Based on HiveUDF.This function is a distributed implementation of SVM(Support Vector Machine).If you want to get a series of Machine Learing methods in Spark.I'm sure you should use MLlib.(http://spark.apache.org/mllib/).

SVM:
https://en.wikipedia.org/wiki/Support_vector_machine

Support Vector Machine in the field of machine learning is a supervised learning model, which is usually used for pattern recognition, classification and regression analysis.It implements classification by building a hyperplane.And SMO is choosed to get the Lagrange parameters(https://en.wikipedia.org/wiki/Sequential_minimal_optimization) in this UDAF.

Any BUG or advice,contact me freely : zkeal@outlook.com

How to use 
1.you can use the sourcecode to compile and build a jar,or use the jar directly.

2.upload it to your spark and register it as a UDAF.

3.the jar includes train part and predict park,use it diffriently.
