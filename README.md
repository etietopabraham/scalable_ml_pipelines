# scalable_ml_pipelines
Advanced BD ITMO University Project Work

A system that trains a machine learning model and serves incoming request making predictions for them using the trained model.

Dataset: https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset

Requirements:

1. Spark ML should be used to implement training ETL and model training.
2. Spark Streaming should be used to implement serving application.
3. Live Metrics should be calculated using Spark Streaming.
4. All services and applications should be deployed in Kubernetes.
5. Use kuberenetes cron to implement periodical checking for new data upload and new model events.
6. Cross validation has to be used for ensemble models.

It is assumed that there is a source that periodically dumps a new batch of train data to hdfs. The system should detect apperance of such a batch and trigger a training workflow. The training workflow consists of (1) ETL part that cleans data and prepares features; (2) enemble part that trains two or more ML models (let’s say gradient boosting, SVM, linear/logistic regression); (3) stacking part that trains an ML Model (linear/logistic regression) sitting on top of the ensemble and gives final predictions. Upon finishing the training and cross-validation of the model, the train set is exported to HDFS. The trained model should be registered with MLFlow Tracking service (as an artifact). The quality metrics (both for the ensemble models and the stacking model), training and validations times, models parameters (for all models) should all be logged to MLFlow Tracking service too.

There is also a model serving application that works constantly (even when the new model training is in progress) starting since training of the very first model has been completed. This application listens to incoming data from Kafka. The app filters incoming records and makes all required preparation to represent a record in a suitable form for the model, than the model is applied to each record and resulting predictions is written to Kafka. It is known, that except requests for predictions there may be incoming messages that contains eventual price set by manager for records that predictions were generated earlier. This fact makes it possible to compute live metrics for the serving application (for a time limited window).

The serving application should be redeployed to serve with a new model each time the new model registered in MLFlow Tracking service. However, there shouldn’t be downtime for the serving app even during redeployment.

Notes:

1. To make parallel training of ensemble models you may use either Spark parallel capabilities (FIFO scheduler, submitting parallel jobs from multiple threads, etc.) or Apache Airflow (for intermediate data use HDFS in the second case).
2. Take a dataset from kaggle https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset
3. Split the dataset on train and test parts. The test part should be used for sending to the inference application.
4. Simulate periodical appearance of new data by splitting the train dataset on many subparts.
5. Take a look on ‘kubectl rollout restart’ command to make redeployment without downtime.

## Local Setup

### Prerequisites

Ensure you have a suitable version of Python installed (preferably 3.8 or higher). 

```bash
python --version
````

Setting up HDFS

1. Installing Java JDK 8
If you're using a Macbook M1, remove any existing JDK:

```
sudo rm -rf /Library/Java/JavaVirtualMachines/jdk-20.jdk
sudo rm -rf /Library/PreferencePanes/JavaControlPanel.prefPane
sudo rm -rf /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin
sudo rm -rf ~/Library/Application\ Support/Oracle/Java
```

Download & Install JDK8 from Oracle's website.https://www.oracle.com/uk/java/technologies/downloads/#java8-mac

Update your shell profile:

```
nano ~/.zprofile
```

Add the following lines:

```
JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
export JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
```

Reload the profile:

```
source ~/.zprofile
```

2. Setting Up SSH (Required for Hadoop)
Enable remote login:

Go to System Preferences > Sharing.
Check "Remote Login" and allow full access for remote users.
Create a security key for SSH:

```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/id_rsa.pub
ssh localhost
```

Press CTRL+D to close the connection to localhost.

3. Installing Hadoop

Download Hadoop. https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

Update your shell profile:

```
nano ~/.zprofile
```

... (Your existing Hadoop environment variables setup remains here) ...

Reload the profile:

```
source ~/.zprofile
```

... (Your existing Hadoop configuration setup remains here) ...

4. Starting Hadoop Services
Format the Hadoop filesystem:

```
hdfs namenode -format
```

Start all Hadoop services:

```
start-all.sh
```

Use jps command to check if the Hadoop daemons are running.

Web UIs:

NameNode: http://localhost:9870/
ResourceManager (YARN): http://localhost:8088/
DataNode: http://localhost:9864/
Secondary NameNode: http://localhost:9868/

5. Creating User Directories in HDFS

```
hadoop fs -mkdir /geekradius/
hadoop fs -mkdir /geekradius/etietop
```

6. Transferring Data From Kaggle to HDFS

```
hadoop fs -put /Users/macbookpro/Downloads/used_cars_data.csv /geekradius/etietop/
```

### Setting up Spark



