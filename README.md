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


Setup.

1. Make sure you have a suitable version of Python installed (preferably 3.8 or higher). python --version.
2. pip install pyspark
3. Remove Java JDK from Macbook M1, and install Java JDK 8
    sudo rm -rf /Library/Java/JavaVirtualMachines/jdk-20.jdk
    sudo rm -rf /Library/PreferencePanes/JavaControlPanel.prefPane
    sudo rm -rf /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin
    sudo rm -rf ~/Library/Application\ Support/Oracle/Java
    Download & Install JDK8 https://www.oracle.com/uk/java/technologies/downloads/#java8-mac
    nano ~/.zprofile
    JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
    export JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"
    source ~/.zprofile
4. Create security key for SSH Hadoop
    System preference, Sharing, Check; Remote Login, allow full access for remote users
    To create security key for SSH; 
        ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        chmod 0600 ~/.ssh/id_rsa.pub
        ssh localhost
        CTRL+D to close connection to localhost
5. Download Hadoop
    https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    nano ~/.zprofile
        # Define the Hadoop home directory
        export HADOOP_HOME=/Users/macbookpro/hadoop-3.3.6

        # Essential Hadoop Environment Variables
        export HADOOP_INSTALL=$HADOOP_HOME
        export HADOOP_MAPRED_HOME=$HADOOP_HOME
        export HADOOP_COMMON_HOME=$HADOOP_HOME
        export HADOOP_HDFS_HOME=$HADOOP_HOME
        export YARN_HOME=$HADOOP_HOME

        # Point to native Hadoop library
        export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native

        # Java options for Hadoop
        export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

        # Update PATH to include Hadoop binary and sbin directories
        export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
    source ~/.zprofile

6. sudo code $HADOOP_HOME/etc/hadoop/hadoop-env.sh
    # variable is REQUIRED on ALL platforms except OS X!
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home

7. sudo code $HADOOP_HOME/etc/hadoop/core-site.xml
    <configuration>
        <property>
            <name>hadoop.tmp.dir</name>
            <value>/Users/macbookpro/hdfs/tmp/</value>
            <description>Path to temporary directories.</description>
        </property>
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://127.0.0.1:9000</value>
            <description>The address of the NameNode.</description>
        </property>
    </configuration>

8. sudo code $HADOOP_HOME/etc/hadoop/hdfs-site.xml 
    <configuration>
        <property>
            <name>dfs.namenode.name.dir</name>
            <value>/Users/macbookpro/hdfs/namenode</value>
            <description>Directory where NameNode stores its metadata.</description>
        </property>
        <property>
            <name>dfs.datanode.data.dir</name>
            <value>/Users/macbookpro/hdfs/datanode</value>
            <description>Directory where DataNode stores its data blocks.</description>
        </property>
        <property>
            <name>dfs.replication</name>
            <value>1</value>
            <description>Default block replication.</description>
        </property>
    </configuration>

9. sudo code $HADOOP_HOME/etc/hadoop/mapred-site.xml
    <configuration> 
    <property> 
        <name>mapreduce.framework.name</name> 
        <value>yarn</value> 
        <description>Which MapReduce framework to use.</description>
    </property> 
    </configuration>

10. sudo code $HADOOP_HOME/etc/hadoop/yarn-site.xml
    <configuration>
        <property>
            <name>yarn.nodemanager.aux-services</name>
            <value>mapreduce_shuffle</value>
        </property>
        <property>
            <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
            <value>org.apache.hadoop.mapred.ShuffleHandler</value>
        </property>
        <property>
            <name>yarn.resourcemanager.hostname</name>
            <value>127.0.0.1</value>
        </property>
        <property>
            <name>yarn.acl.enable</name>
            <value>0</value>
        </property>
        <property>
            <name>yarn.nodemanager.env-whitelist</name>   
            <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PERPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
        </property>
    </configuration>
    
11.  hdfs namenode -format

12. start-all.sh
13 use jps to command to check if the Hadoop daemons are running

NameNode Web UI: http://localhost:9870/
ResourceManager Web UI (YARN): http://localhost:8088/
DataNode Web UI: http://localhost:9864/
Secondary NameNode Web UI:http://localhost:9868/

14. Creating users
    hadoop fs -mkdir /geekradius/
    hadoop fs -mkdir /geekradius/etietop

15. Transfer Data From Kaggle
    hadoop fs -put /Users/macbookpro/Downloads/used_cars_data.csv /geekradius/etietop/
