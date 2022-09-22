### Create cluster
eksctl create cluster --name=igor \
--managed \
--instance-types=m5.large \
--spot \
--nodes-min=2 --nodes-max=4 \
--region=us-east-2 \
--alb-ingress-access \
--node-private-networking \
--full-ecr-access \
--nodegroup-name=ng-igor

### airflow

kubectl create namespace airflow
helm repo add apache-airflow https://airflow.apache.org
helm show values apache-airflow/airflow > airflow/myvalues2.yaml
mudar executor
mudar variaveis de env
mudar fernet key
mudar defaultUser e CLusterIP para LoadBalancer
redis para falso
mudar gitsync
helm install airflow apache-airflow/airflow -f airflow/myvalues2.yaml -n airflow --debug
kubectl get svc -n airflow


### spark on k8s
kubectl create namespace processing
kubectl create serviceaccount spark  -n processing
kubectl create clusterrolebinding spark-role-binding --clusterrole=edit --serviceaccount=processing:spark -n processing
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark spark-operator/spark-operator -n processing
helm ls -n processing
kubectl get pods -n processing


### arquivo spark
Depois de escrever o codigo, subir no bucket aws
criar os secrets kubectl create secret generic aws-credentials --from-literal=aws_access_key_id=MEUKEYID --from-literal=aws_secret_access_key=MEUSECRETACCESS -n processing