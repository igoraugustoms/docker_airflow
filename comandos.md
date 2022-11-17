### Docker
docker build -f Dockerfile -t igoraugustoms91/spark-operator-spotipy:v1 .
docker push igoraugustoms91/spark-operator-spotipy:v1

### Create cluster
eksctl create cluster --name=igor \
--managed \
--instance-types=m5.large \
--spot \
--nodes-min=3 --nodes-max=5 \
--region=us-east-2 \
--alb-ingress-access \
--node-private-networking \
--full-ecr-access \
--nodegroup-name=ng-igor


eksctl scale nodegroup --cluster igor -r us-east-2 --nodes=5 --nodes-min=3 --nodes-max=6 ng-igor

### airflow

kubectl create namespace airflow
helm repo add apache-airflow https://airflow.apache.org
helm show values apache-airflow/airflow > airflow/myvalues3.yaml
mudar executor
mudar variaveis de env
mudar fernet key
mudar defaultUser e CLusterIP para LoadBalancer
redis para falso
mudar gitsync
helm install airflow apache-airflow/airflow -f airflow/myvalues2.yaml -n airflow --debug
kubectl get svc -n airflow3


### spark on k8s
kubectl create namespace processing
kubectl create serviceaccount spark  -n processing
kubectl create clusterrolebinding spark-role-binding --clusterrole=edit --serviceaccount=processing:spark -n processing
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator ou helm repo update
helm install spark spark-operator/spark-operator -n processing
helm ls -n processing
kubectl get pods -n processing
criar o aws-credentials
kubectl apply -f spark-batch-operator-k8s-v1beta2.yaml -n processing
kubectl get pods -n processing --watch
kubectl get sparkapplication -n processing
kubectl describe sparkapplication ref-tracks-features -n processing
kubectl describe id_do_pod
kubectl logs exp-f-artist-driver -n processing


kubectl apply -f daily-job-raw.yaml -n processing
kubectl apply -f historical-job-raw.yaml -n processing

### arquivo spark
Depois de escrever o codigo, subir no bucket aws
criar os secrets kubectl create secret generic aws-credentials --from-literal=aws_access_key_id=XXXXX --from-literal=aws_secret_access_key=XXXXXXXXX -n processing

kubectl create secret generic spotipy-credentials --from-literal=client_id_spotipy=XXXXXXXXXX --from-literal=client_secret_spotipy=xxxxxxxxxxxxx -n processing

### no airflow
Mudar senha
Criar connection do aws e kubernetes
criar variable com key id aws
kubectl apply -f airflow/rolebinding_for_airflow.yaml -n airflow