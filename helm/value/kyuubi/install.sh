kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
kubectl create clusterrolebinding kyuubi-role --clusterrole=edit --serviceaccount=default:kyuubi --namespace=default