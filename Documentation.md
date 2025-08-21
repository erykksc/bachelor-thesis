## Deploy Infrastructure

```bash
# deploy shared infra, like networks
cd ./opentofu/shared/
tofu apply -auto-approve

# deploy load generator VM
# NOTE: make sure to have id_rsa generated on local machine
cd ./opentofu/load-generator/
tofu apply -var "ssh_public_key=$(cat ~/.ssh/id_rsa.pub)" -auto-approve
# Note the resulting IP address for the ssh connection for later

# deploy a k8s cluster of 3 nodes
cd ./opentofu/aks-cluster/
tofu apply -var "node_count=3" -auto-approve
# configure kubectl to use the deployed cluster
az aks get-credentials --resource-group benchmark-rg --name benchmark-aks-cluster
```

Follow the instructions to install cratedb operator
[CrateDB-Docs](https://cratedb.com/docs/guide/install/container/kubernetes/kubernetes-operator.html)

```bash
helm repo add crate-operator https://crate.github.io/crate-operator
kubectl create namespace crate-operator
helm install crate-operator crate-operator/crate-operator --namespace crate-operator --set env.CRATEDB_OPERATOR_DEBUG_VOLUME_STORAGE_CLASS=my-azure-storageclass
```

## Setup the load generator VM

NOTE: wait a bit after deploying the load-generator as the initialization script is being run (5 min should be enough).

```bash
export LOAD_GENERATOR_IP={IP-OBTAINED-FROM-DEPLOYMENT}
# git is being synced in order for the nix to ignore the dataset files when entering nix develop
rsync -avh --progress --exclude 'escooter-trips-generator/.venv' --exclude 'escooter-trips-generator/cache' --exclude 'load-generator/results' \
   ./dataset-generator ./load-generator ./flake.* ./.gitignore ./.git \
   "azureuser@$LOAD_GENERATOR_IP:/mnt/data/ba/"
```

## Run benchmark

```bash
# SSH into the load generator.
cd /mnt/data/ba
nix develop
cd ./load-generator
# db-target is either cratedb|mobilitydbc
# db-conn should be obtained from the kubectl get service
./benchmark.sh \
   --db-target 'cratedb' \
   --db-conn 'postgresql://researcher:bachelorthesis@localhost:5432/doc' \
   --nworkers 4
```
