# Scrat Cluster Deployment

Deployment of clusters is handled through [Pegasus](https://github.com/InsightDataScience/pegasus).To deploy clusters exactly as I had for my demonstration run `deploy.sh`. This will create the instances for data producers, kafka, flink, cassandra, and flask-server clusters. Alternatively, you can configure each cluster size, and instance type by editing their respective YAML files.

## Setup
1. Install [Pegasus](https://github.com/InsightDataScience/pegasus), make sure export your AWS credentials to `bash_profile`.

2.  Save your AWS IAM PEM key as `key-pair.pem` in `~/.ssh/`, you can verify your Pegasus installation by using `peg describe`.

3. Create two files in `~/.ssh/`, `sg.txt` and `subnet.txt`, with your AWS security group id, and AWS VPC subnet id.
```Bash
cd ~/.ssh/

touch sg.txt
echo 'sg-XXXXXXXX' > sg.txt

touch subnet.txt
echo 'subnet-XXXXXXXX' > subnet.txt
```

4. Run the deployment script. It will install and start the necessary technologies to your clusters.
```Bash
bash scrat/deployment/deploy.sh
```