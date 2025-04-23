


## CRON Batch jobs

Set up a new or update an existing cron job:

```
kubectl apply -f kubernetes/drivers_stage1.yml
```

View currently scheduled cronjobs

```
kubectl get cronjobs -n eco4cast
```



Trigger a manual run of a CRON job:

```
kubectl create job --from cronjob/gefs-osn-cronjob gefs-osn-cronjob-manual -n eco4cast
```



## Secrets management

Create secrets for the namespace if they do not already exist:

```
kubectl create secret generic github-secrets --from-literal=GITHUB_PAT=<your-github-token> -n eco4cast
kubectl create secret generic osn-secrets --from-literal=OSN_KEY=<your-osn-key> --from-literal=OSN_SECRET=<your-osn-secret> -n eco4cast
```

Edit/view secrets

```
kubectl get secrets -n eco4cast
kubectl edit secret github-secrets -n eco4cast
```
