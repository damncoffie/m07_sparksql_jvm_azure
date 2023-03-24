* Navigate to terraform directory and deploy infrastructure with:
```
terraform init
terraform plan 
terraform apply
```
* Create next folders in Azure Storage account's `data` container:
    * expedia
    * hotel-months-count
    * hotel-weather
    * hotel-weather-and-trend
    * hotel-weather-temp
* Open Azure Portal, navigate to "All resources" tab and select Azure Databricks Service
* In opened Databricks UI create new Notebook and import .dbc archive from `notebooks/export` directory
* Create new cluster, fill empty strings with your properties in cmd 2 and run notebook
* Check output and remove infrastructure with:
```
terraform destroy
```

<br/>
<br/>
Screenshots with query results and data container are placed in `notebooks/screenshots` directory.
<br/>
To check out output in your browser just open `.html` file `notebooks/export` 