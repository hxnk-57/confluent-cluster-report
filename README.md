# Confluent Cluster Report

This script generates a metric report using the Confluent Cloud Metrics API.
The report includes active connections and partitions, providing more visibiility into utilization.


|**Metric**|**Limit**|**What happens when the limit is reached?**|
|---|:---|:---|
|Active Connections|18000 per CKU |Soft Limit. Potential Performance Degregagation|
|Partitions|4500 per CKU| Hard Limit. No new topics can be created once the limit is reached|
| Schemas| 1000 for the Essentials Package| Under the current commit we are still entitled to 1000 free schemas


## Getting Started

### Setup Instructions
1. Clone the repository and navigate to the project directory.
```bash
git clone https://github.com/hxnk-57/confluent-cluster-report.git
cd confluent-cluster-report
```

2. Create a Virtual Environment:
```
python -m venv <virtual-environment-name>
```

3. Activate the Virtual Environment:
```
source virtual-environment-name/bin/activate
```

4. Install the required dependencies:
```
python -m pip install -r requirements.txt
```

### Configuration

1. Confluent Service Account
    - Create a Confluent Cloud service account or use the existing `svc_vg_cluster_report` service account.
    - Assign the MetricsViewer role to the service account.

1. Generate an API Key
    - Create a Cloud API key owned by the service account.
    - Base64 encode the `API_KEY:SECRET` pair (e.g., `echo -n "API_KEY:SECRET" | base64`)

1. Prepare Configuration files
    - Rename `.env.example` to `.env` and update it with the Base64-encoded credentials.
    - Rename `clusters.json.example` to `clusters.json` and populate it with cluster details in the following format:
    ```json
        "cluster_alias" : {
            "id": "cluster_id",
            "CKU" : 1    
        } 
    ```
    - Finally, Rename `schema_registries.json.example` to `schema_registries.json` and populate it with the schema registry details in the following format:

    ```json
        "EnvironmentName" : {
            "id" : "lsrc-00000",
        }
    ```

## Running the Script
1. Generate the Report

```
python cluster-report.py
```
The script will fetch metrics and generate a PDF report.
