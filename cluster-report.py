from datetime import datetime, timedelta
from dotenv import load_dotenv
import http.client
import json
import logging
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os
import pytz
import time

load_dotenv()

HEADERS = { 'Authorization': f"Basic {os.getenv('HEADER')}" }

with open('clusters.json', 'r') as f:
    clusters = json.load(f)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if not [HEADERS['Authorization']]:
    logging.error("Please configure the `HEADER` environmental variable.")
    exit(1)


def generate_time_interval(days: int) -> str:
    tz = pytz.timezone('Europe/Belgrade')
    end_time = datetime.now(pytz.utc).astimezone(tz)
    start_time = (end_time - timedelta(days=days)).astimezone(tz)
    return f"{start_time.strftime('%Y-%m-%dT%H:%M:%S%z')}/{end_time.strftime('%Y-%m-%dT%H:%M:%S%z')}"


def make_request(endpoint, payload, retries=1, delay=5):
    for attempt in range(retries):
        try:
            conn = http.client.HTTPSConnection("api.telemetry.confluent.cloud")
            conn.request("POST", endpoint, body=json.dumps(payload), headers={**HEADERS, "Content-Type": "application/json"})
            response = conn.getresponse()
            if response.status == 200:
                return json.loads(response.read().decode("utf-8"))
            else:
                logging.error(f"HTTP Error {response.status}: {response.reason}")
            conn.close()
        except Exception as e:
            logging.error(f"Error: {e}")
        time.sleep(delay)
    return {}


def get_metric_data(cluster, metric, interval, granularity):
    payload = {
        "aggregations": [{"metric": metric}],
        "filter": {
            "op": "OR",
            "filters": [
                {"field": "resource.kafka.id", "op": "EQ", "value": cluster["id"]}
            ]
        },
        "granularity": granularity,
        "intervals": [interval],
        "limit": 1000
    }

    response = make_request("/v2/metrics/cloud/query", payload)
    return response if response else {"data": []}
    

def generate_report(clusters):
    cluster_names = list(clusters.keys())
    x = range(len(cluster_names))

    avg_connections = [clusters[name]["avg_active_connections"] for name in cluster_names]
    max_connections = [clusters[name]["max_avg_active_connections"] for name in cluster_names]
    avg_cluster_load = [100 * clusters[name]["avg_cluster_load"] for name in cluster_names]
    max_cluster_load = [100 *clusters[name]["max_avg_cluster_load"] for name in cluster_names]

    cku_thresholds = [18000 * clusters[name]["CKU"] for name in cluster_names]
    partitions = [clusters[name]["partition_count"] for name in cluster_names]
    partition_limits = [4500 * clusters[name]["CKU"] for name in cluster_names]
    
    today = datetime.today()
    seven_days_ago = today - timedelta(days=7) 
    end_date = today.strftime("%m_%d")
    start_date = seven_days_ago.strftime("%m_%d")
    report_name = f"Environment Report {start_date}-{end_date}.pdf"

    with PdfPages(report_name) as pdf:
        width = 0.35
        plt.figure(figsize=(10, 6))
        plt.bar([i - width/2 for i in x], partitions, width, color="skyblue", label="Partitions")
        plt.bar([i + width/2 for i in x], partition_limits, width, color="orange", label="Partition Limit")
        plt.xticks(x, cluster_names)
        plt.xlabel("Cluster")
        plt.ylabel("Partitions")
        plt.title("Cluster Partitions: Current vs Limit")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        width = 0.25
        plt.figure(figsize=(10, 6))
        plt.bar([i - width for i in x], avg_connections, width, color="skyblue", label="Avg Active Connections")
        plt.bar(x, max_connections, width, color="orange", label="Max Active Connections")
        plt.bar([i + width for i in x], cku_thresholds, width, color="red", label="Recommended Connections")
        plt.xticks(x, cluster_names)
        plt.ylabel("Active Connections")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Connections: Average, Maximum & Recommended")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        width = 0.25
        plt.figure(figsize=(10, 6))
        plt.bar([i - width for i in x], avg_cluster_load, width, color="orange", label="Avg Cluster Load")
        plt.bar(x, max_cluster_load, width, color="red", label="Max Cluster Load")
        plt.xticks(x, cluster_names)
        plt.ylabel("Cluster Load %")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Load")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

    logging.info(f"Generated Report: {report_name}")


def main():
    past_week = generate_time_interval(7)
    today = generate_time_interval(1)
    logging.info(f"Retrieving metrics for the period {past_week}...")
    
    # Metrics included here will contain an average and maximum.
    metrics = {
        "avg_active_connections": "io.confluent.kafka.server/active_connection_count",
        "avg_cluster_load": "io.confluent.kafka.server/cluster_load_percent"
    }
    partition_metric = "io.confluent.kafka.server/partition_count"

    for cluster_name, cluster_data in clusters.items():
        logging.info(f"Retrieving metrics for cluster `{cluster_name}` during interval {past_week}")
        
        for metric_name, metric_key in metrics.items():
            data = get_metric_data(cluster_data, metric_key, past_week, "PT4H")
            values = [entry["value"] for entry in data.get("data", [])]
            metric_value = sum(values) / len(values) if values else 0
            max_value = max(values) if values else 0
            cluster_data[metric_name] = metric_value
            cluster_data[f"max_{metric_name}"] = max_value

        partition_data = get_metric_data(cluster_data, partition_metric, today, "P1D")
        partition_values = [entry["value"] for entry in partition_data.get("data", [])]
        cluster_data["partition_count"] = partition_values[0] if partition_values else 0

    print(json.dumps(clusters, indent=2))
    generate_report(clusters)


if __name__ == '__main__':
    main()

# curl 'https://api.telemetry.confluent.cloud/v2/metrics/cloud/query' \
# -H "Content-Type: application/json" \
# -d '{"aggregations":[{"metric":"io.confluent.kafka.schema_registry/schema_count"}],"filter":{"op":"OR","filters":[{"field":"resource.schema_registry.id","op":"EQ","value":"lsrc-5mmo32"}]},"granularity":"PT1M","intervals":["2024-11-28T12:56:00+02:00/2024-11-28T13:56:00+02:00"],"limit":1000}' \
# -H "Authorization: Basic base64(<API_KEY>:<API_SECRET>)"