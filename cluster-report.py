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

with open('schema_registries.json') as f:
    schema_registries = json.load(f)

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


def get_metric_data(cluster, metric, interval, granularity, field):
    payload = {
        "aggregations": [{"metric": metric}],
        "filter": {
            "op": "OR",
            "filters": [
                {"field": field, "op": "EQ", "value": cluster["id"]}
            ]
        },
        "granularity": granularity,
        "intervals": [interval],
        "limit": 1000
    }

    response = make_request("/v2/metrics/cloud/query", payload)
    return response if response else {"data": []}

def add_text_to_pdf(text, pdf, max_lines=25, line_height=0.04):
    lines = text.split("\n")
    for i in range(0, len(lines), max_lines):
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axis("off")
        page_text = "\n".join(lines[i:i + max_lines])
        ax.text(0.05, 0.95, page_text, fontsize=10, verticalalignment="top", horizontalalignment="left", wrap=True)
        pdf.savefig(fig)
        plt.close(fig)

def generate_report(clusters, schema_registries):
    cluster_names = list(clusters.keys())
    x = range(len(cluster_names))

    avg_connections = [clusters[name]["avg_active_connections"] for name in cluster_names]
    max_connections = [clusters[name]["max_avg_active_connections"] for name in cluster_names]
    avg_cluster_load = [100 * clusters[name]["avg_cluster_load"] for name in cluster_names]
    max_cluster_load = [100 *clusters[name]["max_avg_cluster_load"] for name in cluster_names]

    cku_thresholds = [18000 * clusters[name]["CKU"] for name in cluster_names]
    partitions = [clusters[name]["partition_count"] for name in cluster_names]
    partition_limits = [4500 * clusters[name]["CKU"] for name in cluster_names]

    environment_names = list(schema_registries.keys())
    xs = range(len(environment_names))
    schema_counts = [schema_registries[name]["schema_counts"] for name in environment_names]

    today = datetime.today()
    seven_days_ago = today - timedelta(days=7)
    end_date = today.strftime("%m_%d")
    start_date = seven_days_ago.strftime("%m_%d")
    report_name = f"Environment Report {start_date}-{end_date}.pdf"

    with PdfPages(report_name) as pdf:
        # Partition Limit Diagram
        width = 0.45
        plt.figure(figsize=(10, 6))
        partition_bars = plt.bar([i - width/2 for i in x], partitions, width, color="skyblue", label="Partitions")
        partition_limit_bars = plt.bar([i + width/2 for i in x], partition_limits, width, color="orange", label="Partition Limit")

        for bar in partition_bars:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=6)
        for bar in partition_limit_bars:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=6)

        plt.xticks(x, cluster_names, rotation=45, ha="right")
        plt.xlabel("Cluster")
        plt.ylabel("Partitions")
        plt.title("Cluster Partitions: Current vs Limit")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        text = """Partition Management:
Each CKU (Confluent Kafka Unit) supports a maximum of 4,500 partitions before replication.
This means that for a cluster with multiple CKUs, the total partition limit is calculated as
4,500 multiplied by the cluster's CKU count.

Reaching this limit prevents the creation of additional partitions, and exceeding it may lead to
degraded performance or errors when attempting to create new topics.

Partitions are fairly stable and typically only increase when services are expanded.

Some general recommendations for managing partition utilization includes:
- Delete unused topics with high partition counts.
- Ensure that topics are created with the appropriate number of partitions.
- Partitions are Kafka's unit of parallelism and lower environments seldomly require a lot of partitions.
- Note: A topics partition's can only be increased and not decreased.
"""
        add_text_to_pdf(text, pdf)

        # Active Connections Diagram
        width = 0.30
        plt.figure(figsize=(12, 6))
        avg_bar = plt.bar([i - width for i in x], avg_connections, width, color="skyblue", label="Avg Active Connections")
        max_bar = plt.bar(x, max_connections, width, color="orange", label="Max Active Connections")
        recommended_bar = plt.bar([i + width for i in x], cku_thresholds, width, color="red", label="Recommended Connections")
        for bar in avg_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=5)
        for bar in max_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=5)
        for bar in recommended_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=5)

        plt.xticks(x, cluster_names, rotation=60, ha="right")
        plt.ylabel("Active Connections")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Connections: Average, Maximum & Recommended")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        text = """Active Connection Management:
Each CKU (Confluent Kafka Unit) supports up to 18,000 client connections.
All non-production clusters typically have 1 CKU, while FF & SLI production clusters have 3 CKUs,
and the remaining production clusters have 2 CKUs.

This maximum represents a soft limit and should be used as a guideline to avoid latency and performance issues,
particularly for test clients. Exceeding the recommended connection count may not impact all workloads
but can result in increased latency for test clients.

It is important to monitor how the connection count affects cluster load, as this provides the most accurate
representation of the workload's impact on the cluster's underlying resources.

Some general recommendations for managing Active Client Connections include:
- Reduce unncessary or unused client connections.
- Review and optimize client configurations.
- Increase the size of the cluster (this has a significant cost impact)
"""
        add_text_to_pdf(text, pdf)

        # Cluster Load Diagram
        width = 0.30
        plt.figure(figsize=(10, 6))
        avg_bar = plt.bar([i - width for i in x], avg_cluster_load, width, color="orange", label="Avg Cluster Load")
        max_bar = plt.bar(x, max_cluster_load, width, color="red", label="Max Cluster Load")
        for bar in avg_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=8)
        for bar in max_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=8)
        plt.xticks(x, cluster_names, rotation=45)
        plt.ylabel("Cluster Load %")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Load")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        text = """Cluster Load Management:
Cluster load measures the percentage utilization of the cluster's resources.
A cluster load below 50% is considered optimal, while loads between 50% and 75% indicate moderate utilization.
A load above 75% suggests high utilization, which could impact performance.

Regular monitoring and optimization of cluster load are crucial to ensuring optimal performance for your workloads.

Some general recommendations for managing high cluster load includes:
- Identify the factors that are driving the cluster load.
- Review and optimize client configurations.
- Increase the size of the cluster (this has a significant cost impact).
"""
        add_text_to_pdf(text, pdf)
        # Schema Reigstry Diagram
        width = 0.45
        plt.figure(figsize=(10, 6))
        schema_bar = plt.bar([i - width/2 for i in xs], schema_counts, width, color="skyblue", label="Schemas")
        for bar in schema_bar:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{bar.get_height():.0f}', ha='center', va='bottom')

        plt.axhline(1000, color="red", linestyle="solid", label="Free Schemas")
        plt.xticks(xs, environment_names, rotation=45, ha="right")
        plt.xlabel("Environment")
        plt.ylabel("Schemas")
        plt.title("Schema Usage per Environment")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        text = """Schema Management:
Schema registries store schemas for topics and ensure compatibility.
The recommended limit for schemas per registry is 1,000 to maintain optimal performance.
Exceeding this limit may result in slower schema lookups and increased latency for client applications.
Remaining within the limit provides a small cost saving.
There is also a point where upgrading the the schema registry package could save costs.

Some general recommendations for Schema Registry include:
- Consolidate or remove unused schemas to stay within recommended limits.
- Ensure schemas are versioned and compatible to avoid unnecessary proliferation.
- Upgrade the schema governance package (perform a cost analysis first to determine if this will result in a saving)
"""
        add_text_to_pdf(text, pdf)
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
        logging.info(f"Retrieving metrics for cluster `{cluster_name}`...")

        for metric_name, metric_key in metrics.items():
            data = get_metric_data(cluster_data, metric_key, past_week, "PT4H", "resource.kafka.id")
            values = [entry["value"] for entry in data.get("data", [])]
            metric_value = sum(values) / len(values) if values else 0
            max_value = max(values) if values else 0
            cluster_data[metric_name] = metric_value
            cluster_data[f"max_{metric_name}"] = max_value

        partition_data = get_metric_data(cluster_data, partition_metric, today, "P1D", "resource.kafka.id")
        partition_values = [entry["value"] for entry in partition_data.get("data", [])]
        cluster_data["partition_count"] = partition_values[0] if partition_values else 0

    schema_metric = "io.confluent.kafka.schema_registry/schema_count"

    for environment_name, environment_data in schema_registries.items():
        logging.info(f"Retrieving schema metrics for environment `{environment_name}`...")
        schema_data = get_metric_data(environment_data, schema_metric, today, "P1D", "resource.schema_registry.id")
        schema_counts = [entry["value"] for entry in schema_data.get("data", [])]
        environment_data["schema_counts"] = schema_counts[0] if schema_counts else 0

    print(json.dumps(schema_registries, indent=2))
    print(json.dumps(clusters, indent=2))
    generate_report(clusters, schema_registries)


if __name__ == '__main__':
    main()