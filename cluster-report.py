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
    """
    Helper function to add long text to multiple PDF pages.
    """
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
        width = 0.35
        plt.figure(figsize=(10, 6))
        plt.bar([i - width/2 for i in x], partitions, width, color="skyblue", label="Partitions")
        plt.bar([i + width/2 for i in x], partition_limits, width, color="orange", label="Partition Limit")
        plt.xticks(x, cluster_names, rotation=45, ha="right")
        plt.xlabel("Cluster")
        plt.ylabel("Partitions")
        plt.title("Cluster Partitions: Current vs Limit")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Partition Count Analysis
        text = "Partition Count Analysis\n\n"
        text += f"Summary: The average number of partitions across all clusters is {sum(partitions) / len(partitions):.0f}. "
        text += f"The cluster with the highest partition count is {cluster_names[partitions.index(max(partitions))]} with {max(partitions):.0f} partitions.\n\n"

        # Partition Limits Explained
        text = """Partition Limits:
Each CKU (Confluent Kafka Unit) supports a maximum of 4,500 partitions before replication. 
This means that for a cluster with multiple CKUs, the total partition limit is calculated as 
4,500 multiplied by the cluster's CKU count. 

Reaching this limit prevents the creation of additional partitions, and exceeding it may lead to 
degraded performance or errors when attempting to create new topics.
"""

        text += "Cluster Details:\n"
        for i, cluster_name in enumerate(cluster_names):
            utilization = (partitions[i] / partition_limits[i]) * 100 if partition_limits[i] > 0 else 0
            text += f"- {cluster_name}: Partitions: {partitions[i]:.0f} | Limit: {partition_limits[i]} | Utilization: {utilization:.2f}%\n"

        # # Add recommendations if limits are high
        # text += "\nRecommendations:\n"
        # text += "To reduce the number of partitions:\n"
        # text += "- Delete unused topics with high partition counts.\n"
        # text += "- Consolidate topics with similar data into fewer partitions.\n"
        # text += "- Use the Kafka Admin interface to increase the partition count of existing topics if needed.\n\n"

        # Save the text to the PDF using the helper function
        add_text_to_pdf(text, pdf)


        # Active Connections Graph
        width = 0.25
        plt.figure(figsize=(10, 6))
        plt.bar([i - width for i in x], avg_connections, width, color="skyblue", label="Avg Active Connections")
        plt.bar(x, max_connections, width, color="orange", label="Max Active Connections")
        plt.bar([i + width for i in x], cku_thresholds, width, color="red", label="Recommended Connections")
        plt.xticks(x, cluster_names, rotation=45, ha="right")
        plt.ylabel("Active Connections")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Connections: Average, Maximum & Recommended")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        width = 0.25

        # Active Connections Analysis
        text = "Active Connections Analysis\n\n"
        
        # Summary
        text += f"Summary: The average number of active connections across all clusters is {sum(avg_connections) / len(avg_connections):.0f}. "
        text += f"The cluster with the highest active connections is {cluster_names[max_connections.index(max(max_connections))]} with {max(max_connections):.0f} connections.\n\n"

        # Explanation of Connection Limits
        text += """Connection Limits Explained:
Each CKU (Confluent Kafka Unit) supports up to 18,000 client connections. 
All non-production clusters typically have 1 CKU, while FF & SLI production clusters have 3 CKUs, 
and the remaining production clusters have 2 CKUs. 

This is a soft limit and should be used as a guideline to avoid latency and performance issues, 
particularly for test clients. Exceeding the recommended connection count may not impact all workloads 
but can result in increased latency for test clients. 

It is important to monitor how the connection count affects cluster load, as this provides the most accurate 
representation of the workload's impact on the cluster's underlying resources.
"""
        
        # Cluster Details
        text += "Cluster Details:\n"
        for i, cluster_name in enumerate(cluster_names):
            status = "Within Limit" if max_connections[i] <= cku_thresholds[i] else "Exceeds Limit"
            text += f"- {cluster_name}: Avg Connections: {avg_connections[i]:.0f} | Max Connections: {max_connections[i]:.0f} | Recommended Limit: {cku_thresholds[i]} | Status: ({status})\n"

        # # Recommendations
        # text += "\nRecommendations:\n"
        # text += "- Monitor cluster load as connection count increases to ensure performance remains optimal.\n"
        # text += "- Reduce the number of unnecessary or unused client connections.\n"
        # text += "- If connection limits are regularly exceeded, consider increasing the number of CKUs in the cluster.\n"

        # Save the text to the PDF using the helper function
        add_text_to_pdf(text, pdf)


        # Cluster Load Graph
        plt.figure(figsize=(10, 6))
        plt.bar([i - width for i in x], avg_cluster_load, width, color="orange", label="Avg Cluster Load")
        plt.bar(x, max_cluster_load, width, color="red", label="Max Cluster Load")
        plt.xticks(x, cluster_names, rotation=45, ha="right")
        plt.ylabel("Cluster Load %")
        plt.xlabel("Cluster")
        plt.title(f"Cluster Load")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # # Cluster Load Analysis
        text = "Cluster Load Analysis\n\n"

        # Summary
        average_load = sum(avg_cluster_load) / len(avg_cluster_load)
        max_load_cluster = cluster_names[avg_cluster_load.index(max(avg_cluster_load))]
        max_load = max(avg_cluster_load)
        text += f"Summary: The average cluster load across all clusters is {average_load:.2f}%. "
        text += f"The cluster with the highest load is {max_load_cluster} with a load of {max_load:.2f}%.\n\n"

        # Explanation of Cluster Load
        text += """Cluster Load Explained:
Cluster load measures the percentage utilization of the cluster's resources. 
A cluster load below 50% is considered optimal, while loads between 50% and 75% indicate moderate utilization. 
A load above 75% suggests high utilization, which could impact performance. 

Regular monitoring and optimization of cluster load are crucial to ensuring optimal performance for your workloads.
"""

        # Cluster Details
        text += "Cluster Details:\n"
        for i, cluster_name in enumerate(cluster_names):
            status = "Optimal" if avg_cluster_load[i] < 50 else "Moderate" if avg_cluster_load[i] < 75 else "High"
            text += f"- {cluster_name}: Avg Load: {avg_cluster_load[i]:.2f}% | Max Load: {max_cluster_load[i]:.2f}% | Status: ({status})\n"

        # # Recommendations
        # text += "\nRecommendations:\n"
        # text += "- Optimize workload distribution to reduce load on clusters with high utilization.\n"
        # text += "- Regularly monitor the cluster load to ensure it remains within acceptable limits.\n"
        # text += "- Consider increasing CKU capacity for clusters consistently exceeding 75% load.\n"

        # Save to PDF
        add_text_to_pdf(text, pdf)


        width = 0.35
        plt.figure(figsize=(10, 6))
        plt.bar([i - width/2 for i in xs], schema_counts, width, color="skyblue", label="Schemas")
        plt.axhline(1000, color="red", linestyle="solid", label="Free Schemas")
        plt.xticks(xs, environment_names, rotation=45, ha="right")
        plt.xlabel("Environment")
        plt.ylabel("Schemas")
        plt.title("Schema Usage per Environment")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Schema Count Analysis
        text = "Schema Count Analysis\n\n"

        # Summary
        total_schemas = sum(schema_counts)
        max_schemas = max(schema_counts)
        max_schemas_env = environment_names[schema_counts.index(max_schemas)]
        text += f"Summary: The total number of schemas across all environments is {total_schemas}. "
        text += f"The environment with the highest number of schemas is {max_schemas_env} with {max_schemas} schemas.\n\n"

        # Explanation of Schema Limits
        text += """Schema Limits Explained:
Schema registries store schemas for topics and ensure compatibility. 
The recommended limit for schemas per registry is 1,000 to maintain optimal performance. 
Exceeding this limit may result in slower schema lookups and increased latency for client applications.
"""

        # Schema Registry Details
        text += "Schema Registry Details:\n"
        for i, env_name in enumerate(environment_names):
            status = "Within Limit" if schema_counts[i] <= 1000 else "Exceeds Limit"
            text += f"- {env_name}: Schema Count: {schema_counts[i]} | Recommended Limit: 1000 | Status: ({status})\n"

        # # Recommendations
        # text += "\nRecommendations:\n"
        # text += "- Consolidate and remove unused schemas to stay within recommended limits.\n"
        # text += "- Ensure schemas are versioned and compatible to avoid unnecessary proliferation.\n"
        # text += "- If limits are exceeded, consider splitting schemas across multiple registries or optimizing schema reuse.\n"

        # Save to PDF
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