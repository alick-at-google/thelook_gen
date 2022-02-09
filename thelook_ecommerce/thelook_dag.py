from airflow import DAG
from airflow.contrib.operators import gcs_to_bq, kubernetes_pod_operator
from datetime import datetime, timedelta

default_args = {
    'owner': 'alick',
    'start_date': datetime(2022, 1, 31),
    'email': ['alick@google.com'],
    'email_on_failures': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='thelook_ecommerce',
    max_active_runs=1,
    default_args=default_args,
    description="Generating thelook and loads from GCS to BQ",
    schedule_interval="@hourly",
    default_view="graph"
) as dag:  # run DAG once a day

    # Run CSV transform within kubernetes pod
    thelook_gen = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="generate_thelook",
        name="thelook_gen",
        namespace="default",
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        image="gcr.io/looker-private-demo/thelook_ecommerce:1.0",
        env_vars={
            "NUM_OF_USERS": "100",
            "TARGET_GCS_BUCKET": "us-central1-thelook-faker-d1ecf43a-bucket",
            "EXTRANEOUS_HEADERS": '["event_type", "ip_address", "browser", "traffic_source", "session_id", "sequence_number", "uri", "is_sold"]'
        },
        # resources={"request_memory": "4G", "request_cpu": "1"},
    )
    products_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_products_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/products.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_ecommerce.products",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "retail_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sku", "type": "STRING", "mode": "NULLABLE"},
            {"name": "distribution_center_id", "type": "INTEGER", "mode": "NULLABLE"}
            ],
    )
    events_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_events_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/events.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.events",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sequence_number", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "browser", "type": "STRING", "mode": "NULLABLE"},
            {"name": "traffic_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "uri", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_type", "type": "STRING", "mode": "NULLABLE"}
            ],
    )
    inventory_items_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_inventory_items_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/inventory_items.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.inventory_items",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "sold_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "product_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_retail_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "product_department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_sku", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_distribution_center_id", "type": "INTEGER", "mode": "NULLABLE"}
            ],
    )
    order_items_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_order_items_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/order_items.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.order_items",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "order_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "inventory_item_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "shipped_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "delivered_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "returned_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "sale_price", "type": "FLOAT", "mode": "NULLABLE"}
            ],
    )
    orders_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_orders_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/orders.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.orders",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "order_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "returned_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "shipped_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "delivered_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "num_of_item", "type": "INTEGER", "mode": "NULLABLE"}
            ],
    )
    users_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_users_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/users.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.users",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "street_address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "traffic_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
            ],
    )
    distribution_centers_gcs_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_distribution_centers_to_bq",
        bucket="us-central1-thelook-faker-d1ecf43a-bucket",
        source_objects=["data/distribution_centers.csv"],
        source_format="CSV",
        ignore_unknown_values=True,
        destination_project_dataset_table="thelook_composer.distribution_centers",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"}
            ],
    )

thelook_gen >> products_gcs_to_bq >> events_gcs_to_bq >> inventory_items_gcs_to_bq >> order_items_gcs_to_bq >> orders_gcs_to_bq >> users_gcs_to_bq >> distribution_centers_gcs_to_bq
 
