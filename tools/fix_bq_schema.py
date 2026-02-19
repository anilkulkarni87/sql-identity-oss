from google.cloud import bigquery


def fix_schema():
    # Fix output tables
    client = bigquery.Client(project="ga4-134745")
    dataset_id = "idr_out"
    tables = [
        "identity_resolved_membership_current",
        "identity_clusters_current",
        "identity_edges_current",
    ]

    for t_name in tables:
        full_table_id = f"ga4-134745.{dataset_id}.{t_name}"
        try:
            table = client.get_table(full_table_id)
            col_names = [field.name for field in table.schema]

            if "run_id" not in col_names:
                print(f"Adding run_id to {full_table_id}...")
                sql = f"ALTER TABLE `{full_table_id}` ADD COLUMN run_id STRING"
                query_job = client.query(sql)
                query_job.result()
                print(f"Successfully added run_id to {full_table_id}")
            else:
                print(f"run_id already exists in {full_table_id}")

        except Exception as e:
            print(f"Error processing {full_table_id}: {str(e)}")

    # Check run_state in meta
    meta_dataset_id = "idr_meta"
    full_table_id = f"ga4-134745.{meta_dataset_id}.run_state"
    try:
        table = client.get_table(full_table_id)
        col_names = [field.name for field in table.schema]

        # run_state uses last_run_id, unrelated to this specific error but good to check
        if "last_run_id" not in col_names:
            print(f"Adding last_run_id to {full_table_id}...")
            sql = f"ALTER TABLE `{full_table_id}` ADD COLUMN last_run_id STRING"
            query_job = client.query(sql)
            query_job.result()
            print(f"Successfully added last_run_id to {full_table_id}")
        else:
            print(f"last_run_id already exists in {full_table_id}")

    except Exception as e:
        print(f"Error processing {full_table_id}: {str(e)}")


if __name__ == "__main__":
    fix_schema()
