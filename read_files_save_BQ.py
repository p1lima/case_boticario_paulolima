import pandas as pd
import google.auth
from google.cloud import storage
from google.cloud import bigquery
import pandas_gbq
import os

def main():

    # For local VS Code run 
    os.environ["GCLOUD_PROJECT"] = "t-sunlight-454318-h7"

    # Initialize the Cloud clients
    credentials, project = google.auth.default()
    storage_client = storage.Client(credentials=credentials)
    bigquery_client = bigquery.Client(credentials=credentials,location="southamerica-east1")


    def list_blobs(bucket_name):
        """Lists all the blobs in the bucket."""

        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs())

        return blobs

    def read_xlsx(blobs):
        """Read all .xlsx files in blobs list and append to Dataframe."""

        df = pd.DataFrame()
        
        for blob in blobs:
            if blob.name[-4:] == 'xlsx':
                path = f"gs://{blob.bucket.name}/{blob.name}"
                # print(path)

                data = pd.read_excel(path)
                df = df._append(data)

                # Save each processed file Dataframe to CSV
                path_csv = f"gs://{blob.bucket.name}/csv/{blob.name.replace('.xlsx', '.csv')}"
                data.to_csv(path_csv, index=False)

        #print(df)
        return df

    def delete_blobs(blobs):
        """Delete blobs from Storage bucket."""

        for blob in blobs:
            if blob.name[-4:] == 'xlsx':
                path = f"gs://{blob.bucket.name}/{blob.name}"
                blob.delete()

    #------------------------------------------------------#

    """Reading files"""
    bucket_name = "case_boticario_paulolima"

    # List files/blobs in bucket
    blobs = list_blobs(bucket_name)

    # Read .xlsx files in blobs list
    df = read_xlsx(blobs)


    #-------------------------#
    """Dataframe treatment"""
    # Change Dataframe Data Types
    df['DATA_VENDA'] = pd.to_datetime(df['DATA_VENDA']).dt.date
    df = df.astype({'ID_MARCA': str, 
                'MARCA': str,
                'ID_LINHA': str,
                'LINHA': str,
                'QTD_VENDA': int})
    # print(df.dtypes)

    # Drop Dataframe duplicated rows
    df = df.drop_duplicates()
    # print(df)


    #-------------------------#
    """BigQuery - Insert files Data to Table"""
    # A temporary table is used to load data do db
    # Later, this data is merged to the final SoT/Silver table in BigQuery

    # Dataset + Table
    table_temp_id = "db_case_boticario_paulolima.tb_temp_sales_boticario"
    table_final_id = "db_case_boticario_paulolima.tb_sales_boticario"

    # Truncate BigQuery tables
    bigquery_client.query(f"TRUNCATE TABLE {project}.{table_temp_id}").result()
    # bigquery_client.query(f"TRUNCATE TABLE {project}.{table_final_id}").result()

    # Write final Dataframe to BigQuery temporary table
    pandas_gbq.to_gbq(df, table_temp_id, project_id=project, if_exists = 'append')

    # MERGE query temporary/bronze table to SoT/Silver in BigQuery
    # Dynamically create condition for MERGE query: all columns
    condition = " AND ".join([f"silver.{col} = bronze.{col}" for col in df.columns])
    merge_query = f"""
        MERGE {table_final_id} AS silver
        USING {table_temp_id} AS bronze
        ON ({condition})
        WHEN NOT MATCHED THEN 
            INSERT ({", ".join(df.columns)}) 
            VALUES ({", ".join(f"bronze.{col}" for col in df.columns)});
    """
    # Execute query
    bigquery_client.query(merge_query).result()


    #-------------------------#
    """BigQuery - Create Spec layer tables for Sales Consolidation"""
    # Using the same temporary/bronze table
    # Create aggregation steps to show Sales Consolidation views

    # SELECTs with aggregations from temporary/bronze table
    select_cons_query = []
    columns = []
    tables = []

    # 1- Consolidated sales by YEAR and MONTH
    select_cons_query_ym = f"""
        SELECT  cast(EXTRACT(year FROM DATA_VENDA) as string) AS ano,
                cast(EXTRACT(month FROM DATA_VENDA) as string)  AS mes,
                sum(QTD_VENDA) AS QTD_VENDA
        FROM {table_temp_id}
        GROUP BY 1,2
    """
    select_cons_query.append(select_cons_query_ym)
    columns.append(["ano", "mes", "QTD_VENDA"])
    tables.append("db_case_boticario_paulolima.tb_cons_sales_anomes")

    # 2- Consolidated sales by MARCA and LINHA
    # This table has the same MARCA-LINHA aggregation, has to always be truncated
    bigquery_client.query(f"TRUNCATE TABLE \
                          {project}.db_case_boticario_paulolima.tb_cons_sales_marca_linha").result()
    select_cons_query_ml = f"""
        SELECT  MARCA,
                LINHA,
                sum(QTD_VENDA) AS QTD_VENDA
        FROM {table_temp_id}
        GROUP BY 1,2
    """
    select_cons_query.append(select_cons_query_ml)
    columns.append(["MARCA", "LINHA", "QTD_VENDA"])
    tables.append("db_case_boticario_paulolima.tb_cons_sales_marca_linha")

    # 3- Consolidated sales by MARCA, YEAR and MONTH
    select_cons_query_mym = f"""
        SELECT  cast(EXTRACT(year FROM DATA_VENDA) as string)  AS ano,
                cast(EXTRACT(month FROM DATA_VENDA) as string)  AS mes,
                MARCA,
                sum(QTD_VENDA) AS QTD_VENDA
        FROM {table_temp_id}
        GROUP BY 1,2,3
    """
    select_cons_query.append(select_cons_query_mym)
    columns.append(["ano", "mes", "MARCA", "QTD_VENDA"])
    tables.append("db_case_boticario_paulolima.tb_cons_sales_anomes_marca")

    # 4- Consolidated sales by LINHA, YEAR and MONTH
    select_cons_query_lym = f"""
        SELECT  cast(EXTRACT(year FROM DATA_VENDA) as string)  AS ano,
                cast(EXTRACT(month FROM DATA_VENDA) as string)  AS mes,
                LINHA,
                sum(QTD_VENDA) AS QTD_VENDA
        FROM {table_temp_id}
        GROUP BY 1,2,3
    """
    select_cons_query.append(select_cons_query_lym)
    columns.append(["ano", "mes", "LINHA", "QTD_VENDA"])
    tables.append("db_case_boticario_paulolima.tb_cons_sales_anomes_linha")

    for query, columns, table in zip(select_cons_query, columns, tables):
    # MERGE consolidation query from temporary/bronze table 
        # to final Spec/Gold tables in BigQuery
        
        condition = " AND ".join([f"cons.{col} = temp_cons.{col}" for col in columns if col!="QTD_VENDA"])
        insert = ", ".join(columns)
        values = ", ".join(f"cons.{col}" for col in columns)

        merge_cons_query = f"""
            MERGE {table} AS temp_cons
            USING ({query}) AS cons
            ON ({condition})
            WHEN NOT MATCHED THEN 
                INSERT ({insert})
                VALUES ({values});

        """
        # Execute query
        bigquery_client.query(merge_cons_query).result()


    #-------------------------#
    """Cleaning"""
    # Delete all .xlsx files from bucket
        # Files have already been saved as CSV in 'csv' subfolder
    # delete_blobs(blobs)
    # Truncate BigQuery temporary table
    bigquery_client.query(f"TRUNCATE TABLE {project}.{table_temp_id}").result()

if __name__ == "__main__":
    main()