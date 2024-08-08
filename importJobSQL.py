import pandas as pd
import hashlib
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np

load_dotenv()

# Database connection details
host = os.getenv("POSTGRESS_HOST")
database = os.getenv("POSTGRESS_DB")
user = os.getenv("POSTGRESS_USER")
password = os.getenv("POSTGRESS_PASSWORD")
port = os.getenv("POSTGRESS_PORT")

def main():
    try:
        print("Started importing...")

        # Function to generate a unique hash ID based on row values
        def generate_hash_id(row):
            unique_str = ''.join(row.astype(str))
            hash_id = hashlib.sha256(unique_str.encode()).hexdigest()
            return hash_id

        # Load the CSV file
        file_path = 'mmt_recon_data.csv'
        df = pd.read_csv(file_path)

        # Apply the function to each row to create a new column 'ID'
        df['id'] = df.apply(generate_hash_id, axis=1)

        # Remove commas from numeric columns and convert to numeric type
        numeric_columns = [
            "Basic_Fare", "K3", "AI_CGST", "AI_SGST", "AI_IGST", "AI_TaxRate", "AI_Taxable", "AI_TotalAmount"
        ]

        for col in numeric_columns:
            df[col] = df[col].apply(lambda x: str(x).replace(',', '') if pd.notnull(x) else x).astype(float)

        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()

        # Create the table (if it doesn't already exist)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.mmt_flight_recon (
            "id" text NOT NULL,
            "Agency_Name" text NULL,
            "Agency_Invoice_Number" text NULL,
            "Customer_GSTIN" text NULL,
            "Customer_Name" text NULL,
            "Workspace" text NULL,
            "Ticket_PNR" text NULL,
            "Ticket_Number" text NULL,
            "PNR" text NULL,
            "Basic_Fare" numeric NULL,
            "K3" numeric NULL,
            "Location" text NULL,
            "Booking_Date" text NULL,
            "MainTag" text NULL,
            "Vendor" text NULL,
            "TravellerName" text NULL,
            "AI_InvoiceNoteNumber" text NULL,
            "AI_InvoiceNoteDate" text NULL,
            "AI_CGST" numeric NULL,
            "AI_SGST" numeric NULL,
            "AI_IGST" numeric NULL,
            "AI_TaxRate" numeric NULL,
            "AI_Taxable" numeric NULL,
            "AI_TotalAmount" numeric NULL,
            "AI_VendorGSTIN" text NULL,
            "Origin" text NULL,
            "PlaceofSupply" text NULL,
            "AI_DocumentType" text NULL,
            "SOTO_Status" text NULL,
            "Invoice_Status" text NULL,
            "GST_Exempted" text NULL,
            "Invoice_link" text NULL,
            "Provision_Status" text NULL,
            CONSTRAINT mmt_flight_recon_pkey PRIMARY KEY ("id")
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        # Insert data into the PostgreSQL table
        for index, row in df.iterrows():
            insert_query = """INSERT INTO public.mmt_flight_recon ("id","Agency_Name","Agency_Invoice_Number", 
            "Customer_GSTIN","Customer_Name","Workspace","Ticket_PNR","Ticket_Number","PNR","Basic_Fare","K3","Location", 
            "Booking_Date","MainTag","Vendor","TravellerName","AI_InvoiceNoteNumber","AI_InvoiceNoteDate","AI_CGST", 
            "AI_SGST","AI_IGST","AI_TaxRate","AI_Taxable","AI_TotalAmount","AI_VendorGSTIN","Origin","PlaceofSupply", 
            "AI_DocumentType","SOTO_Status","Invoice_Status","GST_Exempted","Invoice_link","Provision_Status") VALUES ( %s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (
            "id") DO NOTHING;
        
            """
            cur.execute(insert_query, (
                row["id"], row["Agency_Name"], row["Agency_Invoice_Number"], row["Customer_GSTIN"], row["Customer_Name"],
                row["Workspace"], row["Ticket_PNR"], row["Ticket_Number"], row["PNR"], row["Basic_Fare"], row["K3"],
                row["Location"],
                row["Booking_Date"], row["MainTag"], row["Vendor"], row["TravellerName"], row["AI_InvoiceNoteNumber"],
                row["AI_InvoiceNoteDate"], row["AI_CGST"], row["AI_SGST"], row["AI_IGST"], row["AI_TaxRate"],
                row["AI_Taxable"],
                row["AI_TotalAmount"], row["AI_VendorGSTIN"], row["Origin"], row["PlaceofSupply"], row["AI_DocumentType"],
                row["SOTO_Status"], row["Invoice_Status"], row["GST_Exempted"], row["Invoice_link"], row["Provision_Status"]
            ))
        # Commit the transaction
        conn.commit()
        # Close the connection
        cur.close()
        conn.close()
        print("Imported data successfully")
    except Exception as e:
        print("Exception happened in the import job: "+str(e))


if __name__ == '__main__':
    main()
