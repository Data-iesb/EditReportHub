import boto3
import json
import pandas as pd

# S3 client initialization
s3_client = boto3.client('s3')

def get_reports_from_s3():
    # Get the reports.json file from the S3 bucket
    s3_bucket = 'dataiesb'
    s3_key = 'reports.json'

    # Download the file content
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    data = response['Body'].read().decode('utf-8')

    # Load the JSON data
    reports_data = json.loads(data)
    return reports_data

def create_dataframe(reports_data):
    # Process the JSON into a DataFrame
    reports_list = []

    for report_id, report_info in reports_data.items():
        report = {
            'ID': report_id,
            'Título': report_info.get('titulo'),
            'Descrição': report_info.get('descricao'),
            'Autor': report_info.get('autor'),
            'S3 Path': report_info.get('id_s3'),
            'Deletado': report_info.get('deletado')
        }
        reports_list.append(report)

    # Create a DataFrame
    df = pd.DataFrame(reports_list)
    return df

def generate_readme(df):
    # Convert the DataFrame to Markdown table format
    readme_content = "# Relatórios\n\n"
    readme_content += df.to_markdown(index=False)
    
    # Write the content to the README.md file
    with open('README.md', 'w') as f:
        f.write(readme_content)

if __name__ == "__main__":
    reports_data = get_reports_from_s3()
    df = create_dataframe(reports_data)
    generate_readme(df)
    print("README.md file has been updated.")

