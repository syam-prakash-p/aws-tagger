import boto3
import pandas as pd


client = boto3.client('s3')

def list_buckets():
    response =client.list_buckets()
    buckets=[item["Name"] for item in response["Buckets"]]
    return buckets

def get_tags(bucket_names,key_match=[],key_val_match=dict(),exclude_key_match=[],exclude_key_val_match=dict()):
    data={}
    if not bucket_names:
        return data
    def update_data(bucket_name,tag):
        if data.get(bucket_name,None):
            data[bucket_name].update({tag["Key"]:tag["Value"]})
        else:
            data[bucket_name]={tag["Key"]:tag["Value"]}

    for bucket_name in bucket_names:
        try:
            bucket_tagging = client.get_bucket_tagging(Bucket=bucket_name)
        except Exception as e:
            print(e)
            data[bucket_name]={}
            print("It seems, Tag not available for the bucket : ",bucket_name)
            continue
        for tag in bucket_tagging['TagSet']:
            if tag['Key'] in exclude_key_match:
                continue
            if exclude_key_val_match.get(tag['Key'],"")==tag['Value']:
                continue
            if key_match and tag["Key"] in key_match:
                update_data(bucket_name,tag)
            if key_val_match and key_val_match.get(tag['Key'],"")==tag['Value']:
                update_data(bucket_name,tag)        
            else:
                update_data(bucket_name,tag)

    return data


def add_tags(csv_data):
    for bucket in  csv_data:
        tagset = [{'Key':key,'Value':value} for key,value in csv_data[bucket].items()]
        response = client.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                'TagSet': tagset
            },
        )    







def create_csv(filename,my_dict):
    df = pd.DataFrame.from_dict(my_dict, orient='index')
    print(df)
    csv_file_path = filename
    df.to_csv(csv_file_path)
    print(f"CSV file '{csv_file_path}' created successfully.")



def read_csv(filename):

    # Read the CSV file (replace 'data.csv' with your actual file path)
    df = pd.read_csv(filename,dtype=str)

    # Print the DataFrame (optional)
    print(df)

    df.set_index(df.columns[0], inplace=True)

    my_dict = df.to_dict(orient='index')

    return my_dict






############# Tested get section
# buckets=list_buckets()
# print(buckets)
# tag_data=get_tags(buckets)
# print(tag_data)
# create_csv(tag_data)

################# PUT section
# data=read_csv()
# add_tags(data)



import argparse

def main():
    parser = argparse.ArgumentParser(description="AWS Tagger: Manage tags for S3 resources")

    # Common arguments
    parser.add_argument("--file", required=True, help="CSV file path")
    parser.add_argument("--resource", required=True, choices=["s3"], help="Resource type (e.g., s3)")
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="action", title="Actions")

    # Add tags
    add_tags_parser = subparsers.add_parser("add-tags", help="Add tags to resources")
    add_tags_parser.add_argument("--tags", required=True, help="Comma-separated list of tags (key1=value1,key2=value2)")

    # Get tags
    get_tags_parser = subparsers.add_parser("get-tags", help="Get tags from resources")
    get_tags_parser.add_argument("--exclude-key", nargs="+", help="Exclude specific tag keys (to filter tags)")
    get_tags_parser.add_argument("--exclude-key-val", nargs="+", help="Exclude specific key-value pairs (to filter tags)")
    get_tags_parser.add_argument("--include-key", nargs="+", help="Include specific tag keys (to filter tags)")
    get_tags_parser.add_argument("--include-key-val", nargs="+", help="Include specific key-value pairs (to filter tags)")

    args = parser.parse_args()
    print(args)
    if args.action == "get-tags":
        buckets=list_buckets()
        if args.include_key_val:
            key_val_match=dict([ item.split('=') for item in args.include_key_val[0].split(',')])
        else:
            key_val_match=[]
        if args.exclude_key_val:
            exclude_key_val_match=dict([ item.split('=') for item in args.exclude_key_val[0].split(',')])
        else:
            exclude_key_val_match=[]    
        if args.include_key:
            include_key=args.include_key[0].split(',')
        else:
            include_key=[]
        if args.exclude_key:
            exclude_key=args.exclude_key[0].split(',')
        else:
            include_key=[]
        tag_data=get_tags(buckets,key_match=include_key,key_val_match=key_val_match,exclude_key_match=exclude_key,exclude_key_val_match=exclude_key_val_match)
        create_csv(args.file,tag_data)



if __name__ == "__main__":
    main()
