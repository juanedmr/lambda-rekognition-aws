import boto3
from urllib.parse import unquote_plus

def write_data_to_dax_table(labels,key):
    client = boto3.client('dynamodb')
    count=1
    for i in range(0,len(labels)):
        x=labels[i]
        print(x)
        data = client.put_item(
        TableName='labelsimages',
        Item={
            'id': {
              'S': str(key)
            },
            'name': {
              'S': x['Name']
            },
            'confidence': {
              'N': str(x['Confidence'])
            },
            'n':{
                'N':str(count)
            }
        }
        )
        count+=1


def label_function(bucket, name):
    """This takes an S3 bucket and a image name!"""
    print(f"This is the bucketname {bucket} !")
    print(f"This is the imagename {name} !")
    rekognition = boto3.client("rekognition","us-east-1")
    response = rekognition.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':name}},
     MaxLabels=10,)
    labels = response["Labels"]
    #print(f"I found these labels {labels}")
    return labels


def lambda_handler(event, context):
    """This is a computer vision lambda handler"""

    print(f"This is my S3 event {event}")
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        print(f"This is my bucket {bucket}")
        key = unquote_plus(record["s3"]["object"]["key"])
        print(f"This is my key {key}")

    my_labels = label_function(bucket=bucket, name=key)
    
    print(f"Writing items to the table.")
    write_data_to_dax_table(my_labels,key)
    return "OK"
    
