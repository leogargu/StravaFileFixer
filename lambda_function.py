import json
import os
from botocore.exceptions import ClientError
import boto3
import botocore

# Given a csv file converted from a fit file, this function creates another
# cvs file with the same information, but with no 0 entries for the heart rate
# Instead, it assigns the previous non-zero value.
# returns the path to the modified csv file.
def fix_file(input_file) :
    name_of_file = os.path.basename(input_file)
    file_exists = os.path.isfile(input_file)

    if file_exists is False :
        raise Exception("Input file {x} does not exist".format(x=input_file))
    
    out_file = "/tmp/fixed_" + name_of_file

    out_f = open(out_file,'w')
    last_heat_rate = 60
    with open(input_file) as f:
        line = f.readline()
        while line:
            data_array = line.split(',')
            if data_array[0] == "Data":
                if "heart_rate" in data_array:
                    heart_idx = data_array.index("heart_rate")
                    rate = data_array[heart_idx + 1].strip('\"')
                    if int(rate) == 0:
                        # Need to modify the line
                        data_array[heart_idx + 1] = '"' + str(last_heart_rate) + '"'
                        line = ""
                        for element in data_array:
                            line += str(element) + ','
                        # Added a comma too many
                        line = line[:-1]
                    else:
                        last_heart_rate = int(rate)
            out_f.write(line)
            line = f.readline()
    out_f.close() 
    return out_file


##################################
####     Lambda function      ####
##################################
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    input_file_name = os.path.basename(key)
    input_file = "/tmp/" + input_file_name

    s3 = boto3.resource('s3')
    s3Client = boto3.client('s3')

    # Download object
    print("Downloading " + key + " from bucket " + bucket_name)
    try:
        s3.Bucket(bucket_name).download_file(key, input_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

    # Get object metadata
    print("Getting object metadata")
    external_id = ""
    activity_id = ""
    activity_name = ""
    try:
        metadata = s3Client.head_object(Bucket=bucket_name, Key=key)
        activity_name = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-original_name']
        activity_id = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-activity_id']
        external_id = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-external_id']
        print("Metadata: activity name = " + activity_name)
        print("Metadata: activity id = " + activity_id)
        print("Metadata: name of original fit file uploaded = " + external_id)
    except Exception as e:
        print(e)

    # Fix the downloaded csv file
    print("Fixing file " + input_file)
    fixed_file = fix_file(input_file)
    # Upload the fixed csv file to S3, preserving the metadata
    s3_path = "to_convert/" + os.path.basename(fixed_file)
    print("Uploading fixed file, " + fixed_file + ", to bucket " + bucket_name + " as " + s3_path)
    metadata = {"Metadata": {"Original_Name":activity_name, "External_Id":external_id, "Activity_Id":activity_id}}
    s3.Bucket(bucket_name).upload_file(fixed_file, s3_path, ExtraArgs=metadata)

