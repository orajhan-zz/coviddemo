import oci
import csv
import json
import requests
from base64 import b64encode, b64decode
import time
import io
import os
from fdk import response

def handler(ctx, data: io.BytesIO=None):
    signer = oci.auth.signers.get_resource_principals_signer()
    bucket_name = os.environ.get("OCI_BUCKETNAME")
    resp = do(signer, bucket_name)
    return response.Response(ctx,response_data=json.dumps(resp),headers={"Content-Type": "application/json"})

#Get Streaming OCID
def get_streaming_ocid(stream_admin_client,stream_name,compartment_id):
    list_streams = stream_admin_client.list_streams(compartment_id=compartment_id, name=stream_name, lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    #print(list_streams.data)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        #print("Streaming Name : {}".format(list_streams.data[0].name))
        #print("Streaming Summary : {}".format(list_streams.data[0]))
        sid = list_streams.data[0].id
        messages_endpoint = list_streams.data[0].messages_endpoint
        #stream_pool_id = list_streams.data[0].stream_pool_id
        return sid,messages_endpoint

# Build schema details and payload as a JSON format for Kafka Sink Connector
def kafka_structure(source_data):
    try:
        schema_structure = "{ \'schema\': { \'type\': \'struct\', \'fields\': [{ \'type\': \'int64\', \'optional\': false, \'field\': \'SERIALNUM\' }, { \'type\': \'string\', \'optional\': false, \'field\': \'PATIENT\' }, { \'type\': \'string\', \'optional\': false, \'field\': \'CONFIRMATION_DATE\' }, { \'type\': \'string\', \'optional\': false, \'field\': \'RESIDENCE\' }, { \'type\': \'string\', \'optional\': true, \'field\': \'TRAVEL\' }, { \'type\': \'string\', \'optional\': false, \'field\': \'CONTACT_FORCE\' }, { \'type\': \'string\', \'optional\': true, \'field\': \'ACTION\' }], \'optional\': false, \'name\': \'TEST\' }, \'payload\': "
        kafka_list = []
        for row in range(len(source_data)):
            kafka_format = schema_structure+str(source_data[row]).upper()+"}"
            kafka_list.append(kafka_format.replace("\'","\""))
            kafka_list.append(kafka_format)
        print("kafka list: {}".format(kafka_list[0]))
    except Exception as e:
        print("----------------- Error while converting Kafka structure -------------------")
        print(e)
        print("--------------------------------------End------------------------------------")

    return kafka_list

#Start Streaming
def put_messages_streaming(stream_client, streaming_ocid, data):
    #Record start time
    print("Streaming Starts at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
    #total_records = len(list(data))
    #print("Total records: {}".format(total_records))

    # Stream the content of the object into my code
    put_messages_details = []
    batch_size = int(os.environ.get("BATCH_SIZE"))

    for i in range(0, len(data), batch_size):
        for row in data[i:i + batch_size]:
            try:
                # Encode key and value, Append into list
                #encoded_value = base64.urlsafe_b64encode(str(row).encode('UTF-8')).decode('ascii')
                encoded_value = b64encode(str(row).encode()).decode()
                # Append into list
                put_messages_details.append(oci.streaming.models.PutMessagesDetailsEntry(value=encoded_value))

                # Create Message Details with list
                messages = oci.streaming.models.PutMessagesDetails(messages=put_messages_details)

            except Exception as e:
                print("----------Failed to push the following message -------------------")
                print(row)
                print(e)
                print("--------------------------End----------------------------")
        try:
            #print("Current Message: {}".format(messages))
            # PUT messages
            stream_client.put_messages(streaming_ocid, messages)
            # Clear List
            put_messages_details.clear()
        except Exception as e:
            print("----------Failed to put the stream_client message -------------------")
            print(e)
            print("--------------------------End----------------------------")

    print("All messages have been pushed at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))

def rename_object(namespace, bucket_name, object_storage , object_name):
    # Rename the object first
    new_object_name = object_name + '-Done'
    rename_object_details = oci.object_storage.models.RenameObjectDetails(source_name=object_name, new_name=new_object_name)
    object_storage.rename_object(namespace, bucket_name, rename_object_details)

def conversionCSVtoJSON(object):
    try:
        source_data = []
        reader = csv.DictReader(io.StringIO(object.data.text))
        for dict in reader:
            for item in dict:
                if str(item).lower() == "serialnum":
                    dict[item] = int(float(dict[item]))
            source_data.append(dict)
        print(source_data)
        json_data = json.dumps(source_data)
        print(json_data)
        data = json.loads(json_data)
        #print(json_data)
    except Exception as e:
        print("----------------- Error while converting from CSV to JSON -------------------")
        print(e)
        print("--------------------------------------End------------------------------------")
    return data

def do(signer, bucket_name):
    try:
        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
        namespace = object_storage.get_namespace().data

        # Get Compartment ocid
        compartment_id = os.environ.get("OCI_COMPARTMENT")
        # Get Stream Name
        stream_name = os.environ.get("STREAM_NAME")
        # Create StreamAdminClient / StreamClient
        stream_admin_client = oci.streaming.StreamAdminClient(config={}, signer=signer)
        stream_client = oci.streaming.StreamClient(config={}, signer=signer)
        streaming_summary = get_streaming_ocid(stream_admin_client, stream_name, compartment_id)

        streaming_ocid = streaming_summary[0]
        stream_service_endpoint = streaming_summary[1]
        #stream_pool_id = streaming_summary[2]
        #print(streaming_ocid,stream_service_endpoint,stream_pool_id)

        #Get object name
        all_objects = object_storage.list_objects(namespace, bucket_name).data

        for new in range(len(all_objects.objects)):
            object_name = all_objects.objects[new].name
            print(object_name)
            # Get object name and contents of object
            object = object_storage.get_object(namespace, bucket_name, object_name)
            if str(object_name).lower()[-4:] == 'json':
                source_data=json.loads(object.data.text)
                print(source_data)
                # Rename the object first
                rename_object(namespace, bucket_name, object_storage, object_name)
                # Kafka sink connector schema and payload structure
                kafka_connector = kafka_structure(source_data)
                # Put message into Streaming
                put_messages_streaming(stream_client, streaming_ocid, kafka_connector)
            elif str(object_name).lower()[-3:] == 'csv':
                data = conversionCSVtoJSON(object)
                # Rename the object first
                rename_object(namespace, bucket_name, object_storage, object_name)
                # Kafka sink connector schema and payload structure
                kafka_connector = kafka_structure(data)
                # Put message into Streaming
                put_messages_streaming(stream_client, streaming_ocid, kafka_connector)
            else:
                print(object_name + " is neither a json nor csv. Please delete and re-upload a file as json format")
                continue

    except Exception as e:
        message = "Failed: " + str(e.message)
        print(message)
