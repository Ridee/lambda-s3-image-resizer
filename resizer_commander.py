# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import asyncio
import concurrent.futures
import json
import time

import boto3
import pandas
import uvloop
from cloud.aws import AsyncioBotocore

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
async_lambda_client = AsyncioBotocore('lambda', region_name='eu-west-1')

jinn_images_bucket = s3_resource.Bucket('jinn-images')


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_types_for_keys_in_chunks(keys):
    s3_client = boto3.client('s3')
    type_list = []
    for key in keys:
        response = s3_client.head_object(Bucket='jinn-images', Key=key)
        type_list.append({'content_type': response['ContentType'], 'key': key})
    return type_list


def get_types_from_keys(keys):
    types_list = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(get_types_for_keys_in_chunks, key_chunk) for key_chunk in chunks(keys, 1000)}
        for num, future in enumerate(concurrent.futures.as_completed(futures)):
            print('Done getting types for', num, '/', len(futures))
            body = future.result()
            types_list.extend(body)
    return pandas.DataFrame(types_list)


images = []
# Easy switch
if True:
    for image in jinn_images_bucket.objects.all():
        image_type = s3_client.head_object(Bucket='jinn-images', Key=image.key)
        images.append({'key': image.key, 'date': image.last_modified, 'size': image.size})
    all_files = pandas.DataFrame(images)

    types_df = get_types_from_keys(all_files.key.values)

    all_files = all_files.merge(types_df, on=['key'], how='outer')

    all_files.to_pickle('all_files-{}.pickle'.format(time.time()))
else:
    all_files = pandas.read_pickle('all_files-1479331018.954418.pickle')
exit()

df = all_files[-all_files.key.str.contains('/')]
orig_df = df[df.key.str.startswith('orig-')]
converted_df = df[-df.key.str.startswith('orig-')]
orig_df = pandas.concat([orig_df.rename(columns={'key': 'orig_key'}), orig_df.key.str.replace('orig-', '')], axis=1)
image_list = pandas.merge(
    left=converted_df,
    right=orig_df,
    on=['key'],
    how='outer',
    suffixes=('_converted', '_original')
)
# Here we have a problem

jpeg_list = image_list[
    (image_list.content_type_converted == 'image/jpeg')
    |
    (image_list.content_type_original == 'image/jpeg')
    ]

print('Total:', len(df))
print('Total, coalescing converted:', len(image_list))
print('JPEG:', len(jpeg_list))
print('Non JPEG:', len(image_list) - len(jpeg_list))
print('Converted:', len(jpeg_list[(-jpeg_list.size_converted.isnull()) & (-jpeg_list.size_original.isnull())]))
print('JPEG size converted > original:',
      len(jpeg_list[jpeg_list.size_converted > jpeg_list.size_original])
      )
print('JPEG size converted == original:',
      len(jpeg_list[jpeg_list.size_original == jpeg_list.size_converted])
      )
print('JPEG size converted < original:',
      len(jpeg_list[jpeg_list.size_converted <= jpeg_list.size_original])
      )
print('JPEG only renamed:',
      len(jpeg_list[jpeg_list.size_converted.isnull()])
      )
not_converted = jpeg_list[jpeg_list.size_original.isnull()]
print('Not converted:', not_converted)


def get_object_event(key):
    event = {'Records': [{
        "eventVersion": "2.0",
        "eventTime": "1970-01-01T00:00:00.000Z",
        "requestParameters": {
            "sourceIPAddress": "127.0.0.1"
        },
        "s3": {
            "configurationId": "testConfigRule",
            "object": {
                "eTag": "0123456789abcdef0123456789abcdef",
                "sequencer": "0A1B2C3D4E5F678901",
                "key": key,
                "size": 1024
            },
            "bucket": {
                "arn": "arn:aws:s3:::jinn-images",
                "name": "jinn-images",
                "ownerIdentity": {
                    "principalId": "joseba"
                }
            },
            "s3SchemaVersion": "1.0"
        },
        "responseElements": {
            "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
            "x-amz-request-id": "EXAMPLE123456789"
        },
        "awsRegion": "eu-west-1",
        "eventName": "ObjectCreated:Put",
        "userIdentity": {
            "principalId": "EXAMPLE"
        },
        "eventSource": "aws:s3"
    }]}
    return event


pic_list = list(not_converted.key)[1000:]


def resize_all(start, ids):
    lambda_client = boto3.client('lambda')
    for num, pic in enumerate(ids, start=start):
        event = get_object_event(key=pic)
        lambda_client.invoke(
            FunctionName='S3-Image-Resizer-jinn-images',
            InvocationType='Event',
            Payload=json.dumps(event)
        )
        print('Invoked', num, pic)
    print('Run resize', start, '/', len(pic_list))
    return


def exception_handler(*args, **kwargs):
    print(args, kwargs)


with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(resize_all, start=num * 1000, ids=chunk) for num, chunk in
               enumerate(chunks(pic_list, 1000))}
    for future in concurrent.futures.as_completed(futures):
        print('Done with future')
