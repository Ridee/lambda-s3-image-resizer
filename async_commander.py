# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import asyncio
import json
import signal

import boto3
import pandas
import uvloop
from cloud.aws import AsyncioBotocore

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

s3_client = boto3.resource('s3')
lambda_client = boto3.client('lambda')
async_lambda_client = AsyncioBotocore('lambda', region_name='eu-west-1')

not_converted = pandas.DataFrame([
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
    {'key': 'test'},
])


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


async def invoke_lambda(event, num):
    print('hi')
    future = asyncio.ensure_future(async_lambda_client.invoke(
        FunctionName='S3-Image-Resizer-jinn-images',
        InvocationType='Event',
        Payload=json.dumps(event)
    ))
    asyncio.wait(future)
    print('Executed', num, '/', len(not_converted))


async def resize_all(not_converted):
    call_list = []
    for num, pic in enumerate(not_converted.key):
        event = get_object_event(key=pic)
        call = invoke_lambda(event, num)
        future = asyncio.ensure_future(call)
        call_list.append(future)
        # lambda_client.invoke(
        #     FunctionName='S3-Image-Resizer-jinn-images',
        #     InvocationType='Event',
        #     Payload=json.dumps(event)
        # )
    asyncio.wait(call_list)
    return


def exception_handler(*args, **kwargs):
    print(args, kwargs)


loop.set_exception_handler(exception_handler)
loop.add_signal_handler(signal.SIGINT, exception_handler)
loop.set_debug(True)

loop.run_until_complete(resize_all(not_converted))
