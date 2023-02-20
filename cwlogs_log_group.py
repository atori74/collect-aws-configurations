import copy
from typing import Any, Dict, List

import boto3
import pandas as pd


logs = boto3.client('logs')


def collect_configurations_of_log_group(log_group_name: str) -> Dict[str, Any]:
    res = logs.describe_log_groups(logGroupNamePrefix=log_group_name)
    if len(res['logGroups']) != 1:
        print(f'Cannot find the unique log group: {log_group_name}')
        return
    info = res['logGroups'][0]

    log_stream_names = [
        s['logStreamName']
        for s in logs.describe_log_streams(
            logGroupName=info['logGroupName']
        )['logStreams']
    ]
    subscription_filter_names = [
        s['filterName']
        for s in logs.describe_subscription_filters(
            logGroupName=info['logGroupName']
        )['subscriptionFilters']
    ]
    tags = logs.list_tags_log_group(logGroupName=info['logGroupName'])['tags']

    info['subscriptionFilters'] = '\n'.join(subscription_filter_names)
    info['logStreams'] = '\n'.join(log_stream_names)
    info['kmsKeyId'] = info.get('kmsKeyId', '')
    info['tags'] = str(tags)
    info['tagName'] = tags.get('Name', '')
    info['tagEnv'] = tags.get('Env', '')

    return info


def main():
    list_of_info: List[Dict] = []

    log_groups = logs.describe_log_groups()
    while True:
        for log_group in log_groups['logGroups']:
            info = collect_configurations_of_log_group(
                log_group['logGroupName']
            )
            if info is None:
                continue
            list_of_info.append(info)

        if log_group.get('nextToken') is None:
            break
        log_groups = logs.describe_log_groups(
            nextToken=log_groups['nextToken']
        )

    df = pd.DataFrame(list_of_info)
    df.to_csv('cwlogs_loggroup_result.csv')


if __name__ == '__main__':
    main()
