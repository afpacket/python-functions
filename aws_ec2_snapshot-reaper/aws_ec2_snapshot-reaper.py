#!/usr/bin/env python3

import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dateutil.tz import tzutc

dry_run = True

retention_keys = ['do_not_delete', 'keep', 'retain', 'retention']
snapshot_retention_days = 30
true_values = ['true', 'yes', '1']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    regions = ['us-east-2', 'us-west-2']

    # Append errors to this list and display at the end
    errors = []

    for region in regions:
        ec2_client = boto3.client('ec2', region_name=region)

        reap_snapshots(ec2_client, region, errors)

    if errors:
        print("Errors:",'\n')
        for error in errors:
            print(error)
        raise

    return

def reap_snapshots(ec2_client, region, errors):
    """Reap snapshots with timestamp older than snapshot_retention_days 
    and without retention tag key-value pair"""

    response = ec2_client.describe_snapshots(MaxResults=150,OwnerIds=['self'])
    snapshots = response['Snapshots']
    retention_tag_value = ''

    for snapshot in snapshots:

        now = datetime.now(tzutc())
        snapshot_timestamp = snapshot['StartTime'].replace(tzinfo=tzutc())

        if (now - snapshot_timestamp) > timedelta(snapshot_retention_days):
            try: 
                # Get snapshot tag key for retention if it exists
                for tag in snapshot['Tags']:
                    if tag['Key'].lower() in retention_keys:
                        retention_tag_value = tag.get('Value')

                # Skip snapshot if retention tag is True
                if retention_tag_value.lower() in true_values:
                    break

                print(f"Deleting snapshot-id: { snapshot['SnapshotId'] }, region: { region }, timestamp: { snapshot['StartTime'] }")
                ec2_client.delete_snapshot(SnapshotId=snapshot['SnapshotId'],DryRun=dry_run)

            except KeyError as error:
                # KeyError occurs if the snapshot is completely untagged
                try:
                    print(f"Deleting snapshot-id: { snapshot['SnapshotId'] }, region: { region }, timestamp: { snapshot['StartTime'] }")
                    ec2_client.delete_snapshot(SnapshotId=snapshot['SnapshotId'],DryRun=dry_run)
                except ClientError as error:
                    if 'DryRunOperation' not in str(error):
                        errors.append(f"ClientError: { snapshot['SnapshotId'] }: { error }")

            except ClientError as error:
                if 'DryRunOperation' not in str(error):
                    errors.append(f"ClientError: { snapshot['SnapshotId'] }: { error }")
            
if __name__ == "__main__":
   event = {'hello': 'world'}
   lambda_handler(event, {})
