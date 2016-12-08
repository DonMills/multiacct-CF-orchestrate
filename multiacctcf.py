#!/usr/bin/python
from __future__ import print_function

import threading
import boto3
import botocore
import argparse
from time import ctime
###############
# Some Global Vars
##############
lock = threading.Lock()

awsaccts = [{'acct': 'acct1ID',
             'name': 'master',
             'cffile': 'location of cloudformation file in S3'},
            {'acct': 'acct2ID',
             'name': 'dev',
             'cffile': 'location of cloudformation file in S3'},
            {'acct': 'acct3ID',
             'name': 'staging',
             'cffile': 'location of cloudformation file in S3'},
            {'acct': 'acct4ID',
             'name': 'test',
             'cffile': 'location of cloudformation file in S3'},
            {'acct': 'acct5ID',
             'name': 'QA',
             'cffile': 'location of cloudformation file in S3'}]
###################################
# This results dict is prepopulated with the info for the master vpc in a region.  It will be overwritten
# if the master cloudform is run
###################################
results = {
    'master': {
        'CIDRblock': '172.0.1.0/22',
        'RTBint': [
            'rtb-xxxxxxxx',
            'rtb-xxxxxxxx'],
        'VPCID': 'vpc-xxxxxxxx'}}
threads = []

#######################
# The function that does CloudFormation and peering requests
#######################


def run_cloudform(acct, acctname, region, cffile, nopeer, results):
    ################
    # Don't like these, but necessary due to scoping
    ###############
    cfgood = None
    ismaster = None
    cidrblock = None
    vpcid = None
    rtbid = None
    rtb_inta = None
    rtb_intb = None

    threadname = threading.current_thread().name
    if acctname == "master":
        ismaster = True
###################
# If we are running in master, we don't need sts creds
###################
    if ismaster:
        try:
            cf = boto3.client('cloudformation',
                              region_name=region)
            validate = cf.validate_template(
                TemplateURL=cffile
            )
            cfgood = True
            print(
                "[%s] %s CloudFormation file %s validated successfully for account %s" %
                (ctime(), threadname, cffile, acctname))
        except botocore.exceptions.ClientError as e:
            print(
                "[%s] %s CloudFormation file %s validation failed for account %s with error: %s" %
                (ctime(), threadname, cffile, acctname, e))
            cfgood = False
###################
# Otherwise, we do.
###################
    else:
        with lock:
            print(
                "[%s] %s is assuming STS role for account %s" %
                (ctime(), threadname, acctname))
        try:
            with lock:
                sts = boto3.client('sts')
                role = sts.assume_role(
                    RoleArn='arn:aws:iam::' + acct + ':role/MasterAcctRole',
                    RoleSessionName='STSTest',
                    DurationSeconds=900
                )
                accesskey = role["Credentials"]["AccessKeyId"]
                secretkey = role["Credentials"]["SecretAccessKey"]
                sessiontoken = role["Credentials"]["SessionToken"]
                print(
                    "[%s] %s successfully assumed STS role for account %s" %
                    (ctime(), threadname, acctname))
        except botocore.exceptions.ClientError as e:
            with lock:
                print(
                    "[%s] %s failed to assume role for account %s with error: %s" %
                    (ctime(), threadname, acctname, e))
        with lock:
            print(
                "[%s] %s is verifying CloudFormation file %s for account %s" %
                (ctime(), threadname, cffile, acctname))
        try:
            cf = boto3.client('cloudformation',
                              aws_access_key_id=accesskey,
                              aws_secret_access_key=secretkey,
                              aws_session_token=sessiontoken,
                              region_name=region)
            validate = cf.validate_template(
                TemplateURL=cffile
            )
            cfgood = True
            with lock:
                print(
                    "[%s] %s CloudFormation file %s validated successfully for account %s" %
                    (ctime(), threadname, cffile, acctname))
        except botocore.exceptions.ClientError as e:
            with lock:
                print(
                    "[%s] %s CloudFormation file %s validation failed for account %s with error: %s" %
                    (ctime(), threadname, cffile, acctname, e))
                cfgood = False
##########################
# Ok the CF should be validated (cfgood=True), so let's run it.
#########################
    if cfgood:
        with lock:
            print(
                "[%s] %s Preparing to run CloudFormation file %s in account %s" %
                (ctime(), threadname, cffile, acctname))
        stackid = cf.create_stack(
            StackName=region + "-" + acctname,
            TemplateURL=cffile,
            Parameters=[
                {
                },
            ],
            Tags=[
                {
                    'Key': 'Purpose',
                    'Value': 'Infrastructure'
                },
            ]
        )['StackId']
        with lock:
            print("[%s] %s StackID %s is running in account %s" %
                  (ctime(), threadname, stackid, acctname))
        waiter = cf.get_waiter('stack_create_complete')
        waiter.wait(StackName=stackid)
        with lock:
            print(
                "[%s] %s StackID %s completed creation in account %s" %
                (ctime(), threadname, stackid, acctname))
        stack = cf.describe_stacks(StackName=stackid)
        for item in stack['Stacks'][0]['Outputs']:
            if item['OutputKey'] == "VPCId":
                vpcid = item["OutputValue"]
            elif item['OutputKey'] == "VPCCIDRBlock":
                cidrblock = item["OutputValue"]
            elif item['OutputKey'] == "RouteTableId":
                rtbid = item["OutputValue"]
            elif item['OutputKey'] == "InternalRouteTableA":
                rtbid_inta = item["OutputValue"]
            elif item['OutputKey'] == "InternalRouteTableB":
                rtbid_intb = item["OutputValue"]
        pcxid = "None"
###########################
# Don't do peering if we are master vpc or if nopeer is set via cli
# otherwise, this is the peering code
##########################
        if not ismaster and not nopeer:
            with lock:
                print(
                    "[%s] %s Preparing to request peering with Master vpc in account %s" %
                    (ctime(), threadname, acctname))
            try:
                ec2 = boto3.client('ec2',
                                   aws_access_key_id=accesskey,
                                   aws_secret_access_key=secretkey,
                                   aws_session_token=sessiontoken,
                                   region_name=region)
                pcx = ec2.create_vpc_peering_connection(
                    VpcId=vpcid,
                    PeerVpcId=results['master']['VPCID'],
                    PeerOwnerId='masteracctID'
                )
                pcxid = pcx['VpcPeeringConnection']['VpcPeeringConnectionId']
                with lock:
                    print(
                        "[%s] %s Peering Connection request ID %s sent from account %s" %
                        (ctime(), threadname, pcxid, acctname))
                    print(
                        "[%s] %s Preparing to add route to table %s to Peer Connection ID %s in account %s" %
                        (ctime(), threadname, rtbid, pcxid, acctname))
                route = ec2.create_route(
                    DestinationCidrBlock=results['master']['CIDRblock'],
                    VpcPeeringConnectionId=pcxid,
                    RouteTableId=rtbid
                )
                if route['Return']:
                    print(
                        "[%s] Route added to route table %s for network %s to peer connection %s in account %s" %
                        (ctime(), rtbid, results['master']['CIDRblock'], pcxid, acctname))
                else:
                    print(
                        "[%s] Failed adding to route table %s for network %s to peer connection %s in account %s" %
                        (ctime(), rtbid, results['master']['CIDRblock'], pcxid, acctname))
            except botocore.exceptions.ClientError as e:
                with lock:
                    print(
                        "[%s] %s Peering Connection request failed for account %s with error: %s" %
                        (ctime(), threadname, acctname, e))

        results[acctname] = {
            "CIDRblock": cidrblock,
            "VPCID": vpcid,
            "PCXID": pcxid}
############################
# master results need the route table ids of both internal tables to add routes to both
###########################
        if ismaster:
            results[acctname].update({'RTBint': [rtbid_inta, rtbid_intb]})


def printdata(results, acctname):
    print(
        "The CIDRBlock for VPC %s in account %s is %s.  The VPC peering id is %s" %
        (results[acctname]['VPCID'],
         acctname,
         results[acctname]['CIDRblock'],
         results[acctname]['PCXID']))


def printdatamaster(results):
    print(
        "The CIDRBlock for VPC %s in master account is %s.  The internal route table ids are %s and %s" %
        (results['master']['VPCID'],
         results['master']['CIDRblock'],
         results['master']['RTBint'][0],
         results['master']['RTBint'][1]))


def main():
    #############################
    # Parse CLI options - setup the parser
    ############################
    parser = argparse.ArgumentParser(
        description='An orchestration script that runs multi-account CloudFormation and can set up peering relationships between the VPCs created')
    parser.add_argument(
        "region",
        type=str,
        choices=[
            "us-west-2",
            "us-east-1"],
        help="The AWS Region you would like to operate in")
    parser.add_argument(
        "-sa",
        "--single_account",
        action='append',
        help="Provide a single account name(dev,hdp,test,beps) and only operate on that account.  You can perform this action multiple times to operate on more than one account.")
    parser.add_argument(
        "-np",
        "--no_peering",
        action='store_true',
        dest='no_peering',
        help="Run the CloudFormation, but don't do the inter-VPC peering")
    #################################
    # Parse CLI options - read the parser
    #################################
    nopeer = None

    args = parser.parse_args()
    region = args.region
    acct = args.single_account
    if args.no_peering:
        nopeer = True
############################
# Do single account or multiple single account runs
############################
    if acct:
        for line in acct:
            foundacct = None
            print(
                "[%s] Single account selected: Preparing to run CloudFormation on %s account" %
                (ctime(), line))
            print("[%s] Preparing to spawn thread" % ctime())
            for entry in awsaccts:
                if entry['name'] == line:
                    t = threading.Thread(
                        target=run_cloudform,
                        args=(
                            entry['acct'],
                            entry['name'],
                            region,
                            entry['cffile'],
                            nopeer,
                            results))
                    threads.append(t)
                    t.start()
                    foundacct = True
            if not foundacct:
                print("[%s] No matching account name found!" % ctime())
                print("[%s] Current configured accounts are:" % ctime())
                for entry in awsaccts:
                    print(
                        "[%s] Account ID: %s Account Name: %s" %
                        (ctime(), entry['acct'], entry['name']))
        for i in range(len(threads)):
            threads[i].join()
#############################
# Or run the whole shebang
#############################
    else:
        print(
            "[%s] Preparing to run CloudFormation across all AWS accounts" %
            ctime())
        print("[%s] Preparing to run Master account CloudFormation" % ctime())
        masteracct = list(
            (entry for entry in awsaccts if entry['name'] == 'master'))[0]
        run_cloudform(
            masteracct['acct'],
            masteracct['name'],
            region,
            masteracct['cffile'],
            nopeer,
            results)
        printdatamaster(results)
        print("[%s] Preparing to spawn threads" % ctime())
        subaccts = (entry for entry in awsaccts if entry['name'] != 'master')
##############################
# do the threading for subaccts
#############################
        for entry in subaccts:
            t = threading.Thread(
                target=run_cloudform,
                args=(
                    entry['acct'],
                    entry['name'],
                    region,
                    entry['cffile'],
                    nopeer,
                    results))
            threads.append(t)
            t.start()
        for i in range(len(threads)):
            threads[i].join()
    print("[%s] All CloudFormations run!" % ctime())
    if len(results) > 1:
        print("[%s] Printing outputs:" % ctime())
        for entry in (entry for entry in results if entry != 'master'):
            printdata(results, entry)
###############################
# Accept peering and add final routes to peering vpcs
##############################
        if not nopeer and len(results) > 1:
            print(
                "[%s] Attempting to accept peering requests in Master" %
                ctime())
            try:
                master = boto3.client('ec2',
                                      region_name=region)
                subaccts = (entry for entry in results if entry != "master")
                for entry in subaccts:
                    pcx = master.accept_vpc_peering_connection(
                        VpcPeeringConnectionId=results[entry]['PCXID']
                    )
                    print(
                        "[%s] VPC Peering connection from %s with ID %s is status: %s" %
                        (ctime(),
                         entry,
                         results[entry]['PCXID'],
                            pcx['VpcPeeringConnection']['Status']['Code']))
                    for table in results['master']['RTBint']:
                        route = master.create_route(
                            DestinationCidrBlock=results[entry]['CIDRblock'],
                            VpcPeeringConnectionId=results[entry]['PCXID'],
                            RouteTableId=table
                        )
                        if route['Return']:
                            print(
                                "[%s] Route added to Master route table %s for network %s to peer connection %s" %
                                (ctime(), table, results[entry]['CIDRblock'], results[entry]['PCXID']))
                        else:
                            print(
                                "[%s] Adding route to Master route table %s for network %s to peer connection %s failed!" %
                                (ctime(), table, results[entry]['CIDRblock'], results[entry]['PCXID']))

            except botocore.exceptions.ClientError as e:
                print(
                    "[%s] Failed to manipulate account %s with error: %s" %
                    (ctime(), "Master", e))

    print("[%s] Finished" % ctime())

if __name__ == '__main__':
    main()
