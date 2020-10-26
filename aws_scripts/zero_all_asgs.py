'''
 This script will set the max size, min size, and desired capacity values for all
 auto-scaling groups to zero then wait until all instances have been terminated 
 after which these initial values will be restored. A list of asg's with active
 instances is printed to the console until no active instances are detected.

 There is also an option to read in an asg config file that will assign the
 max, min, and desired capacity values. The format for this file is:

 
 Flags:
 -f: input asg config file
 -p <int>: specifies interval over which to print ASG's with active instances.
           If not specified, default is '2'.

 Example usage:  python zero_all_asgs.py -p 3
'''

import boto3
import json
import time
import argparse

# AWS ASG parameters
MAX_SIZE = "MaxSize"
MIN_SIZE = "MinSize"
DESIRED_CAPACITY = "DesiredCapacity"

client = boto3.client('autoscaling')

'''
Stores maxsize, minsize, and desired capacity values to a config
file then sets these ASG parameters to zero.
'''
def set_asgs_to_zero():
    f = open("asg_config.json", "w")
    data = {}

    response = client.describe_auto_scaling_groups()
    asgs = response['AutoScalingGroups']

    for asg in asgs:
        # Get asg name
        asg_name = asg['AutoScalingGroupName']
        print("\n" + asg_name)        
        data[asg_name] = []

        # Collect initial asg values then, if desired capacity, max, or min are non-zero, set to zero
        if (asg['MinSize'] is not 0):
            data[asg_name].append({'MinSize':asg['MinSize']})
            client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MinSize=0) 
            print("...setting MinSize to 0")

        if (asg['MaxSize'] is not 0):
            data[asg_name].append({'MaxSize':asg['MaxSize']})
            client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MaxSize=0)
            print("...setting MaxSize to 0")

        if (asg['DesiredCapacity'] is not 0):
            #data[asg_name].append({'DesiredCapacity':asg['DesiredCapacity']})
            client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=0)
            print("...setting DesiredCapacity to 0")

    # Write initial asg values to config file
    json.dump(data, f)
    f.close()

'''
Print list of ASG's with active instances until all instances have been terminated.
'''
def wait_for_asgs_to_zero(period):
    while(True):
        response = client.describe_auto_scaling_instances()
        asg_set = set()

        if (len(response['AutoScalingInstances']) <= 0):
            print("\n***********************************************")
            print("\nAll asg's zero'd. No active instances detected.")
            print("\n***********************************************")
            break;

        print("\nWaiting for active instances from the following ASG's to shutdown:")
        for i in response['AutoScalingInstances']:
            asg_set.add(i['AutoScalingGroupName'])
        print('\n'.join(asg_set))
        print("Total number of active instances: " + str(len(response['AutoScalingInstances'])))        
        time.sleep(int(period))

'''
Sets maxsize, minsize, and desired capacity back to initial values once all instances
have been terminated.
'''
def set_asgs_to_defaults():
    # Open file containing default asg values
    f = open("asg_config.json", "r")
    data = json.load(f)

    for asg in data.items():
        param = tuple(asg)[1]
        asg_name = tuple(asg)[0]
        print("\n" + asg_name)

        # Set asg default values
        if (len(param) > 0):
            for obj in param:  
                if ("MaxSize" in obj):
                    maxSize = obj["MaxSize"]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MaxSize=maxSize)       
                    print("Set MaxSize to", maxSize)
             
                '''
                if (DESIRED_CAPACITY in obj):
                    desiredCapacity = obj[DESIRED_CAPACITY]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=desiredCapacity)
                    print("Set DesiredCapacity to", desiredCapacity)
                '''

                if (MIN_SIZE in obj):
                    minSize = obj[MIN_SIZE]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MinSize=minSize)
                    print("Set MinSize to", minSize)
                  
'''
Sets asg min, max, and/or desired capacity values from config
'''    
def set_asg_from_config(config):
    print("Setting config values...")
    f = open(config, "r")
    data = json.load(f)

    for asg in data.items():
        param = tuple(asg)[1]
        asg_name = tuple(asg)[0]
        print("\n" + asg_name)

        # Set asg default values
        if (len(param) > 0):
            for obj in param:
                if ("MaxSize" in obj):
                    maxSize = obj["MaxSize"]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MaxSize=maxSize)
                    print("Set MaxSize to", maxSize)

                if (DESIRED_CAPACITY in obj):
                    desiredCapacity = obj[DESIRED_CAPACITY]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, DesiredCapacity=desiredCapacity)
                    print("Set DesiredCapacity to", desiredCapacity)

                if (MIN_SIZE in obj):
                    minSize = obj[MIN_SIZE]
                    client.update_auto_scaling_group(AutoScalingGroupName=asg_name, MinSize=minSize)
                    print("Set MinSize to", minSize)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--period", default=2, help="Interval in seconds to print list of ASG's with active instances.")
    parser.add_argument("-f", "--f", help="Input asg config file containing desired values.")
    args = parser.parse_args()

    if (args.f is not None):
        # Set asg values to those specified in config
        set_asg_from_config(args.f)
    else:
        set_asgs_to_zero()
        wait_for_asgs_to_zero(args.period)
        set_asgs_to_defaults()
