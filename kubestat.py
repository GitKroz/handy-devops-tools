#!/usr/bin/env python3

from typing import TypeVar, Dict, List, Optional, Union
import logging
import argparse
import sys
import traceback

import json
import csv
import copy
import re
import subprocess

################################################################################
# Constants, global variables, types
################################################################################
# Type
JSON = TypeVar('JSON', Dict, List)

# Global variables
logger: Optional[logging.Logger] = None  # Will be filled in setup_logging()
args: Optional[argparse.Namespace] = None  # Will be filled in parse_args()


################################################################################
# Classes
################################################################################

class ContainerListItem:
    fields: Dict = {}

    appName_width: int = 0
    podName_width: int = 0
    containerName_width: int = 0

    containerMemoryLimits_width: int = 0
    containerMemoryRequests_width: int = 0
    containerCPULimits_width: int = 0
    containerCPURequests_width: int = 0

    containerPVCList_width: int = 0
    containerPVCQuantity_width: int = 0
    containerPVCRequests_width: int = 0

    def __init__(self, values: Optional[Dict] = None):
        if values is None:
            values = {}

        self.reset()

        for key in self.fields.keys():
            if key in values:
                self.fields[key] = values[key]

    def reset(self):
        self.fields: Dict = {
            "containerKey": "",
            "podKey": "",

            "podGlobalIndex": 0,  # int - global numeration of containers
            "appIndex": "",  # str - index within Application; is a suffix in the beginning

            "workloadType": "",  # DaemonSet, ReplicaSet, StatefulSet, Job
            "appName": "",  # Can be viewed as pod name without suffixes
            "podName": "",
            "containerType": "",  # init, reg
            "containerName": "",

            "containerCPURequests": 0,  # int, milliCore
            "containerCPULimits": 0,  # int, milliCore
            "containerMemoryRequests": 0,  # int, bytes
            "containerMemoryLimits": 0,  # int, bytes

            "containerPVCList": set(),  # List of strings
            "containerPVCQuantity": 0,  # int
            "containerPVCRequests": 0,  # int, bytes

            "change": "Unchanged",  # Unchanged, Deleted Pod, Deleted Container, New Pod, New Container, Modified

            "ref_containerCPURequests": 0,  # int, milliCore
            "ref_containerCPULimits": 0,  # int, milliCore
            "ref_containerMemoryRequests": 0,  # int, bytes
            "ref_containerMemoryLimits": 0,  # int, bytes

            "ref_containerPVCList": set(),  # List of strings
            "ref_containerPVCQuantity": 0,  # int
            "ref_containerPVCRequests": 0,  # int, bytes
        }

    def make_keys(self):
        self.fields['podKey'] = self.fields['appName'] + '/' + self.fields['appIndex']
        self.fields['containerKey'] = self.fields['appName'] + '/' + self.fields['appIndex'] + '/' + self.fields['containerName']

    def set_appIndex(self, appIndex: str):
        self.fields['appIndex'] = appIndex
        self.make_keys()

    def has_pod(self) -> bool:
        return self.fields["podName"] != ""

    def has_container(self) -> bool:
        return self.fields["containerName"] != ""

    def is_decoration(self) -> bool:  # Header, Line etc
        return type(self.fields["podGlobalIndex"]) is not int and self.fields["podGlobalIndex"] != ""

    def is_same_pod(self, container, trust_pod_key: bool = True) -> bool:
        if trust_pod_key:
            return container is not None and self.fields["podKey"] == container.fields["podKey"]
        else:
            # To be used in functions when key is being generated
            return container is not None and self.fields["podName"] == container.fields["podName"]

    def is_same_app(self, container) -> bool:
        return container is not None and self.fields["appName"] == container.fields["appName"]

    def is_deleted(self) -> bool:
        return self.fields['change'] in ['Deleted Pod', 'Deleted Container']

    def check_if_modified(self):
        for res_field in ['containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits', 'containerPVCList', 'containerPVCRequests']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'

    def get_formatted_fields(self, raw_units: bool) -> Dict:
        formatted_fields = copy.deepcopy(self.fields)

        # Make human-readable values
        if not self.is_decoration():
            formatted_fields["containerCPURequests"] = res_cpu_millicores_to_str(formatted_fields["containerCPURequests"], raw_units)
            formatted_fields["containerCPULimits"] = res_cpu_millicores_to_str(formatted_fields["containerCPULimits"], raw_units)

            formatted_fields["containerMemoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["containerMemoryRequests"], raw_units)
            formatted_fields["containerMemoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["containerMemoryLimits"], raw_units)

            formatted_fields["containerPVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["containerPVCRequests"], raw_units)

            formatted_fields["ref_containerCPURequests"] = res_cpu_millicores_to_str(formatted_fields["ref_containerCPURequests"], raw_units)
            formatted_fields["ref_containerCPULimits"] = res_cpu_millicores_to_str(formatted_fields["ref_containerCPULimits"], raw_units)

            formatted_fields["ref_containerMemoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_containerMemoryRequests"], raw_units)
            formatted_fields["ref_containerMemoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["ref_containerMemoryLimits"], raw_units)

            formatted_fields["ref_containerPVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_containerPVCRequests"], raw_units)

        return formatted_fields

    def decorate_changes(self, template: str, is_pod_only: bool) -> str:
        r = template

        if self.is_decoration():
            pass
        elif self.fields['change'] == "Unchanged":
            if is_pod_only:
                r = '\033[1;37m' + template + '\033[0m'
            else:
                pass
        elif self.fields['change'] == "Deleted Pod":
            if is_pod_only:
                r = '\033[1;31m' + template + '\033[0m'
            else:
                r = '\033[0;31m' + template + '\033[0m'
        elif self.fields['change'] == "Deleted Container":
            if is_pod_only:
                r = '\033[1;37m' + template + '\033[0m'
            else:
                r = '\033[0;31m' + template + '\033[0m'
        elif self.fields['change'] == "New Pod":
            if is_pod_only:
                r = '\033[1;32m' + template + '\033[0m'
            else:
                r = '\033[0;32m' + template + '\033[0m'
        elif self.fields['change'] == "New Container":
            if is_pod_only:
                r = '\033[1;37m' + template + '\033[0m'
            else:
                r = '\033[0;32m' + template + '\033[0m'
        elif self.fields['change'] == "Modified":
            if is_pod_only:
                r = '\033[1;37m' + template + '\033[0m'
            else:
                r = '\033[0;93m' + template + '\033[0m'
        else:
            raise RuntimeError("Invalid change for pod '{}': {}".format(self.fields["podName"], self.fields["change"]))

        return r

    def print_table(self, raw_units: bool, prev_container, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        # Skip application/workload values if it was contained in the previous container
        if self.is_same_app(prev_container):
            formatted_fields["workloadType"] = ""
            formatted_fields["appName"] = ""

        # Skip pod values if it was contained in the previous container
        if self.is_same_pod(prev_container):
            formatted_fields["podGlobalIndex"] = ""
            formatted_fields["podName"] = ""

        template = \
            "{appName:<" + str(ContainerListItem.appName_width + 2) + "}" + \
            "{workloadType:<13}" + \
            "{podGlobalIndex:<4}" + \
            "{podName:<" + str(ContainerListItem.podName_width + 2) + "}" + \
            "{containerType:<7}" + \
            "{containerName:<" + str(ContainerListItem.containerName_width + 2) + "}" + \
            "{containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
            "{containerPVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
            "{containerPVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        if with_changes:
            template = template + \
                       "  " + \
                       "{change:<18}" + \
                       "{ref_containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
                       "{ref_containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
                       "{ref_containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                       "{ref_containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
                       "{ref_containerPVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
                       "{ref_containerPVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        # logger.info("{}".format(self.fields))

        if not self.is_decoration():
            template = self.decorate_changes(template, is_pod_only=False)

        print(template.format(**formatted_fields))

    def get_tree_columns_width(self, with_changes: bool):
        container_indent = 6

        pod_width = 4 + 13 + (ContainerListItem.podName_width + 2)
        container_width = container_indent + (4 + 2 + 1) + (ContainerListItem.containerName_width + 2)
        item_width = max(pod_width, container_width)

        resources_width = \
            (ContainerListItem.containerCPURequests_width + 2) + \
            (ContainerListItem.containerCPULimits_width + 2) + \
            (ContainerListItem.containerMemoryRequests_width + 2) + \
            (ContainerListItem.containerMemoryLimits_width + 2) + \
            (ContainerListItem.containerPVCQuantity_width + 2) + \
            (ContainerListItem.containerPVCRequests_width + 2)

        if with_changes:
            resources_width = resources_width + \
                              2 + \
                              18 + \
                              (ContainerListItem.containerCPURequests_width + 2) + \
                              (ContainerListItem.containerCPULimits_width + 2) + \
                              (ContainerListItem.containerMemoryRequests_width + 2) + \
                              (ContainerListItem.containerMemoryLimits_width + 2) + \
                              (ContainerListItem.containerPVCQuantity_width + 2) + \
                              (ContainerListItem.containerPVCRequests_width + 2)

        return container_indent, item_width, resources_width

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        # Calculating column widths
        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)

        # pod_width = 4 + 13 + (ContainerListItem.podName_width + 2)
        # container_width = container_indent + (4 + 2 + 1) + (ContainerListItem.containerName_width + 2)
        # item_width = max(pod_width, container_width)

        podName_width = item_width - 4 - 13 - 2
        containerName_width = item_width - container_indent - (4 + 2 + 1) - 2

        # Add special line for pods
        pod_template = ""
        if not self.is_same_pod(prev_container):
            # '\033[1;37m' +\
            pod_template = \
                "{podGlobalIndex:<4}" + \
                "{workloadType:<13}" + \
                "{podName:<" + str(podName_width + 2) + "}"
            # "{appName:<" + str(ContainerListItem.appName_width + 2) + "}" + \

            pod_template = self.decorate_changes(pod_template, is_pod_only=True)
            pod_template = pod_template + '\n'

        container_template = \
            " " * container_indent + \
            "({containerType:<4}) " + \
            "{containerName:<" + str(containerName_width + 2) + "}" + \
            "{containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
            "{containerPVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
            "{containerPVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        if with_changes:
            container_template = container_template + \
                                 "  " + \
                                 "{change:<18}" + \
                                 "{ref_containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
                                 "{ref_containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
                                 "{ref_containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                                 "{ref_containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
                                 "{ref_containerPVCQuantity:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                                 "{ref_containerPVCRequests:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}"
        # " {containerKey}"

        container_template = self.decorate_changes(container_template, is_pod_only=False)

        template = pod_template + container_template

        print(template.format(**formatted_fields))

    def print_csv(self):
        csv_writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        csv_writer.writerow([
            self.fields["containerKey"],
            self.fields["podKey"],

            self.fields["appIndex"],
            self.fields["appName"],
            self.fields["workloadType"],
            self.fields["podGlobalIndex"],
            self.fields["podName"],
            self.fields["containerType"],
            self.fields["containerName"],

            self.fields["containerCPURequests"],
            self.fields["containerCPULimits"],
            self.fields["containerMemoryRequests"],
            self.fields["containerMemoryLimits"],

            self.fields["containerPVCList"],
            self.fields["containerPVCQuantity"],
            self.fields["containerPVCRequests"],

            self.fields["change"],

            self.fields["ref_containerCPURequests"],
            self.fields["ref_containerCPULimits"],
            self.fields["ref_containerMemoryRequests"],
            self.fields["ref_containerMemoryLimits"],

            self.fields["ref_containerPVCList"],
            self.fields["ref_containerPVCQuantity"],
            self.fields["ref_containerPVCRequests"]
        ])


class ContainerListLine(ContainerListItem):
    def __init__(self):
        super().__init__()

        self.fields["containerKey"] = "-" * 4
        self.fields["podKey"] = "-" * 4

        # Note: q-ty of dashes must correspond to size of fields
        self.fields["appIndex"] = "-" * 4
        self.fields["appName"] = "-" * (ContainerListItem.appName_width + 2)
        self.fields["workloadType"] = "-" * 13
        self.fields["podGlobalIndex"] = "-" * 4
        self.fields["podName"] = "-" * (ContainerListItem.podName_width + 2)
        self.fields["containerType"] = "-" * 7
        self.fields["containerName"] = "-" * (ContainerListItem.containerName_width + 2)
        self.fields["containerCPURequests"] = "-" * (ContainerListItem.containerCPURequests_width + 2)
        self.fields["containerCPULimits"] = "-" * (ContainerListItem.containerCPULimits_width + 2)
        self.fields["containerMemoryRequests"] = "-" * (ContainerListItem.containerMemoryRequests_width + 2)
        self.fields["containerMemoryLimits"] = "-" * (ContainerListItem.containerMemoryLimits_width + 2)
        self.fields["containerPVCQuantity"] = "-" * (ContainerListItem.containerMemoryRequests_width + 2)
        self.fields["containerPVCRequests"] = "-" * (ContainerListItem.containerMemoryLimits_width + 2)

        self.fields["change"] = "-" * 18

        self.fields["ref_containerCPURequests"] = "-" * (ContainerListItem.containerCPURequests_width + 2)
        self.fields["ref_containerCPULimits"] = "-" * (ContainerListItem.containerCPULimits_width + 2)
        self.fields["ref_containerMemoryRequests"] = "-" * (ContainerListItem.containerMemoryRequests_width + 2)
        self.fields["ref_containerMemoryLimits"] = "-" * (ContainerListItem.containerMemoryLimits_width + 2)
        self.fields["ref_containerPVCQuantity"] = "-" * (ContainerListItem.containerMemoryRequests_width + 2)
        self.fields["ref_containerPVCRequests"] = "-" * (ContainerListItem.containerMemoryLimits_width + 2)

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)
        print('-' * (item_width + resources_width))


class ContainerListHeader(ContainerListItem):
    def __init__(self):
        super().__init__()

        self.fields["containerKey"] = "cKey"
        self.fields["podKey"] = "pKey"

        self.fields["appIndex"] = "Cnt"
        self.fields["appName"] = "Application"
        self.fields["workloadType"] = "Workload"
        self.fields["podGlobalIndex"] = "#"
        self.fields["podName"] = "Pod"
        self.fields["containerType"] = "Type"
        self.fields["containerName"] = "Container"
        self.fields["containerCPURequests"] = "CPU_R"
        self.fields["containerCPULimits"] = "CPU_L"
        self.fields["containerMemoryRequests"] = "Mem_R"
        self.fields["containerMemoryLimits"] = "Mem_L"
        self.fields["containerPVCQuantity"] = "PVC_Q"
        self.fields["containerPVCRequests"] = "PVC_R"

        self.fields["change"] = "Change"

        self.fields["ref_containerCPURequests"] = "rCPU_R"
        self.fields["ref_containerCPULimits"] = "rCPU_L"
        self.fields["ref_containerMemoryRequests"] = "rMem_R"
        self.fields["ref_containerMemoryLimits"] = "rMem_L"
        self.fields["ref_containerPVCQuantity"] = "rPVC_Q"
        self.fields["ref_containerPVCRequests"] = "rPVC_R"

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)

        formatted_fields['item_txt'] = "Item"

        template = \
            "{item_txt:" + str(item_width) + "}" + \
            "{containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
            "{containerPVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
            "{containerPVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        if with_changes:
            template = template + \
                       "  " + \
                       "{change:<18}" + \
                       "{ref_containerCPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
                       "{ref_containerCPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
                       "{ref_containerMemoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                       "{ref_containerMemoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
                       "{ref_containerPVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
                       "{ref_containerPVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        print(template.format(**formatted_fields))


class PVCListItem:
    fields: Dict = {}

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.fields = {
            'key': '',
            'index': 0,  # int - global numeration of PVCs
            'uid': '',  # str

            'name': '',  # str - used for binding with containers
            'storageClassName': '',  # str

            'containerList': set(),  # List of strings - keys of containers using this PVC
            'containerQuantity': int,  # int, containers using this PVC

            'requests': 0  # int, bytes
        }

    def make_key(self):
        self.fields['key'] = self.fields['name']


class KubernetesResourceSet:
    containers: List[ContainerListItem] = list()
    pvcs: List[PVCListItem] = list()

    def __init__(self):
        self.reset()

    def reset(self):
        self.containers = list()
        self.pvcs = list()

    def sort(self):
        sorted_containers = sorted(self.containers, key=lambda container: container.fields['containerKey'])
        self.containers = sorted_containers

    # Important: this must NOT be run after compare, otherwise deleted pods will be identified as separate ones
    def renew_replica_indices(self):
        for container in self.containers:
            appName_len = len(container.fields['appName'])
            container.set_appIndex(container.fields['podName'][appName_len:])

        self.sort()

        replica_global_index = 1
        replica_app_index = 1

        prev_container = None
        for container in self.containers:
            if prev_container is None:
                prev_container = container

            if not container.is_same_pod(prev_container, trust_pod_key=False):
                replica_global_index = replica_global_index + 1

                replica_app_index = replica_app_index + 1
                if not container.is_same_app(prev_container):
                    replica_app_index = 0

            container.set_appIndex(str(replica_app_index).zfill(3))
            container.fields['podGlobalIndex'] = replica_global_index

            prev_container = container

    def renew_pvc_information(self):
        for container in self.containers:
            container.fields['containerPVCQuantity'] = len(container.fields['containerPVCList'])
            # TODO: PVC size

    # Note: each field in criteria is a regex
    def filter(self, criteria: ContainerListItem):
        r = KubernetesResourceSet()
        for container in self.containers:
            matches = True
            for field in ["workloadType", "podName", "containerType", "containerName"]:
                matches = matches and bool(re.search(criteria.fields[field], container.fields[field]))

            if matches:
                r.containers.append(container)

        return r

    def get_resources_total(self, with_changes: bool) -> ContainerListItem:
        r = ContainerListItem()

        pod_count = 0
        container_count = 0

        prev_container = None
        for container in self.containers:
            if container.is_deleted():  # TODO: review logic
                continue

            container_count = container_count + 1
            if not container.is_same_pod(prev_container):
                pod_count = pod_count + 1

            r.fields["containerCPURequests"] = r.fields["containerCPURequests"] + container.fields["containerCPURequests"]
            r.fields["containerCPULimits"] = r.fields["containerCPULimits"] + container.fields["containerCPULimits"]
            r.fields["containerMemoryRequests"] = r.fields["containerMemoryRequests"] + container.fields["containerMemoryRequests"]
            r.fields["containerMemoryLimits"] = r.fields["containerMemoryLimits"] + container.fields["containerMemoryLimits"]

            # TODO: Re-make, since potentially single PVC may be connected to multiple pods
            r.fields["containerPVCQuantity"] = r.fields["containerPVCQuantity"] + container.fields["containerPVCQuantity"]
            r.fields["containerPVCRequests"] = r.fields["containerPVCRequests"] + container.fields["containerPVCRequests"]

            r.fields["ref_containerCPURequests"] = r.fields["ref_containerCPURequests"] + container.fields["ref_containerCPURequests"]
            r.fields["ref_containerCPULimits"] = r.fields["ref_containerCPULimits"] + container.fields["ref_containerCPULimits"]
            r.fields["ref_containerMemoryRequests"] = r.fields["ref_containerMemoryRequests"] + container.fields["ref_containerMemoryRequests"]
            r.fields["ref_containerMemoryLimits"] = r.fields["ref_containerMemoryLimits"] + container.fields["ref_containerMemoryLimits"]

            # TODO: Re-make, since potentially single PVC may be connected to multiple pods
            r.fields["ref_containerPVCQuantity"] = r.fields["ref_containerPVCQuantity"] + container.fields["ref_containerPVCQuantity"]
            r.fields["ref_containerPVCRequests"] = r.fields["ref_containerPVCRequests"] + container.fields["ref_containerPVCRequests"]

            prev_container = container

        r.fields["containerKey"] = ""
        r.fields["podKey"] = ""
        r.fields["podGlobalIndex"] = ""
        r.fields["podName"] = pod_count
        r.fields["containerName"] = container_count

        r.fields["change"] = "Unchanged"
        if with_changes:
            r.check_if_modified()

        return r

    def set_optimal_field_width(self, raw_units: bool):
        # Minimum width
        ContainerListItem.appName_width = 12
        ContainerListItem.podName_width = 12
        ContainerListItem.containerName_width = 22
        ContainerListItem.containerCPURequests_width = 6
        ContainerListItem.containerCPULimits_width = 6
        ContainerListItem.containerMemoryRequests_width = 6
        ContainerListItem.containerMemoryLimits_width = 6

        ContainerListItem.containerPVCList_width = 4
        ContainerListItem.containerPVCQuantity_width = 6
        ContainerListItem.containerPVCRequests_width = 6
        # Note: assuming that for ref_* fields width will be the same

        for container in self.containers:
            ContainerListItem.appName_width = max(ContainerListItem.appName_width, len(container.fields['appName']))
            ContainerListItem.podName_width = max(ContainerListItem.podName_width, len(container.fields['podName']))
            ContainerListItem.containerName_width = max(ContainerListItem.containerName_width, len(container.fields['containerName']))
            ContainerListItem.containerCPURequests_width = \
                max(ContainerListItem.containerCPURequests_width,
                    len(res_mem_bytes_to_str_1024(container.fields['containerCPURequests'], raw_units))
                    )
            ContainerListItem.containerCPULimits_width = \
                max(ContainerListItem.containerCPULimits_width,
                    len(res_mem_bytes_to_str_1024(container.fields['containerCPULimits'], raw_units))
                    )
            ContainerListItem.containerMemoryRequests_width = \
                max(ContainerListItem.containerMemoryRequests_width,
                    len(res_mem_bytes_to_str_1024(container.fields['containerMemoryRequests'], raw_units))
                    )
            ContainerListItem.containerMemoryLimits_width = \
                max(ContainerListItem.containerMemoryLimits_width,
                    len(res_mem_bytes_to_str_1024(container.fields['containerMemoryLimits'], raw_units))
                    )

            ContainerListItem.containerPVCList_width = \
                max(ContainerListItem.containerPVCList_width,
                    len("{}".format(container.fields['containerPVCList']))
                    )
            # ContainerListItem.containerPVCQuantity_width = 4
            ContainerListItem.containerPVCRequests_width = \
                max(ContainerListItem.containerPVCRequests_width,
                    len(res_mem_bytes_to_str_1024(container.fields['containerPVCRequests'], raw_units))
                    )

    def print_table(self, raw_units: bool, pretty: bool, with_changes: bool):
        self.set_optimal_field_width(raw_units)

        ContainerListHeader().print_table(raw_units, None, with_changes)
        ContainerListLine().print_table(raw_units, None, with_changes)

        prev_container = None
        for container in self.containers:
            container.print_table(raw_units, prev_container, with_changes)
            if pretty:
                prev_container = container

        ContainerListLine().print_table(raw_units, None, with_changes)

        # All total
        total = self.get_resources_total(with_changes=with_changes)
        total.fields['podName'] = "{} pods".format(total.fields['podName'])
        total.fields['containerName'] = "{} containers".format(total.fields['containerName'])
        total.print_table(raw_units, None, with_changes)

        # Non-jobs, non-init containers
        running = self.filter(ContainerListItem(
            {
                "workloadType": '^(?!Job).*$',
                "containerType": "^(?!init).*$"
            }
        ))
        total = running.get_resources_total(with_changes=with_changes)
        total.fields['podName'] = "{} non-jobs".format(total.fields['podName'])
        total.fields['containerName'] = "{} non-init containers".format(total.fields['containerName'])
        total.print_table(raw_units, None, with_changes)

    def print_tree(self, raw_units: bool, with_changes: bool):
        self.set_optimal_field_width(raw_units)

        ContainerListHeader().print_tree(raw_units, None, with_changes)
        ContainerListLine().print_tree(raw_units, None, with_changes)

        prev_container = None
        for container in self.containers:
            container.print_tree(raw_units, prev_container, with_changes=with_changes)
            prev_container = container

    def print_csv(self):
        ContainerListHeader().print_csv()

        for row in self.containers:
            row.print_csv()

    def add_pod(self) -> Dict:  # Returns fields of newly added container
        i = len(self.containers)

        if i > 0 and not self.containers[i - 1].has_pod():
            i = i - 1
            fields = self.containers[i].fields
        else:
            self.containers.append(ContainerListItem())
            fields = self.containers[i].fields

            # Pod-specific information
            fields["podGlobalIndex"] = 1
            if i > 0:  # Not the first pod
                fields["podGlobalIndex"] = self.containers[i - 1].fields["podGlobalIndex"] + 1

        return fields

    # If pod without containers - replaces container info, otherwise adds new container
    def add_container(self) -> Dict:  # Returns fields of newly added container
        i = len(self.containers)

        if i == 0:
            raise RuntimeError("Trying to add container when pod is not added")

        if not self.containers[i - 1].has_pod():
            raise RuntimeError("Container can be added only to existing pod")

        if not self.containers[i - 1].has_container():
            i = i - 1
            fields = self.containers[i].fields
        else:
            self.containers.append(ContainerListItem())
            fields = self.containers[i].fields

            fields["podGlobalIndex"] = self.containers[i - 1].fields["podGlobalIndex"]
            fields["appIndex"] = self.containers[i - 1].fields["appIndex"]
            fields["workloadType"] = self.containers[i - 1].fields["workloadType"]
            fields["appName"] = self.containers[i - 1].fields["appName"]
            fields["podName"] = self.containers[i - 1].fields["podName"]

        return fields

    def add_pvc(self) -> PVCListItem:  # TODO: align with add_pods and add_containers
        i = len(self.pvcs)

        self.pvcs.append(PVCListItem())
        pvc = self.pvcs[i]

        pvc.fields['index'] = i

        return pvc

    def parse_container_resources(self, container: JSON, containerType: str, pod_volumes: JSON):
        fields = self.add_container()

        fields["containerName"] = container["name"]
        fields["containerType"] = containerType

        try:
            fields["containerCPURequests"] = res_cpu_str_to_millicores(container["resources"]["requests"]["cpu"])
        except KeyError:
            pass

        try:
            fields["containerCPULimits"] = res_cpu_str_to_millicores(container["resources"]["limits"]["cpu"])
        except KeyError:
            pass

        try:
            fields["containerMemoryRequests"] = res_mem_str_to_bytes(container["resources"]["requests"]["memory"])
        except KeyError:
            pass

        try:
            fields["containerMemoryLimits"] = res_mem_str_to_bytes(container["resources"]["limits"]["memory"])
        except KeyError:
            pass

        if 'volumeMounts' in container:
            for mount in container["volumeMounts"]:
                volume_type = None
                for volume in pod_volumes:
                    if volume['name'] == mount['name']:
                        # Usually volume contains two fields: "name" and something identifying type of the volume
                        volume_fields_except_name = set(volume.keys()) - {'name'}

                        if len(volume_fields_except_name) != 1:
                            raise RuntimeError("Expecting 2 fields for volume {}, but there are: {}".format(volume['name'], volume.keys()))

                        volume_type = volume_fields_except_name.pop()

                        if volume_type == 'persistentVolumeClaim':
                            fields['containerPVCList'].add(volume['persistentVolumeClaim']['claimName'])

                if volume_type is None:
                    raise RuntimeError("Volume mount '{}' not found in pod_descpod volumes".format(mount['name']))

    def read_res_desc_from_cluster(self, namespace: str) -> JSON:
        cmd = ['cat', namespace]
        cmd_str = ' '.join(cmd)

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            raise RuntimeError(
                "Cannot get namespace content, return code is {}. Command: `{}`. Error: '{}'".format(result.returncode, cmd_str, result.stderr.decode('utf-8')))

        content = result.stdout.decode('utf-8')

        pods = json.loads(content)

        return pods

    def read_res_desc_from_file(self, filename: str) -> JSON:
        with open(filename) as podsFile:
            res_desc = json.load(podsFile)

        return res_desc

    def read_res_desc(self, source: str) -> JSON:
        if source[:1] == "@":
            res_desc = self.read_res_desc_from_cluster(namespace=source[1:])
        else:
            res_desc = self.read_res_desc_from_file(filename=source)

        try:
            if res_desc['apiVersion'] != 'v1':
                raise RuntimeError("Unsupported input format: expecting 'apiVersion': 'v1', but '{}' is given".format(res_desc['apiVersion']))
        except KeyError:
            raise RuntimeError("Unsupported input format: expecting apiVersion 'v1', but no apiVersion is given")

        return res_desc

    def load(self, source: str) -> None:
        global logger

        context = {'source': source}

        logger.debug("Parsing {}".format(context))

        res_desc: JSON = self.read_res_desc(source=source)

        res_index = 0
        for res_item_desc in res_desc["items"]:
            context_res_item = {**context, 'index': res_index}

            if res_item_desc['kind'] == 'Pod':
                self.load_pod(pod_desc=res_item_desc, context=context_res_item)
            elif res_item_desc['kind'] == 'PersistentVolumeClaim':
                self.load_pvc(pvc_desc=res_item_desc, context=context_res_item)
            else:
                raise RuntimeError("Unexpected resource kind: {}. Context: {}".format(res_item_desc['kind'], context_res_item))

            res_index = res_index + 1

        self.renew_replica_indices()
        self.renew_pvc_information()

    def load_pod(self, pod_desc: JSON, context: Dict) -> None:
        logger.debug("Parsing pod {}".format(context))

        fields = self.add_pod()

        fields["podName"] = pod_desc["metadata"]["name"]

        context = {**context, 'podName': fields["podName"]}

        if len(pod_desc["metadata"]["ownerReferences"]) != 1:
            raise RuntimeError("Pod has {} owner references; exactly one is expected. Context: {}".format(
                len(pod_desc["metadata"]["ownerReferences"]),
                context
            ))
        fields["workloadType"] = pod_desc["metadata"]["ownerReferences"][0]["kind"]
        if 'app' in pod_desc["metadata"]["labels"]:
            fields["appName"] = pod_desc["metadata"]["labels"]["app"]
        else:
            fields["appName"] = pod_desc["metadata"]["labels"]["app.kubernetes.io/name"]

        try:
            pod_volumes = pod_desc['spec']['volumes']
        except KeyError:
            pod_volumes = []

        if "initContainers" in pod_desc["spec"]:
            container_index_in_pod = -1
            for container in pod_desc["spec"]["initContainers"]:
                container_index_in_pod = container_index_in_pod + 1
                self.parse_container_resources(container=container, containerType="init", pod_volumes=pod_volumes)

        container_index_in_pod = -1
        for container in pod_desc["spec"]["containers"]:
            container_index_in_pod = container_index_in_pod + 1
            self.parse_container_resources(container=container, containerType="reg", pod_volumes=pod_volumes)

    def load_pvc(self, pvc_desc: JSON, context: Dict) -> None:
        logger.debug("Parsing PVC {}".format(context))

        pvc = self.add_pvc()  # Note: this will fill index

        # TODO: Delete
        """
            'key': '',
            'index': 0,  # int - global numeration of PVCs
            'uid': '',  # str

            'name': '',  # str - used for binding with containers
            'storageClassName': '',  # str

            'containerList': set(),  # List of strings - keys of containers using this PVC
            'containerQuantity': int,  # int, containers using this PVC

            'requests': 0  # int, bytes

        """

        pvc.fields['name'] = pvc_desc['metadata']['name']

        context = {**context, 'pvcName': pvc.fields['name']}

        pvc.fields['uid'] = pvc_desc['metadata']['uid']
        pvc.fields['storageClassName'] = pvc_desc['spec']['storageClassName']
        pvc.fields['requests'] = res_mem_str_to_bytes(pvc_desc['spec']['resources']['requests']['storage'])

        pvc.make_key()


    # Get FIRST container by key
    def get_container_by_key(self, key) -> Union[ContainerListItem, None]:
        for container in self.containers:
            if container.fields['containerKey'] == key:
                return container

        return None

    def compare(self, ref_res):
        # Added and modified
        for container in self.containers:
            ref_container = ref_res.get_container_by_key(container.fields['containerKey'])

            if ref_container is None:
                container.fields['change'] = 'New Container'
            else:
                for res_field in ['containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits']:
                    container.fields['ref_' + res_field] = ref_container.fields[res_field]
                container.check_if_modified()

        # Deleted
        for ref_container in ref_res.containers:
            container = self.get_container_by_key(ref_container.fields['containerKey'])

            if container is None:
                self.containers.append(ref_container)

                deleted_container = self.containers[-1]

                deleted_container.fields['change'] = 'Deleted Container'
                deleted_container.fields['podGlobalIndex'] = 0

                deleted_container.fields["ref_containerCPURequests"] = deleted_container.fields["containerCPURequests"]
                deleted_container.fields["ref_containerCPULimits"] = deleted_container.fields["containerCPULimits"]
                deleted_container.fields["ref_containerMemoryRequests"] = deleted_container.fields["containerMemoryRequests"]
                deleted_container.fields["ref_containerMemoryLimits"] = deleted_container.fields["containerMemoryLimits"]

                deleted_container.fields["containerCPURequests"] = 0
                deleted_container.fields["containerCPULimits"] = 0
                deleted_container.fields["containerMemoryRequests"] = 0
                deleted_container.fields["containerMemoryLimits"] = 0

        self.sort()


################################################################################
# Functions
################################################################################

def setup_logging():
    global logger

    logger = logging.getLogger(__name__)
    syslog = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    syslog.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(syslog)


def parse_args():
    global args

    epilog = \
        """
        Filter criteria is a comma-separated list of 'field=regex' tokens. Fields can be specified as full names or as aliases: workloadType (kind), podName (pod), containerType (type), containerName (container). If field is not specified, podName is assumed. Regular expressions are case-sensitive.
        
        Examples:\n
        
        Filter all ReplicaSets:
        -f kind=Replica
        -f workloadType=Replica
        
        Filter all ReplicaSets and StatefulSets
        -f kind=Replica\|State
        -f kind='Replica|State'
        
        Filter all non-Jobs:
        -f kind='^(?!Job).*$'
        
        Filter all pods having 'log' in the name:
        -f log
        -f pod=log
        -f podName=log
        
        Filter all application (non-init) containers in all ReplicaSets with "log" in pod name:
        -f log,kind=R,type=reg
        """

    parser = argparse.ArgumentParser(
        description='Provides statistics for resources from `kubectl describe pods -o json`',
        epilog=epilog,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-f', '--filter', dest='filter_criteria', type=str,
                        help='Match only pods/containers matching criteria. Refer below for details.')
    parser.add_argument('-o', '--output', dest='output_format', type=str, default='pretty',
                        help='Specify output format: pretty, table, tree, csv')
    parser.add_argument('-r', metavar='FILE', dest='reference', type=str,
                        help='Reference file or @namespace to compare with')  # TODO: Allow several references
    parser.add_argument('-u', dest='raw_units', action="store_true",
                        help="Don't convert CPU and Memory values in human-readable format")
    parser.add_argument(metavar="FILE", dest='inputs', type=str, nargs='+',
                        help='Input file or @namespace')

    # Show help if no arguments supplied
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    parser.parse_args()

    args = parser.parse_args()


def parse_filter_expression(criteria: str) -> ContainerListItem:
    r = ContainerListItem()

    if criteria is not None:
        for criterion in criteria.split(','):
            parts = criterion.split("=", 1)
            if len(parts) == 1:
                parts = ["podName", parts[0]]

            # Resolve aliases
            aliases = {
                "kind": "workloadType",
                "pod": "podName",
                "type": "containerType",
                "container": "containerName",
            }
            if parts[0] in aliases.keys():
                parts[0] = aliases[parts[0]]

            # Validate both parts
            if parts[0] not in r.fields:
                raise RuntimeError(
                    "Invalid filtering criterion field: '{}'. All criteria: '{}'".format(parts[0], criteria))

            try:
                re.compile(parts[1])
            except re.error as e:
                raise RuntimeError(
                    "Invalid regular expression for field '{}'. All criterion: '{}'. Error: {}".format(parts[0], criteria, e))

            # Accept criteria
            r.fields[parts[0]] = parts[1]

    return r


def res_cpu_str_to_millicores(value: str) -> int:
    r: int

    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes
    if value[-1:] == "m":
        r = int(value[:-1])
    else:
        r = int(value) * 1000

    return r


def res_mem_str_to_bytes(value: str) -> int:
    r: int

    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes
    if value[-1:] == "k":
        r = int(value[:-1]) * 1024
    elif value[-1:] == "M":
        r = int(value[:-1]) * 1024 * 1024
    elif value[-1:] == "G":
        r = int(value[:-1]) * 1024 * 1024 * 1024
    elif value[-1:] == "T":
        r = int(value[:-1]) * 1024 * 1024 * 1024 * 1024
    elif value[-1:] == "P":
        r = int(value[:-1]) * 1024 * 1024 * 1024 * 1024 * 1024
    elif value[-1:] == "E":
        r = int(value[:-1]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024

    elif value[-2:] == "ki":
        r = int(value[:-2]) * 1000
    elif value[-2:] == "Mi":
        r = int(value[:-2]) * 1000 * 1000
    elif value[-2:] == "Gi":
        r = int(value[:-2]) * 1000 * 1000 * 1000
    elif value[-2:] == "Ti":
        r = int(value[:-2]) * 1000 * 1000 * 1000 * 1000
    elif value[-2:] == "Pi":
        r = int(value[:-2]) * 1000 * 1000 * 1000 * 1000 * 1000
    elif value[-2:] == "Ei":
        r = int(value[:-2]) * 1000 * 1000 * 1000 * 1000 * 1000 * 1000

    else:
        r = int(value)

    return r


def res_cpu_millicores_to_str(value: int, raw_units: bool) -> str:
    r = str(value)  # Raw units

    if not raw_units:
        r = str(value) + "m"

        if value > 10 * 1000 - 1:
            r = str(round(float(value) / 1000, 1))

    return r


def res_mem_bytes_to_str_1024(value: int, raw_units: bool) -> str:
    r = str(value)

    if not raw_units:
        if value > 1 * 1024 - 1:
            r = str(round(float(value) / 1024, 1)) + "k"

        if value > 1 * 1024 * 1024 - 1:
            r = str(round(float(value) / 1024 / 1024, 1)) + "M"

        if value > 1 * 1024 * 1024 * 1024 - 1:
            r = str(round(float(value) / 1024 / 1024 / 1024, 1)) + "G"

        if value > 1 * 1024 * 1024 * 1024 * 1024 - 1:
            r = str(round(float(value) / 1024 / 1024 / 1024 / 1024, 1)) + "T"

        if value > 1 * 1024 * 1024 * 1024 * 1024 * 1024 - 1:
            r = str(round(float(value) / 1024 / 1024 / 1024 / 1024 / 1024, 1)) + "P"

        if value > 1 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 - 1:
            r = str(round(float(value) / 1024 / 1024 / 1024 / 1024 / 1024 / 1024, 1)) + "E"

    return r


################################################################################
# Main
################################################################################

def main():
    setup_logging()

    parse_args()

    target_res = KubernetesResourceSet()
    ref_res = KubernetesResourceSet()

    with_changes = args.reference is not None

    try:
        for i in args.inputs:
            target_res.load(i)

        if with_changes:
            ref_res.load(args.reference)
            target_res.compare(ref_res)

        pods = target_res.filter(
            parse_filter_expression(args.filter_criteria)
        )

        if args.output_format == "table":
            pods.print_table(raw_units=args.raw_units, pretty=False, with_changes=with_changes)
            logger.debug("Output format: table")
        elif args.output_format == "pretty":
            pods.print_table(raw_units=args.raw_units, pretty=True, with_changes=with_changes)
            logger.debug("Output format: pretty")
        elif args.output_format == "tree":
            pods.print_tree(raw_units=args.raw_units, with_changes=True)
            logger.debug("Output format: tree")
        elif args.output_format == "csv":
            pods.print_csv()
            logger.debug("Output format: csv")
        else:
            raise RuntimeError("Invalid output format: {}".format(args.output_format))

    except Exception as e:
        logger.error(("{}".format(e)))
        traceback.print_exc()
        quit(1)


if __name__ == "__main__":
    main()
