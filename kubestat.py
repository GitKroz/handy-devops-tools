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
# Types
JSON = TypeVar('JSON', Dict, List)

# Constants
SYM_LINE = '-'

# Global variables
logger: Optional[logging.Logger] = None  # Will be filled in setup_logging()
args: Optional[argparse.Namespace] = None  # Will be filled in parse_args()


################################################################################
# Classes
################################################################################

class ContainerListItem:
    fields: Dict = {}

    column_separator: str

    # Static variable
    fields_width: Dict = {}
    fields_alignment: Dict = {}

    def __init__(self, values: Optional[Dict] = None):
        self.sym_column_separator = '  '

        self.reset()

        if values is None:
            values = {}

        for key in self.fields.keys():
            if key in values:
                self.fields[key] = values[key]

    def reset(self):
        self.fields = {
            "appKey": "",  # str
            "appIndex": 0,  # int: global numeration of application
            "appName": "",  # str: can be viewed as pod name without suffixes
            "workloadType": "",  # str: DaemonSet, ReplicaSet, StatefulSet, Job

            "podKey": "",  # str
            "podIndex": 0,  # int: global numeration of pods
            "podLocalIndex": 0,  # int: numeration of pods within application (ReplicaSet, DaemonSet, etc)
            "podName": "",  # str

            "key": "",  # str
            "index": 0,  # int: global numeration of containers
            "localIndex": 0,  # int: numeration of container within pod
            "containerType": "",  # str: init, reg
            "containerName": "",  # str

            "containerCPURequests": 0,  # int, milliCore
            "containerCPULimits": 0,  # int, milliCore
            "containerMemoryRequests": 0,  # int, bytes
            "containerMemoryLimits": 0,  # int, bytes

            "containerPVCList": set(),  # List of strings
            "containerPVCQuantity": 0,  # int
            "containerPVCRequests": 0,  # int, bytes

            "change": "Unchanged",  # str: Unchanged, Deleted Pod, Deleted Container, New Pod, New Container, Modified

            "ref_containerCPURequests": 0,  # int, milliCore
            "ref_containerCPULimits": 0,  # int, milliCore
            "ref_containerMemoryRequests": 0,  # int, bytes
            "ref_containerMemoryLimits": 0,  # int, bytes

            "ref_containerPVCList": set(),  # List of strings
            "ref_containerPVCQuantity": 0,  # int
            "ref_containerPVCRequests": 0,  # int, bytes
        }

    @staticmethod
    def reset_field_widths():
        ContainerListItem.fields_width = {
            "appKey": 0,
            "appIndex": 0,
            "appName": 0,
            "workloadType": 0,

            "podKey": 0,
            "podIndex": 0,
            "podLocalIndex": 0,
            "podName": 0,

            "key": 0,
            "index": 0,
            "localIndex": 0,
            "containerType": 0,
            "containerName": 0,

            "containerCPURequests": 0,
            "containerCPULimits": 0,
            "containerMemoryRequests": 0,
            "containerMemoryLimits": 0,

            "containerPVCList": 0,
            "containerPVCQuantity": 0,
            "containerPVCRequests": 0,

            "change": 0,

            "ref_containerCPURequests": 0,
            "ref_containerCPULimits": 0,
            "ref_containerMemoryRequests": 0,
            "ref_containerMemoryLimits": 0,

            "ref_containerPVCList": 0,
            "ref_containerPVCQuantity": 0,
            "ref_containerPVCRequests": 0
        }

        ContainerListItem.fields_alignment = {  # < (left) > (right) ^ (center) - see https://docs.python.org/3/library/string.html#grammar-token-format-string-format_spec
            "appKey": '<',
            "appIndex": '>',
            "appName": '<',
            "workloadType": '<',

            "podKey": '<',
            "podIndex": '>',
            "podLocalIndex": '>',
            "podName": '<',

            "key": '<',
            "index": '>',
            "localIndex": '>',
            "containerType": '<',
            "containerName": '<',

            "containerCPURequests": '>',
            "containerCPULimits": '>',
            "containerMemoryRequests": '>',
            "containerMemoryLimits": '>',

            "containerPVCList": '<',
            "containerPVCQuantity": '>',
            "containerPVCRequests": '>',

            "change": '<',

            "ref_containerCPURequests": '>',
            "ref_containerCPULimits": '>',
            "ref_containerMemoryRequests": '>',
            "ref_containerMemoryLimits": '>',

            "ref_containerPVCList": '<',
            "ref_containerPVCQuantity": '>',
            "ref_containerPVCRequests": '>'
        }

    def generate_keys(self):
        self.fields['appKey'] = self.fields['appName']
        self.fields['podKey'] = self.fields['appKey'] + '/' + str(self.fields['podLocalIndex'])
        self.fields['key'] = self.fields['podKey'] + '/' + self.fields['containerName']

    def has_pod(self) -> bool:
        return self.fields["podName"] != ""

    def has_container(self) -> bool:
        return self.fields["containerName"] != ""

    def is_decoration(self) -> bool:  # Header, Line etc
        return False

    def is_same_pod(self, container, trust_key: bool = True) -> bool:
        if trust_key:
            return container is not None and self.fields["podKey"] == container.fields["podKey"]
        else:
            # To be used in functions when key is being generated
            return container is not None and self.fields["podName"] == container.fields["podName"]

    def is_same_app(self, container, trust_key: bool = True) -> bool:
        if trust_key:
            return container is not None and self.fields["appKey"] == container.fields["appKey"]
        else:
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

        # Make sure all fields are strings
        for k, v in formatted_fields.items():
            if type(v) is set:
                formatted_fields[k] = '{}'.format(list(v))
            elif type(v) is not str:
                formatted_fields[k] = '{}'.format(v)

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
        # TODO: exclude usage of 'prev_container'

        # TODO: add to commandline arguments
        columns = ['podIndex', 'workloadType', 'podName', 'containerType', 'containerName', 'containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits', 'containerPVCRequests', 'containerPVCList']
        if with_changes:  # TODO: Check
            columns = ['podIndex', 'workloadType', 'podName', 'containerType', 'containerName', 'containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits', 'containerPVCRequests', 'change', 'ref_containerCPURequests', 'ref_containerCPULimits', 'ref_containerMemoryRequests', 'ref_containerMemoryLimits', 'ref_containerPVCRequests']

        template = ""
        for column in columns:
            template = template + '{' + column + ':' + ContainerListItem.fields_alignment[column] + str(ContainerListItem.fields_width[column]) + '}' + self.sym_column_separator

        formatted_fields = self.get_formatted_fields(raw_units)

        print(template.format(**formatted_fields))

    # TODO: Update
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
                "{podIndex:<4}" + \
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
        # " {key}"

        container_template = self.decorate_changes(container_template, is_pod_only=False)

        template = pod_template + container_template

        print(template.format(**formatted_fields))

    def print_csv(self):
        csv_writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        csv_writer.writerow([
            self.fields["key"],
            self.fields["podKey"],

            self.fields["appIndex"],
            self.fields["appName"],
            self.fields["workloadType"],
            self.fields["podIndex"],
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
        self.sym_column_separator = '-' * len(self.sym_column_separator)

    def reset(self):
        for k, v in ContainerListItem.fields_width.items():
            self.fields[k] = SYM_LINE * ContainerListItem.fields_width[k]
        logger.debug("Tick")

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)
        print(SYM_LINE * (item_width + resources_width))


class ContainerListHeader(ContainerListItem):
    def __init__(self):
        super().__init__()

    def reset(self):
        self.fields = {
            "appKey": "App Key",
            "appIndex": "AppN",
            "appName": "Application",
            "workloadType": "Workload",

            "podKey": "Pod Key",
            "podIndex": "PodN",
            "podLocalIndex": "PodLN",
            "podName": "Pod",

            "key": "Container Key",
            "index": "N",
            "localIndex": "LN",
            "containerType": "Type",
            "containerName": "Container",

            "containerCPURequests": "CPU_R",
            "containerCPULimits": "CPU_L",
            "containerMemoryRequests": "Mem_R",
            "containerMemoryLimits": "Mem_L",

            "containerPVCList": "PVC List",
            "containerPVCQuantity": "PVC_Q",
            "containerPVCRequests": "PVC_R",

            "change": "Change",

            "ref_containerCPURequests": "rCPU_R",
            "ref_containerCPULimits": "rCPU_L",
            "ref_containerMemoryRequests": "rMem_R",
            "ref_containerMemoryLimits": "rMem_L",

            "ref_containerPVCList": "rPVC List",
            "ref_containerPVCQuantity": "rPVC_Q",
            "ref_containerPVCRequests": "rPVC_R",
        }

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

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

    def generate_keys(self):
        self.fields['key'] = self.fields['name']


class KubernetesResourceSet:
    containers: List[ContainerListItem] = list()
    pvcs: List[PVCListItem] = list()

    def __init__(self):
        self.reset()

    def reset(self):
        self.containers = list()
        self.pvcs = list()

    def renew_keys(self) -> None:
        # Sort
        self.containers = sorted(self.containers, key=lambda c: (c.fields['appName'] + '/' + c.fields['podName'] + '/' + c.fields['containerName']))
        self.pvcs = sorted(self.pvcs, key=lambda p: p.fields['name'])

        # Regenerate indices and keys: containers
        app_index: int = 0
        pod_index: int = 0  # Global
        pod_local_index: int = 0  # Within application
        container_index: int = 0  # Global
        container_local_index: int = 0  # Within pod

        for container in self.containers:
            # Indices
            if container_index > 0:
                if not self.containers[container_index - 1].is_same_app(container, trust_key=False):
                    app_index = app_index + 1
            container.fields['appIndex'] = app_index + 1

            if container_index > 0:
                if not self.containers[container_index - 1].is_same_pod(container, trust_key=False):
                    pod_index = pod_index + 1
            container.fields['podIndex'] = pod_index + 1

            if container_index > 0:
                if not self.containers[container_index - 1].is_same_app(container, trust_key=False):
                    pod_local_index = 0
                else:
                    if not self.containers[container_index - 1].is_same_pod(container, trust_key=False):
                        pod_local_index = pod_index + 1
            container.fields['podLocalIndex'] = pod_local_index + 1

            if container_index > 0:
                if not self.containers[container_index - 1].is_same_pod(container, trust_key=False):
                    container_local_index = 0
                else:
                    container_local_index = container_local_index + 1
            container.fields['localIndex'] = container_local_index + 1

            container.fields['index'] = container_index + 1  # TODO: add field

            container_index = container_index + 1

            # Generate keys
            container.generate_keys()

        # Regenerate indices and keys: PVCs
        pvc_index: int = 0
        for pvc in self.pvcs:
            pvc.fields['index'] = pvc_index
            pvc_index = pvc_index + 1
            pvc.generate_keys()

    def sort(self):
        self.containers = sorted(self.containers, key=lambda c: c.fields['key'])
        self.pvcs = sorted(self.pvcs, key=lambda p: p.fields['key'])

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

        r.fields["key"] = ""
        r.fields["podKey"] = ""
        r.fields["podIndex"] = ""
        r.fields["podName"] = pod_count
        r.fields["containerName"] = container_count

        r.fields["change"] = "Unchanged"
        if with_changes:
            r.check_if_modified()

        return r

    def set_optimal_field_width(self, raw_units: bool) -> None:
        ContainerListItem.reset_field_widths()

        for container in self.containers + [ContainerListHeader()]:  # Taking maximum length of values of all containers plus header
            str_fields = container.get_formatted_fields(raw_units=raw_units)
            for k, v in str_fields.items():
                ContainerListItem.fields_width[k] = max(ContainerListItem.fields_width[k], len(v))

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

    def add_pod(self) -> ContainerListItem:
        i: int = len(self.containers)

        container: ContainerListItem

        self.containers.append(ContainerListItem())
        container = self.containers[i]

        return container

    def add_container(self) -> ContainerListItem:
        i: int = len(self.containers)

        if i == 0:  # TODO: Add context
            raise RuntimeError("Trying to add container when no any pod is added")

        if not self.containers[i - 1].has_pod():
            raise RuntimeError("Container can be added only to existing pod")

        container: ContainerListItem
        if self.containers[i - 1].has_container():  # Adding a new record
            self.containers.append(ContainerListItem())
            container = self.containers[i]

            container.fields["appName"] = self.containers[i - 1].fields["appName"]
            container.fields["workloadType"] = self.containers[i - 1].fields["workloadType"]
            container.fields["podName"] = self.containers[i - 1].fields["podName"]
        else:  # There is a pod with no container
            container = self.containers[-1]

        return container

    def add_pvc(self) -> PVCListItem:  # TODO: align with add_pods and add_containers
        i: int = len(self.pvcs)

        pvc: PVCListItem

        self.pvcs.append(PVCListItem())
        pvc = self.pvcs[i]

        return pvc

    def parse_container_resources(self, container_desc: JSON, containerType: str, pod_volumes: JSON):
        container: ContainerListItem
        container = self.add_container()

        container.fields["containerName"] = container_desc["name"]
        container.fields["containerType"] = containerType

        try:
            container.fields["containerCPURequests"] = res_cpu_str_to_millicores(container_desc["resources"]["requests"]["cpu"])
        except KeyError:
            pass

        try:
            container.fields["containerCPULimits"] = res_cpu_str_to_millicores(container_desc["resources"]["limits"]["cpu"])
        except KeyError:
            pass

        try:
            container.fields["containerMemoryRequests"] = res_mem_str_to_bytes(container_desc["resources"]["requests"]["memory"])
        except KeyError:
            pass

        try:
            container.fields["containerMemoryLimits"] = res_mem_str_to_bytes(container_desc["resources"]["limits"]["memory"])
        except KeyError:
            pass

        if 'volumeMounts' in container_desc:
            for mount in container_desc["volumeMounts"]:
                volume_type = None
                for volume in pod_volumes:
                    if volume['name'] == mount['name']:
                        # Usually volume contains two fields: "name" and something identifying type of the volume
                        volume_fields_except_name = set(volume.keys()) - {'name'}

                        if len(volume_fields_except_name) != 1:
                            raise RuntimeError("Expecting 2 fields for volume {}, but there are: {}".format(volume['name'], volume.keys()))

                        volume_type = volume_fields_except_name.pop()

                        if volume_type == 'persistentVolumeClaim':
                            container.fields['containerPVCList'].add(volume['persistentVolumeClaim']['claimName'])

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

        self.renew_keys()
        # TODO: link pvc and containers

    def load_pod(self, pod_desc: JSON, context: Dict) -> None:
        logger.debug("Parsing pod {}".format(context))

        # Pod-specific logic
        container: ContainerListItem = self.add_pod()  # Note: this will fill index

        container.fields["podName"] = pod_desc["metadata"]["name"]

        context = {**context, 'podName': container.fields["podName"]}

        if len(pod_desc["metadata"]["ownerReferences"]) != 1:
            raise RuntimeError("Pod has {} owner references; exactly one is expected. Context: {}".format(
                len(pod_desc["metadata"]["ownerReferences"]),
                context
            ))

        if 'app' in pod_desc["metadata"]["labels"]:
            container.fields["appName"] = pod_desc["metadata"]["labels"]["app"]
        else:
            container.fields["appName"] = pod_desc["metadata"]["labels"]["app.kubernetes.io/name"]

        container.fields["workloadType"] = pod_desc["metadata"]["ownerReferences"][0]["kind"]

        # Storage-specific logic (a part of)
        # Note: pod_volumes will be used later when parsing containers
        try:
            pod_volumes = pod_desc['spec']['volumes']
        except KeyError:
            pod_volumes = []

        # Container-specific logic
        container_desc: JSON

        if "initContainers" in pod_desc["spec"]:
            for container_desc in pod_desc["spec"]["initContainers"]:
                self.parse_container_resources(container_desc=container_desc, containerType="init", pod_volumes=pod_volumes)

        if "containers" in pod_desc["spec"]:
            for container_desc in pod_desc["spec"]["containers"]:
                self.parse_container_resources(container_desc=container_desc, containerType="reg", pod_volumes=pod_volumes)

    def load_pvc(self, pvc_desc: JSON, context: Dict) -> None:
        logger.debug("Parsing PVC {}".format(context))

        pvc = self.add_pvc()

        pvc.fields['name'] = pvc_desc['metadata']['name']

        context = {**context, 'pvcName': pvc.fields['name']}

        pvc.fields['uid'] = pvc_desc['metadata']['uid']
        pvc.fields['storageClassName'] = pvc_desc['spec']['storageClassName']
        pvc.fields['requests'] = res_mem_str_to_bytes(pvc_desc['spec']['resources']['requests']['storage'])

    # Get FIRST container by key
    def get_container_by_key(self, key) -> Union[ContainerListItem, None]:
        for container in self.containers:
            if container.fields['key'] == key:
                return container

        return None

    def compare(self, ref_res):
        # Added and modified
        for container in self.containers:
            ref_container = ref_res.get_container_by_key(container.fields['key'])

            if ref_container is None:
                container.fields['change'] = 'New Container'
            else:
                for res_field in ['containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits']:
                    container.fields['ref_' + res_field] = ref_container.fields[res_field]
                container.check_if_modified()

        # Deleted
        for ref_container in ref_res.containers:
            container = self.get_container_by_key(ref_container.fields['key'])

            if container is None:
                self.containers.append(ref_container)

                deleted_container = self.containers[-1]

                deleted_container.fields['change'] = 'Deleted Container'
                deleted_container.fields['podIndex'] = 0

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
        -f kind=Replica\\|State
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
