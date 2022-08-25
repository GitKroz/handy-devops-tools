#!/usr/bin/env python3

from typing import TypeVar, Dict, List, Set, Optional, Union
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
            "type": "",  # str: init, reg
            "name": "",  # str

            "CPURequests": 0,  # int, milliCore
            "CPULimits": 0,  # int, milliCore
            "memoryRequests": 0,  # int, bytes
            "memoryLimits": 0,  # int, bytes

            "PVCList": set(),  # List of strings
            "PVCQuantity": 0,  # int
            "PVCRequests": 0,  # int, bytes

            "change": "Unchanged",  # str: Unchanged, Deleted Pod, Deleted Container, New Pod, New Container, Modified

            "ref_CPURequests": 0,  # int, milliCore
            "ref_CPULimits": 0,  # int, milliCore
            "ref_memoryRequests": 0,  # int, bytes
            "ref_memoryLimits": 0,  # int, bytes

            "ref_PVCList": set(),  # List of strings
            "ref_PVCQuantity": 0,  # int
            "ref_PVCRequests": 0,  # int, bytes
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
            "type": 0,
            "name": 0,

            "CPURequests": 0,
            "CPULimits": 0,
            "memoryRequests": 0,
            "memoryLimits": 0,

            "PVCList": 0,
            "PVCQuantity": 0,
            "PVCRequests": 0,

            "change": 0,

            "ref_CPURequests": 0,
            "ref_CPULimits": 0,
            "ref_memoryRequests": 0,
            "ref_memoryLimits": 0,

            "ref_PVCList": 0,
            "ref_PVCQuantity": 0,
            "ref_PVCRequests": 0
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
            "type": '<',
            "name": '<',

            "CPURequests": '>',
            "CPULimits": '>',
            "memoryRequests": '>',
            "memoryLimits": '>',

            "PVCList": '<',
            "PVCQuantity": '>',
            "PVCRequests": '>',

            "change": '<',

            "ref_CPURequests": '>',
            "ref_CPULimits": '>',
            "ref_memoryRequests": '>',
            "ref_memoryLimits": '>',

            "ref_PVCList": '<',
            "ref_PVCQuantity": '>',
            "ref_PVCRequests": '>'
        }

    def generate_keys(self):
        self.fields['appKey'] = self.fields['appName']
        self.fields['podKey'] = self.fields['appKey'] + '/' + str(self.fields['podLocalIndex'])
        self.fields['key'] = self.fields['podKey'] + '/' + self.fields['name']

    def has_pod(self) -> bool:
        return self.fields["podName"] != ""

    def has_container(self) -> bool:
        return self.fields["name"] != ""

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
        for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'PVCList', 'PVCRequests']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'

    def get_formatted_fields(self, raw_units: bool) -> Dict:
        formatted_fields = copy.deepcopy(self.fields)

        # Make human-readable values
        if not self.is_decoration():
            formatted_fields["CPURequests"] = res_cpu_millicores_to_str(formatted_fields["CPURequests"], raw_units)
            formatted_fields["CPULimits"] = res_cpu_millicores_to_str(formatted_fields["CPULimits"], raw_units)

            formatted_fields["memoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["memoryRequests"], raw_units)
            formatted_fields["memoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["memoryLimits"], raw_units)

            formatted_fields["PVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["PVCRequests"], raw_units)

            formatted_fields["ref_CPURequests"] = res_cpu_millicores_to_str(formatted_fields["ref_CPURequests"], raw_units)
            formatted_fields["ref_CPULimits"] = res_cpu_millicores_to_str(formatted_fields["ref_CPULimits"], raw_units)

            formatted_fields["ref_memoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_memoryRequests"], raw_units)
            formatted_fields["ref_memoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["ref_memoryLimits"], raw_units)

            formatted_fields["ref_PVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_PVCRequests"], raw_units)

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
        columns = ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'PVCRequests', 'PVCList']
        if with_changes:  # TODO: Check
            columns = ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_PVCRequests']

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
            "({type:<4}) " + \
            "{name:<" + str(containerName_width + 2) + "}" + \
            "{CPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
            "{CPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
            "{memoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
            "{memoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
            "{PVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
            "{PVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        if with_changes:
            container_template = container_template + \
                                 "  " + \
                                 "{change:<18}" + \
                                 "{ref_CPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
                                 "{ref_CPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
                                 "{ref_memoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                                 "{ref_memoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
                                 "{ref_PVCQuantity:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                                 "{ref_PVCRequests:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}"
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
            self.fields["type"],
            self.fields["name"],

            self.fields["CPURequests"],
            self.fields["CPULimits"],
            self.fields["memoryRequests"],
            self.fields["memoryLimits"],

            self.fields["PVCList"],
            self.fields["PVCQuantity"],
            self.fields["PVCRequests"],

            self.fields["change"],

            self.fields["ref_CPURequests"],
            self.fields["ref_CPULimits"],
            self.fields["ref_memoryRequests"],
            self.fields["ref_memoryLimits"],

            self.fields["ref_PVCList"],
            self.fields["ref_PVCQuantity"],
            self.fields["ref_PVCRequests"]
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
            "type": "Type",
            "name": "Container",

            "CPURequests": "CPU_R",
            "CPULimits": "CPU_L",
            "memoryRequests": "Mem_R",
            "memoryLimits": "Mem_L",

            "PVCList": "PVC List",
            "PVCQuantity": "PVC_Q",
            "PVCRequests": "PVC_R",

            "change": "Change",

            "ref_CPURequests": "rCPU_R",
            "ref_CPULimits": "rCPU_L",
            "ref_memoryRequests": "rMem_R",
            "ref_memoryLimits": "rMem_L",

            "ref_PVCList": "rPVC List",
            "ref_PVCQuantity": "rPVC_Q",
            "ref_PVCRequests": "rPVC_R",
        }

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)

        formatted_fields['item_txt'] = "Item"

        template = \
            "{item_txt:" + str(item_width) + "}" + \
            "{CPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
            "{CPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
            "{memoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
            "{memoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
            "{PVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
            "{PVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

        if with_changes:
            template = template + \
                       "  " + \
                       "{change:<18}" + \
                       "{ref_CPURequests:>" + str(ContainerListItem.containerCPURequests_width + 2) + "}" + \
                       "{ref_CPULimits:>" + str(ContainerListItem.containerCPULimits_width + 2) + "}" + \
                       "{ref_memoryRequests:>" + str(ContainerListItem.containerMemoryRequests_width + 2) + "}" + \
                       "{ref_memoryLimits:>" + str(ContainerListItem.containerMemoryLimits_width + 2) + "}" + \
                       "{ref_PVCQuantity:>" + str(ContainerListItem.containerPVCQuantity_width + 2) + "}" + \
                       "{ref_PVCRequests:>" + str(ContainerListItem.containerPVCRequests_width + 2) + "}"

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

    def is_used(self) -> bool:
        return len(self.fields['containerList']) != 0


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
        self.containers = sorted(self.containers, key=lambda c: (c.fields['appName'] + '/' + c.fields['podName'] + '/' + c.fields['name']))
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

    def renew_relations(self) -> None:
        container: ContainerListItem
        pvc: PVCListItem

        # Managing PVCs
        for pvc in self.pvcs:
            pvc.fields['containerList'] = set()
            pvc.fields['containerQuantity'] = 0

        for container in self.containers:
            container.fields['PVCQuantity'] = 0
            container.fields['PVCRequests'] = 0

            for pvc_name in container.fields['PVCList']:
                pvc = self.get_pvc_by_name(name=pvc_name)

                if pvc is not None:
                    container.fields['PVCQuantity'] = container.fields['PVCQuantity'] + 1
                    container.fields['PVCRequests'] = container.fields['PVCRequests'] + pvc.fields['requests']

                    pvc.fields['containerList'].add(container.fields['key'])
                    pvc.fields['containerQuantity'] = len(pvc.fields['containerList'])

    def sort(self) -> None:
        self.containers = sorted(self.containers, key=lambda c: c.fields['key'])
        self.pvcs = sorted(self.pvcs, key=lambda p: p.fields['key'])

    # Note: each field in criteria is a regex
    def filter(self, criteria: ContainerListItem):
        r = KubernetesResourceSet()

        r.pvcs = self.pvcs  # TODO: Think if fileter is to be applied here. May be not.

        for container in self.containers:
            matches = True
            for field in ["workloadType", "podName", "type", "name"]:
                matches = matches and bool(re.search(criteria.fields[field], container.fields[field]))

            if matches:
                r.containers.append(container)

        return r

    def get_used_pvcs(self) -> Set:
        r = set()

        for pvc in self.pvcs:
            if pvc.is_used():
                r.add(pvc.fields['name'])

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

            r.fields["CPURequests"] = r.fields["CPURequests"] + container.fields["CPURequests"]
            r.fields["CPULimits"] = r.fields["CPULimits"] + container.fields["CPULimits"]
            r.fields["memoryRequests"] = r.fields["memoryRequests"] + container.fields["memoryRequests"]
            r.fields["memoryLimits"] = r.fields["memoryLimits"] + container.fields["memoryLimits"]

            r.fields["ref_CPURequests"] = r.fields["ref_CPURequests"] + container.fields["ref_CPURequests"]
            r.fields["ref_CPULimits"] = r.fields["ref_CPULimits"] + container.fields["ref_CPULimits"]
            r.fields["ref_memoryRequests"] = r.fields["ref_memoryRequests"] + container.fields["ref_memoryRequests"]
            r.fields["ref_memoryLimits"] = r.fields["ref_memoryLimits"] + container.fields["ref_memoryLimits"]

            r.fields["PVCList"] = r.fields["PVCList"].union(container.fields["PVCList"])

            prev_container = container

        r.fields['PVCQuantity'] = len(r.fields['PVCList'])
        for pvc_name in r.fields['PVCList']:
            pvc = self.get_pvc_by_name(pvc_name)
            if pvc is not None:
                r.fields['PVCRequests'] = r.fields['PVCRequests'] + pvc.fields['requests']

        r.fields["key"] = ""
        r.fields["podKey"] = ""
        r.fields["podIndex"] = ""
        r.fields["podName"] = pod_count
        r.fields["name"] = container_count

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

        # Calculate total
        total: ContainerListItem

        # All total
        used_pvc_names = self.get_used_pvcs()
        total = self.get_resources_total(with_changes=with_changes)
        total.fields['podName'] = "{} pods using {}/{} PVCs".format(total.fields['podName'], len(used_pvc_names), len(self.pvcs))
        total.fields['name'] = "{} containers".format(total.fields['name'])
        total.print_table(raw_units, None, with_changes)

        # Non-jobs, non-init containers
        running = self.filter(ContainerListItem(
            {
                "workloadType": '^(?!Job).*$',
                "type": "^(?!init).*$"
            }
        ))
        used_pvc_names = running.get_used_pvcs()
        total = running.get_resources_total(with_changes=with_changes)
        total.fields['podName'] = "{} non-jobs using {}/{} PVCs".format(total.fields['podName'], len(used_pvc_names), len(self.pvcs))
        total.fields['name'] = "{} non-init containers".format(total.fields['name'])
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

    def parse_container_resources(self, container_desc: JSON, container_type: str, pod_volumes: JSON):
        container: ContainerListItem
        container = self.add_container()

        container.fields["name"] = container_desc["name"]
        container.fields["type"] = container_type

        try:
            container.fields["CPURequests"] = res_cpu_str_to_millicores(container_desc["resources"]["requests"]["cpu"])
        except KeyError:
            pass

        try:
            container.fields["CPULimits"] = res_cpu_str_to_millicores(container_desc["resources"]["limits"]["cpu"])
        except KeyError:
            pass

        try:
            container.fields["memoryRequests"] = res_mem_str_to_bytes(container_desc["resources"]["requests"]["memory"])
        except KeyError:
            pass

        try:
            container.fields["memoryLimits"] = res_mem_str_to_bytes(container_desc["resources"]["limits"]["memory"])
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
                            container.fields['PVCList'].add(volume['persistentVolumeClaim']['claimName'])

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
        self.renew_relations()
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
                self.parse_container_resources(container_desc=container_desc, container_type="init", pod_volumes=pod_volumes)

        if "containers" in pod_desc["spec"]:
            for container_desc in pod_desc["spec"]["containers"]:
                self.parse_container_resources(container_desc=container_desc, container_type="reg", pod_volumes=pod_volumes)

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

    def get_pvc_by_name(self, name: str) -> Union[PVCListItem, None]:
        for pvc in self.pvcs:
            if pvc.fields['name'] == name:
                return pvc

        return None

    def compare(self, ref_res):
        # Added and modified
        for container in self.containers:
            ref_container = ref_res.get_container_by_key(container.fields['key'])

            if ref_container is None:
                container.fields['change'] = 'New Container'
            else:
                for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits']:
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

                deleted_container.fields["ref_CPURequests"] = deleted_container.fields["CPURequests"]
                deleted_container.fields["ref_CPULimits"] = deleted_container.fields["CPULimits"]
                deleted_container.fields["ref_memoryRequests"] = deleted_container.fields["memoryRequests"]
                deleted_container.fields["ref_memoryLimits"] = deleted_container.fields["memoryLimits"]

                deleted_container.fields["CPURequests"] = 0
                deleted_container.fields["CPULimits"] = 0
                deleted_container.fields["memoryRequests"] = 0
                deleted_container.fields["memoryLimits"] = 0

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
        Filter criteria is a comma-separated list of 'field=regex' tokens. Fields can be specified as full names or as aliases: workloadType (kind), podName (pod), type, name (container). If field is not specified, podName is assumed. Regular expressions are case-sensitive.
        
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
                "type": "type",
                "container": "name",
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
