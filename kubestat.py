#!/usr/bin/env python3

# TODO:
# Summary to tree view
# Toggle Megabytes | Mibibytes units
# Deleted Pod use case

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

class PodListItem:
    fields: Dict = {}

    appName_width: int = 0
    podName_width: int = 0
    containerMemoryLimits_width: int = 0
    containerMemoryRequests_width: int = 0
    containerCPULimits_width: int = 0
    containerCPURequests_width: int = 0
    containerName_width: int = 0

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

            "podGlobalIndex": 0,  # int - global numeration of pods
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

            "change": "Unchanged",  # Unchanged, Deleted Pod, Deleted Container, New Pod, New Container, Modified

            "ref_containerCPURequests": 0,  # int, milliCore
            "ref_containerCPULimits": 0,  # int, milliCore
            "ref_containerMemoryRequests": 0,  # int, bytes
            "ref_containerMemoryLimits": 0,  # int, bytes
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

    def is_same_pod(self, item, trust_pod_key: bool = True) -> bool:
        if trust_pod_key:
            return item is not None and self.fields["podKey"] == item.fields["podKey"]
        else:
            # To be used in functions when key is being generated
            return item is not None and self.fields["podName"] == item.fields["podName"]

    def is_same_app(self, item) -> bool:
        return item is not None and self.fields["appName"] == item.fields["appName"]

    def is_deleted(self) -> bool:
        return self.fields['change'] in ['Deleted Pod', 'Deleted Container']

    def check_if_modified(self):
        for res_field in ['containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'

    def get_formatted_fields(self, raw_units: bool) -> Dict:
        formatted_fields = copy.deepcopy(self.fields)

        # Make human-readable values
        if not self.is_decoration():
            formatted_fields["containerCPURequests"] = res_cpu_millicores_to_str(formatted_fields["containerCPURequests"], raw_units)
            formatted_fields["containerCPULimits"] = res_cpu_millicores_to_str(formatted_fields["containerCPULimits"], raw_units)

            formatted_fields["containerMemoryRequests"] = res_mem_bytes_to_millicores(formatted_fields["containerMemoryRequests"], raw_units)
            formatted_fields["containerMemoryLimits"] = res_mem_bytes_to_millicores(formatted_fields["containerMemoryLimits"], raw_units)

            formatted_fields["ref_containerCPURequests"] = res_cpu_millicores_to_str(formatted_fields["ref_containerCPURequests"], raw_units)
            formatted_fields["ref_containerCPULimits"] = res_cpu_millicores_to_str(formatted_fields["ref_containerCPULimits"], raw_units)

            formatted_fields["ref_containerMemoryRequests"] = res_mem_bytes_to_millicores(formatted_fields["ref_containerMemoryRequests"], raw_units)
            formatted_fields["ref_containerMemoryLimits"] = res_mem_bytes_to_millicores(formatted_fields["ref_containerMemoryLimits"], raw_units)

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

    def print_table(self, raw_units: bool, prev_item, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        # Skip application/workload values if it was contained in the previous item
        if self.is_same_app(prev_item):
            formatted_fields["workloadType"] = ""
            formatted_fields["appName"] = ""

        # Skip pod values if it was contained in the previous item
        if self.is_same_pod(prev_item):
            formatted_fields["podGlobalIndex"] = ""
            formatted_fields["podName"] = ""

        template = \
            "{appName:<" + str(PodListItem.appName_width + 2) + "}" + \
            "{workloadType:<13}" + \
            "{podGlobalIndex:<4}" + \
            "{podName:<" + str(PodListItem.podName_width + 2) + "}" + \
            "{containerType:<7}" + \
            "{containerName:<" + str(PodListItem.containerName_width + 2) + "}" + \
            "{containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"

        if with_changes:
            template = template + \
                       "  " + \
                       "{change:<18}" + \
                       "{ref_containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
                       "{ref_containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
                       "{ref_containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
                       "{ref_containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"

        # logger.info("{}".format(self.fields))

        if not self.is_decoration():
            template = self.decorate_changes(template, is_pod_only=False)

        print(template.format(**formatted_fields))

    def get_tree_columns_width(self, with_changes: bool):
        container_indent = 6

        pod_width = 4 + 13 + (PodListItem.podName_width + 2)
        container_width = container_indent + (4 + 2 + 1) + (PodListItem.containerName_width + 2)
        item_width = max(pod_width, container_width)

        resources_width = \
            (PodListItem.containerCPURequests_width + 2) + \
            (PodListItem.containerCPULimits_width + 2) + \
            (PodListItem.containerMemoryRequests_width + 2) + \
            (PodListItem.containerMemoryLimits_width + 2)

        if with_changes:
            resources_width = resources_width + \
                              2 + \
                              18 + \
                              (PodListItem.containerCPURequests_width + 2) + \
                              (PodListItem.containerCPULimits_width + 2) + \
                              (PodListItem.containerMemoryRequests_width + 2) + \
                              (PodListItem.containerMemoryLimits_width + 2)

        return container_indent, item_width, resources_width

    def print_tree(self, raw_units: bool, prev_item, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        # Calculating column widths
        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)

        # pod_width = 4 + 13 + (PodListItem.podName_width + 2)
        # container_width = container_indent + (4 + 2 + 1) + (PodListItem.containerName_width + 2)
        # item_width = max(pod_width, container_width)

        podName_width = item_width - 4 - 13 - 2
        containerName_width = item_width - container_indent - (4 + 2 + 1) - 2

        # Add special line for pods
        pod_template = ""
        if not self.is_same_pod(prev_item):
            # '\033[1;37m' +\
            pod_template = \
                "{podGlobalIndex:<4}" + \
                "{workloadType:<13}" + \
                "{podName:<" + str(podName_width + 2) + "}"
            # "{appName:<" + str(PodListItem.appName_width + 2) + "}" + \

            pod_template = self.decorate_changes(pod_template, is_pod_only=True)
            pod_template = pod_template + '\n'

        container_template = \
            " " * container_indent + \
            "({containerType:<4}) " + \
            "{containerName:<" + str(containerName_width + 2) + "}" + \
            "{containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"

        if with_changes:
            container_template = container_template + \
                                 "  " + \
                                 "{change:<18}" + \
                                 "{ref_containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
                                 "{ref_containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
                                 "{ref_containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
                                 "{ref_containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"
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

            self.fields["change"],

            self.fields["ref_containerCPURequests"],
            self.fields["ref_containerCPULimits"],
            self.fields["ref_containerMemoryRequests"],
            self.fields["ref_containerMemoryLimits"]
        ])


class PodListLine(PodListItem):
    def __init__(self):
        super().__init__()

        self.fields["containerKey"] = "-" * 4
        self.fields["podKey"] = "-" * 4

        # Note: q-ty of dashes must correspond to size of fields
        self.fields["appIndex"] = "-" * 4
        self.fields["appName"] = "-" * (PodListItem.appName_width + 2)
        self.fields["workloadType"] = "-" * 13
        self.fields["podGlobalIndex"] = "-" * 4
        self.fields["podName"] = "-" * (PodListItem.podName_width + 2)
        self.fields["containerType"] = "-" * 7
        self.fields["containerName"] = "-" * (PodListItem.containerName_width + 2)
        self.fields["containerCPURequests"] = "-" * (PodListItem.containerCPURequests_width + 2)
        self.fields["containerCPULimits"] = "-" * (PodListItem.containerCPULimits_width + 2)
        self.fields["containerMemoryRequests"] = "-" * (PodListItem.containerMemoryRequests_width + 2)
        self.fields["containerMemoryLimits"] = "-" * (PodListItem.containerMemoryLimits_width + 2)

        self.fields["change"] = "-" * 18

        self.fields["ref_containerCPURequests"] = "-" * (PodListItem.containerCPURequests_width + 2)
        self.fields["ref_containerCPULimits"] = "-" * (PodListItem.containerCPULimits_width + 2)
        self.fields["ref_containerMemoryRequests"] = "-" * (PodListItem.containerMemoryRequests_width + 2)
        self.fields["ref_containerMemoryLimits"] = "-" * (PodListItem.containerMemoryLimits_width + 2)

    def print_tree(self, raw_units: bool, prev_item, with_changes: bool):
        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)
        print('-' * (item_width + resources_width))


class PodListHeader(PodListItem):
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

        self.fields["change"] = "Change"

        self.fields["ref_containerCPURequests"] = "rCPU_R"
        self.fields["ref_containerCPULimits"] = "rCPU_L"
        self.fields["ref_containerMemoryRequests"] = "rMem_R"
        self.fields["ref_containerMemoryLimits"] = "rMem_L"

    def print_tree(self, raw_units: bool, prev_item, with_changes: bool):
        formatted_fields = self.get_formatted_fields(raw_units)

        container_indent, item_width, resources_width = self.get_tree_columns_width(with_changes=with_changes)

        formatted_fields['item_txt'] = "Item"

        template = \
            "{item_txt:" + str(item_width) + "}" + \
            "{containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
            "{containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
            "{containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
            "{containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"

        if with_changes:
            template = template + \
                       "  " + \
                       "{change:<18}" + \
                       "{ref_containerCPURequests:>" + str(PodListItem.containerCPURequests_width + 2) + "}" + \
                       "{ref_containerCPULimits:>" + str(PodListItem.containerCPULimits_width + 2) + "}" + \
                       "{ref_containerMemoryRequests:>" + str(PodListItem.containerMemoryRequests_width + 2) + "}" + \
                       "{ref_containerMemoryLimits:>" + str(PodListItem.containerMemoryLimits_width + 2) + "}"

        print(template.format(**formatted_fields))


class PodList:
    items: List[PodListItem] = list()

    def __init__(self):
        self.reset()

    def reset(self):
        self.items: List[PodListItem] = list()

    def sort(self):
        sorted_items = sorted(self.items, key=lambda item: item.fields['containerKey'])
        self.items = sorted_items

    # Important: this must NOT be run after compare, otherwise deleted pods will be identified as separate ones
    def renew_replica_indices(self):
        for item in self.items:
            appName_len = len(item.fields['appName'])
            item.set_appIndex(item.fields['podName'][appName_len:])

        self.sort()

        replica_global_index = 1
        replica_app_index = 1

        prev_item = None
        for item in self.items:
            if prev_item is None:
                prev_item = item

            if not item.is_same_pod(prev_item, trust_pod_key=False):
                replica_global_index = replica_global_index + 1

                replica_app_index = replica_app_index + 1
                if not item.is_same_app(prev_item):
                    replica_app_index = 0

            item.set_appIndex(str(replica_app_index).zfill(3))
            item.fields['podGlobalIndex'] = replica_global_index

            prev_item = item

    # Note: each field in criteria is a regex
    def filter(self, criteria: PodListItem):
        r = PodList()
        for item in self.items:
            matches = True
            for field in ["workloadType", "podName", "containerType", "containerName"]:
                matches = matches and bool(re.search(criteria.fields[field], item.fields[field]))

            if matches:
                r.items.append(item)

        return r

    def get_resources_total(self, with_changes: bool) -> PodListItem:
        r = PodListItem()

        pod_count = 0
        container_count = 0

        prev_item = None
        for item in self.items:
            if item.is_deleted():  # TODO: review logic
                continue

            container_count = container_count + 1
            if not item.is_same_pod(prev_item):
                pod_count = pod_count + 1

            r.fields["containerCPURequests"] = r.fields["containerCPURequests"] + item.fields["containerCPURequests"]
            r.fields["containerCPULimits"] = r.fields["containerCPULimits"] + item.fields["containerCPULimits"]
            r.fields["containerMemoryRequests"] = r.fields["containerMemoryRequests"] + item.fields[
                "containerMemoryRequests"]
            r.fields["containerMemoryLimits"] = r.fields["containerMemoryLimits"] + item.fields["containerMemoryLimits"]

            r.fields["ref_containerCPURequests"] = r.fields["ref_containerCPURequests"] + item.fields[
                "ref_containerCPURequests"]
            r.fields["ref_containerCPULimits"] = r.fields["ref_containerCPULimits"] + item.fields[
                "ref_containerCPULimits"]
            r.fields["ref_containerMemoryRequests"] = r.fields["ref_containerMemoryRequests"] + item.fields[
                "ref_containerMemoryRequests"]
            r.fields["ref_containerMemoryLimits"] = r.fields["ref_containerMemoryLimits"] + item.fields[
                "ref_containerMemoryLimits"]

            prev_item = item

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
        PodListItem.appName_width = 12
        PodListItem.podName_width = 12
        PodListItem.containerName_width = 22
        PodListItem.containerCPURequests_width = 6
        PodListItem.containerCPULimits_width = 6
        PodListItem.containerMemoryRequests_width = 6
        PodListItem.containerMemoryLimits_width = 6
        # Note: assuming that for ref_* items width will be the same

        for item in self.items:
            PodListItem.appName_width = max(PodListItem.appName_width, len(item.fields['appName']))
            PodListItem.podName_width = max(PodListItem.podName_width, len(item.fields['podName']))
            PodListItem.containerName_width = max(PodListItem.containerName_width, len(item.fields['containerName']))
            PodListItem.containerCPURequests_width = max(PodListItem.containerCPURequests_width,
                                                         len(res_mem_bytes_to_millicores(
                                                             item.fields['containerCPURequests'], raw_units)))
            PodListItem.containerCPULimits_width = max(PodListItem.containerCPULimits_width,
                                                       len(res_mem_bytes_to_millicores(
                                                           item.fields['containerCPULimits'], raw_units)))
            PodListItem.containerMemoryRequests_width = max(PodListItem.containerMemoryRequests_width,
                                                            len(res_mem_bytes_to_millicores(
                                                                item.fields['containerMemoryRequests'], raw_units)))
            PodListItem.containerMemoryLimits_width = max(PodListItem.containerMemoryLimits_width,
                                                          len(res_mem_bytes_to_millicores(
                                                              item.fields['containerMemoryLimits'], raw_units)))

    def print_table(self, raw_units: bool, pretty: bool, with_changes: bool):
        self.set_optimal_field_width(raw_units)

        PodListHeader().print_table(raw_units, None, with_changes)
        PodListLine().print_table(raw_units, None, with_changes)

        prev_item = None
        for item in self.items:
            item.print_table(raw_units, prev_item, with_changes)
            if pretty:
                prev_item = item

        PodListLine().print_table(raw_units, None, with_changes)

        # All total
        total = self.get_resources_total(with_changes=with_changes)
        total.fields['podName'] = "{} pods".format(total.fields['podName'])
        total.fields['containerName'] = "{} containers".format(total.fields['containerName'])
        total.print_table(raw_units, None, with_changes)

        # Non-jobs, non-init containers
        running = self.filter(PodListItem(
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

        PodListHeader().print_tree(raw_units, None, with_changes)
        PodListLine().print_tree(raw_units, None, with_changes)

        prev_item = None
        for item in self.items:
            item.print_tree(raw_units, prev_item, with_changes=with_changes)
            prev_item = item

    def print_csv(self):
        PodListHeader().print_csv()

        for row in self.items:
            row.print_csv()

    def add_pod(self) -> Dict:  # Returns fields of newly added item
        i = len(self.items)

        if i > 0 and not self.items[i - 1].has_pod():
            i = i - 1
            fields = self.items[i].fields
        else:
            self.items.append(PodListItem())
            fields = self.items[i].fields

            # Pod-specific information
            fields["podGlobalIndex"] = 1
            if i > 0:  # Not the first pod
                fields["podGlobalIndex"] = self.items[i - 1].fields["podGlobalIndex"] + 1

        return fields

    # If pod without containers - replaces container info, otherwise adds new item
    def add_container(self) -> Dict:  # Returns fields of newly added item
        i = len(self.items)

        if i == 0:
            raise RuntimeError("Trying to add container when pod is not added")

        if not self.items[i - 1].has_pod():
            raise RuntimeError("Container can be added only to existing pod")

        if not self.items[i - 1].has_container():
            i = i - 1
            fields = self.items[i].fields
        else:
            self.items.append(PodListItem())
            fields = self.items[i].fields

            fields["podGlobalIndex"] = self.items[i - 1].fields["podGlobalIndex"]
            fields["appIndex"] = self.items[i - 1].fields["appIndex"]
            fields["workloadType"] = self.items[i - 1].fields["workloadType"]
            fields["appName"] = self.items[i - 1].fields["appName"]
            fields["podName"] = self.items[i - 1].fields["podName"]

        return fields

    def parse_container_resources(self, container: JSON, containerType: str):
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

    def read_pods_desc_from_namespace(self, namespace: str) -> JSON:
        cmd = ['cat', namespace]
        cmd_str = ' '.join(cmd)

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            raise RuntimeError(
                "Cannot get namespace content, return code is {}. Command: `{}`. Error: '{}'".format(result.returncode, cmd_str, result.stderr.decode('utf-8')))

        content = result.stdout.decode('utf-8')

        pods = json.loads(content)

        return pods

    def read_pods_desc_from_file(self, filename: str) -> JSON:
        with open(filename) as podsFile:
            pods = json.load(podsFile)

        return pods

    def read_pods_desc(self, source: str) -> JSON:
        if source[:1] == "@":
            pods = self.read_pods_desc_from_namespace(namespace=source[1:])
        else:
            pods = self.read_pods_desc_from_file(filename=source)

        # TODO: Validate

        return pods

    def load(self, source: str):
        global logger

        self.reset()

        pods_desc_json: JSON = self.read_pods_desc(source=source)

        pod_index_in_json = -1
        for pod in pods_desc_json["items"]:
            pod_index_in_json = pod_index_in_json + 1

            fields = self.add_pod()

            if pod["kind"] != "Pod":
                fields["podName"] = "({})".format(pod["kind"])
                continue

            fields["podName"] = pod["metadata"]["name"]

            # logger.info("Parsing pod '{}'...".format(fields["podName"]))
            # logger.info("metadata = {}".format(pod["metadata"]))

            if len(pod["metadata"]["ownerReferences"]) != 1:
                raise RuntimeError("Pod #{} '{}' has {} owner references; exactly one is expected".format(
                    pod_index_in_json,
                    pod["metadata"]["name"],
                    len(pod["metadata"]["ownerReferences"])
                ))
            fields["workloadType"] = pod["metadata"]["ownerReferences"][0]["kind"]
            if 'app' in pod["metadata"]["labels"]:
                fields["appName"] = pod["metadata"]["labels"]["app"]
            else:
                fields["appName"] = pod["metadata"]["labels"]["app.kubernetes.io/name"]

            if "initContainers" in pod["spec"]:
                container_index_in_pod = -1
                for container in pod["spec"]["initContainers"]:
                    container_index_in_pod = container_index_in_pod + 1
                    self.parse_container_resources(container, "init")

            container_index_in_pod = -1
            for container in pod["spec"]["containers"]:
                container_index_in_pod = container_index_in_pod + 1
                self.parse_container_resources(container, "reg")

        self.renew_replica_indices()

    def get_first_by_key(self, key) -> Union[PodListItem, None]:
        for item in self.items:
            if item.fields['containerKey'] == key:
                return item

        return None

    def compare(self, ref_pods):
        # Added and modified
        for item in self.items:
            ref_item = ref_pods.get_first_by_key(item.fields['containerKey'])

            if ref_item is None:
                item.fields['change'] = 'New Container'
            else:
                for res_field in ['containerCPURequests', 'containerCPULimits', 'containerMemoryRequests', 'containerMemoryLimits']:
                    item.fields['ref_' + res_field] = ref_item.fields[res_field]
                item.check_if_modified()

        # Deleted
        for ref_item in ref_pods.items:
            item = self.get_first_by_key(ref_item.fields['containerKey'])

            if item is None:
                self.items.append(ref_item)

                deleted_item = self.items[-1]

                deleted_item.fields['change'] = 'Deleted Container'
                deleted_item.fields['podGlobalIndex'] = 0

                deleted_item.fields["ref_containerCPURequests"] = deleted_item.fields["containerCPURequests"]
                deleted_item.fields["ref_containerCPULimits"] = deleted_item.fields["containerCPULimits"]
                deleted_item.fields["ref_containerMemoryRequests"] = deleted_item.fields["containerMemoryRequests"]
                deleted_item.fields["ref_containerMemoryLimits"] = deleted_item.fields["containerMemoryLimits"]

                deleted_item.fields["containerCPURequests"] = 0
                deleted_item.fields["containerCPULimits"] = 0
                deleted_item.fields["containerMemoryRequests"] = 0
                deleted_item.fields["containerMemoryLimits"] = 0

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
        Filter criteria is a comma-separated list of 'field=regex' items. Fields can be specified as full names or as aliases: workloadType (kind), podName (pod), containerType (type), containerName (container). If field is not specified, podName is assumed. Regular expressions are case-sensitive.
        
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
                        help='Reference file or @namespace to compare with')
    parser.add_argument('-u', dest='raw_units', action="store_true",
                        help="Don't convert CPU and Memory values in human-readable format")
    parser.add_argument(metavar="FILE", dest='input', type=str, help='Input file or @namespace')

    # Show help if no arguments supplied
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    parser.parse_args()

    args = parser.parse_args()


def parse_filter_expression(criteria: str) -> PodListItem:
    r = PodListItem()

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


def res_mem_bytes_to_millicores(value: int, raw_units: bool) -> str:
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

    all_pods = PodList()
    ref_pods = PodList()

    with_changes = args.reference is not None

    try:
        all_pods.load(args.input)

        if with_changes:
            ref_pods.load(args.reference)
            all_pods.compare(ref_pods)

        pods = all_pods.filter(
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
