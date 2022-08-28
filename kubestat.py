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

from collections import OrderedDict

################################################################################
# Constants, global variables, types
################################################################################
# Types
JSON = TypeVar('JSON', Dict, List)

# Constants
SYM_LINE = '-'

# https://dev.to/ifenna__/adding-colors-to-bash-scripts-48g4
COLOR_RESET = '\033[0m'

COLOR_DEFAULT       = '\033[0m'
COLOR_BLACK         = '\033[0;30m'
COLOR_RED           = '\033[0;31m'
COLOR_GREEN         = '\033[0;32m'
COLOR_YELLOW        = '\033[0;33m'
COLOR_BLUE          = '\033[0;34m'
COLOR_MAGENTA       = '\033[0;35m'
COLOR_CYAN          = '\033[0;36m'
COLOR_LIGHT_GRAY    = '\033[0;37m'
COLOR_GRAY          = '\033[0;90m'
COLOR_LIGHT_RED     = '\033[0;91m'
COLOR_LIGHT_GREEN   = '\033[0;92m'
COLOR_LIGHT_YELLOW  = '\033[0;93m'
COLOR_LIGHT_BLUE    = '\033[0;94m'
COLOR_LIGHT_MAGENTA = '\033[0;95m'
COLOR_LIGHT_CYAN    = '\033[0;96m'
COLOR_WHITE         = '\033[0;97m'

COLOR_BOLD_DEFAULT       = '\033[1m'
COLOR_BOLD_BLACK         = '\033[1;30m'
COLOR_BOLD_RED           = '\033[1;31m'
COLOR_BOLD_GREEN         = '\033[1;32m'
COLOR_BOLD_YELLOW        = '\033[1;33m'
COLOR_BOLD_BLUE          = '\033[1;34m'
COLOR_BOLD_MAGENTA       = '\033[1;35m'
COLOR_BOLD_CYAN          = '\033[1;36m'
COLOR_BOLD_LIGHT_GRAY    = '\033[1;37m'
COLOR_BOLD_GRAY          = '\033[1;90m'
COLOR_BOLD_LIGHT_REDY    = '\033[1;91m'
COLOR_BOLD_LIGHT_GREEN   = '\033[1;92m'
COLOR_BOLD_LIGHT_YELLOW  = '\033[1;93m'
COLOR_BOLD_LIGHT_BLUE    = '\033[1;94m'
COLOR_BOLD_LIGHT_MAGENTA = '\033[1;95m'
COLOR_BOLD_LIGHT_CYAN    = '\033[1;96m'
COLOR_BOLD_WHITE         = '\033[1;97m'

CONFIG = {
    'table_view': {
        'columns_no_diff':   ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'PVCList'],
        'columns_with_diff': ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_ephStorageRequests', 'ref_ephStorageLimits', 'ref_PVCRequests', 'changedFields']
    },
    'tree_view': {
        # Make sure first field is '_tree_branch'
        'columns_no_diff':   ['_tree_branch', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'PVCList'],
        'columns_with_diff': ['_tree_branch', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_ephStorageRequests', 'ref_ephStorageLimits', 'ref_PVCRequests', 'changedFields'],

        'pod_branch':       ['podIndex', 'workloadType', 'podName'],
        'container_branch': ['type', 'name'],

        'header_indent': 4,
        'pod_indent': 0,
        'container_indent': 6,
    },
    'colors': {
        'changes': {
            'Unchanged': COLOR_DEFAULT,
            'Deleted Pod': COLOR_RED,
            'Deleted Container': COLOR_RED,
            'New Pod': COLOR_GREEN,
            'New Container': COLOR_GREEN,
            'Modified': COLOR_LIGHT_YELLOW
        },
        'changes_bold': {
            'Unchanged': COLOR_BOLD_DEFAULT,
            'Deleted Pod': COLOR_BOLD_RED,
            'Deleted Container': COLOR_BOLD_RED,
            'New Pod': COLOR_BOLD_GREEN,
            'New Container': COLOR_BOLD_GREEN,
            'Modified': COLOR_BOLD_LIGHT_YELLOW
        },
        'changes_tree_pod_branch': {
            'Unchanged': COLOR_WHITE,
            'Deleted Pod': COLOR_LIGHT_RED,
            'Deleted Container': COLOR_LIGHT_YELLOW,  # Deleted container is modified pod
            'New Pod': COLOR_LIGHT_GREEN,
            'New Container': COLOR_LIGHT_YELLOW,  # New container is modified pod
            'Modified': COLOR_LIGHT_YELLOW
        }
    },
    'fields': {}  # Header, alignment (no size)
}

# Global variables
logger: Optional[logging.Logger] = None  # Will be filled in setup_logging()
args: Optional[argparse.Namespace] = None  # Will be filled in parse_args()


################################################################################
# Classes
################################################################################

class ContainerListItem:
    fields: OrderedDict = OrderedDict()  # Preserving elements order is important for exporting CSV

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
        self.fields = OrderedDict([
            ("appKey", ""),  # str
            ("appIndex", 0),  # int: global numeration of application
            ("appName", ""),  # str: can be viewed as pod name without suffixes
            ("workloadType", ""),  # str: DaemonSet, ReplicaSet, StatefulSet, Job

            ("podKey", ""),  # str
            ("podIndex", 0),  # int: global numeration of pods
            ("podLocalIndex", 0),  # int: numeration of pods within application (ReplicaSet, DaemonSet, etc)
            ("podName", ""),  # str

            ("key", ""),  # str
            ("index", 0),  # int: global numeration of containers
            ("localIndex", 0),  # int: numeration of container within pod
            ("type", ""),  # str: init, reg
            ("name", ""),  # str

            ("CPURequests", 0),  # int, milliCore
            ("CPULimits", 0),  # int, milliCore
            ("memoryRequests", 0),  # int, bytes
            ("memoryLimits", 0),  # int, bytes
            ("ephStorageRequests", 0),  # int, bytes
            ("ephStorageLimits", 0),  # int, bytes

            ("PVCList", set()),  # List of strings
            ("PVCQuantity", 0),  # int
            ("PVCRequests", 0),  # int, bytes
            ("PVCList_not_found", set()),  # List of strings

            ("change", "Unchanged"),  # str: Unchanged, Deleted Pod, Deleted Container, New Pod, New Container, Modified
            ("changedFields", set()),  # If change == 'Modified" - list of fields modified

            ("ref_CPURequests", 0),  # int, milliCore
            ("ref_CPULimits", 0),  # int, milliCore
            ("ref_memoryRequests", 0),  # int, bytes
            ("ref_memoryLimits", 0),  # int, bytes
            ("ref_ephStorageRequests", 0),  # int, bytes
            ("ref_ephStorageLimits", 0),  # int, bytes

            ("ref_PVCList", set()),  # List of strings
            ("ref_PVCQuantity", 0),  # int
            ("ref_PVCRequests", 0),  # int, bytes
            ("ref_PVCList_not_found", set()),  # List of strings

            # Special dynamically generated fields
            ("_tree_branch", ''),  # str
            ("_tree_branch_pod", ''),  # str
            ("_tree_branch_container", ''),  # str
            ("_tree_branch_summary", ''),  # str
            ("_tree_branch_header", '')  # str
        ])

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
            "ephStorageRequests": 0,
            "ephStorageLimits": 0,

            "PVCList": 0,
            "PVCQuantity": 0,
            "PVCRequests": 0,
            "PVCList_not_found": 0,

            "change": 0,
            "changedFields": 0,

            "ref_CPURequests": 0,
            "ref_CPULimits": 0,
            "ref_memoryRequests": 0,
            "ref_memoryLimits": 0,
            "ref_ephStorageRequests": 0,
            "ref_ephStorageLimits": 0,

            "ref_PVCList": 0,
            "ref_PVCQuantity": 0,
            "ref_PVCRequests": 0,
            "ref_PVCList_not_found": 0,

            # Special dynamically generated fields
            '_tree_branch': 0,  # Combined
            '_tree_branch_pod': 0,
            '_tree_branch_container': 0,
            '_tree_branch_summary': 0,
            '_tree_branch_header': 0
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
            "ephStorageRequests": '>',
            "ephStorageLimits": '>',

            "PVCList": '<',
            "PVCQuantity": '>',
            "PVCRequests": '>',
            "PVCList_not_found": '<',

            "change": '<',
            "changedFields": '<',

            "ref_CPURequests": '>',
            "ref_CPULimits": '>',
            "ref_memoryRequests": '>',
            "ref_memoryLimits": '>',
            "ref_ephStorageRequests": '>',
            "ref_ephStorageLimits": '>',

            "ref_PVCList": '<',
            "ref_PVCQuantity": '>',
            "ref_PVCRequests": '>',
            "ref_PVCList_not_found": '<',

            # Special dynamically generated fields
            '_tree_branch': '<',  # Combined
            '_tree_branch_pod': '<',
            '_tree_branch_container': '<',
            '_tree_branch_summary': '<',
            '_tree_branch_header': '<'
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
        for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCList', 'PVCRequests']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'
                self.fields['changedFields'].add(res_field)

    def get_formatted_fields(self, raw_units: bool) -> Dict:
        formatted_fields = copy.deepcopy(self.fields)

        # Make human-readable values
        if not self.is_decoration():
            formatted_fields["CPURequests"] = res_cpu_millicores_to_str(formatted_fields["CPURequests"], raw_units)
            formatted_fields["CPULimits"] = res_cpu_millicores_to_str(formatted_fields["CPULimits"], raw_units)

            formatted_fields["memoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["memoryRequests"], raw_units)
            formatted_fields["memoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["memoryLimits"], raw_units)

            formatted_fields["ephStorageRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ephStorageRequests"], raw_units)
            formatted_fields["ephStorageLimits"] = res_mem_bytes_to_str_1024(formatted_fields["ephStorageLimits"], raw_units)

            formatted_fields["PVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["PVCRequests"], raw_units)

            formatted_fields["ref_CPURequests"] = res_cpu_millicores_to_str(formatted_fields["ref_CPURequests"], raw_units)
            formatted_fields["ref_CPULimits"] = res_cpu_millicores_to_str(formatted_fields["ref_CPULimits"], raw_units)

            formatted_fields["ref_memoryRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_memoryRequests"], raw_units)
            formatted_fields["ref_memoryLimits"] = res_mem_bytes_to_str_1024(formatted_fields["ref_memoryLimits"], raw_units)

            formatted_fields["ref_ephStorageRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_ephStorageRequests"], raw_units)
            formatted_fields["ref_ephStorageLimits"] = res_mem_bytes_to_str_1024(formatted_fields["ref_ephStorageLimits"], raw_units)

            formatted_fields["ref_PVCRequests"] = res_mem_bytes_to_str_1024(formatted_fields["ref_PVCRequests"], raw_units)

        # Make sure all fields are strings
        for k, v in formatted_fields.items():
            if type(v) is set:
                formatted_fields[k] = ', '.join(v)
            elif type(v) is not str:
                formatted_fields[k] = '{}'.format(v)

        return formatted_fields

    # Special about dynamic fields: they rely on values and width of main fields
    def get_dynamic_fields(self, raw_units: bool) -> Dict:
        global CONFIG

        dynamic_fields: Dict = dict()

        tree_branch_header_indent_width: int = CONFIG['tree_view']['header_indent']
        pod_indent_width: int = CONFIG['tree_view']['pod_indent']
        container_indent_width: int = CONFIG['tree_view']['container_indent']
        summary_indent_width: int = 4

        # Pod
        columns = CONFIG['tree_view']['pod_branch']
        value = self.fields_to_table(columns=columns, raw_units=raw_units, highlight_changes=False, make_bold=False)
        dynamic_fields['_tree_branch_pod'] = (' ' * pod_indent_width) + value

        # Container
        columns = CONFIG['tree_view']['container_branch']
        value = self.fields_to_table(columns=columns, raw_units=raw_units, highlight_changes=False, make_bold=False)
        dynamic_fields['_tree_branch_container'] = (' ' * container_indent_width) + value

        # Summary - relevant only for summary items
        dynamic_fields['_tree_branch_summary'] = (' ' * summary_indent_width) + self.fields['podName'] + ', ' + self.fields['name']
        fake_tree_branch_summary = '999 pods 99 of 99 PVCs (non-jobs), 999 containers (non-init)'

        # Header - relevant only for header items
        dynamic_fields['_tree_branch_header'] = (' ' * tree_branch_header_indent_width) + self.fields['_tree_branch']

        # Combined tree branch - needed to calculate field width
        dynamic_fields['_tree_branch'] = '*' * max(  # Any symbol
            len(dynamic_fields['_tree_branch_pod']),
            len(dynamic_fields['_tree_branch_container']),
            len(fake_tree_branch_summary),
            len(dynamic_fields['_tree_branch_header'])
        )

        # Result
        return dynamic_fields

    def fields_to_table(self, columns: List, raw_units: bool, highlight_changes: bool, make_bold: bool) -> str:
        global CONFIG

        template: str = ""

        color_map = CONFIG['colors']['changes']
        if make_bold:
            color_map = CONFIG['colors']['changes_bold']

        for column in columns:
            field_template = '{' + column + ':' + ContainerListItem.fields_alignment[column] + str(ContainerListItem.fields_width[column]) + '}'

            if highlight_changes:
                # Needed to match both main fields and ref_* fields
                column_changed = column
                if column[:4] == 'ref_':
                    column_changed = column[4:]

                if self.fields['change'] == 'Modified':
                    if column_changed in self.fields['changedFields'] or column == 'change':
                        field_template = color_map['Modified'] + field_template + COLOR_RESET
                    else:
                        field_template = color_map['Unchanged'] + field_template + COLOR_RESET
                else:
                    field_template = color_map[self.fields['change']] + field_template + COLOR_RESET
            else:
                if make_bold:
                    field_template = COLOR_BOLD_DEFAULT + field_template + COLOR_RESET
                else:
                    pass

            template = template + field_template + self.sym_column_separator

        formatted_fields = self.get_formatted_fields(raw_units=raw_units)

        return template.format(**formatted_fields)

    def print_table(self, raw_units: bool, with_changes: bool):
        global CONFIG

        highlight_changes: bool = True
        if self.is_decoration():
            highlight_changes = False

        columns = CONFIG['table_view']['columns_no_diff']
        if with_changes:
            columns = CONFIG['table_view']['columns_with_diff']

        row = self.fields_to_table(columns=columns, raw_units=raw_units, highlight_changes=highlight_changes, make_bold=False)

        print(row)

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        global CONFIG

        dynamic_fields: Dict = self.get_dynamic_fields(raw_units=raw_units)

        if prev_container is None or not prev_container.is_same_pod(container=self):
            # Printing additional pod line

            pod_color_map = CONFIG['colors']['changes_tree_pod_branch']

            # First column (Pod)
            tree_branch: str = dynamic_fields['_tree_branch_pod']

            # Pod: table row
            row_template = '{:' + ContainerListItem.fields_alignment['_tree_branch'] + str(ContainerListItem.fields_width['_tree_branch']) + '}'

            row_template = \
                pod_color_map[self.fields['change']] + \
                row_template + \
                COLOR_RESET

            row = row_template.format(tree_branch)

            print(row)

        # First column (container)
        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_container']

        # Print row
        columns = CONFIG['tree_view']['columns_no_diff']
        if with_changes:
            columns = CONFIG['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, raw_units=raw_units, highlight_changes=True, make_bold=False)

        print(row)

    def print_csv(self):
        csv_writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        values = self.get_formatted_fields(raw_units=True)
        csv_writer.writerow(values.values())


class ContainerListLine(ContainerListItem):
    def __init__(self):
        super().__init__()
        self.sym_column_separator = '-' * len(self.sym_column_separator)

    def reset(self):
        for k, v in ContainerListItem.fields_width.items():
            self.fields[k] = SYM_LINE * ContainerListItem.fields_width[k]

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        global CONFIG

        # First column
        # Already filled with right value

        # Print row
        columns = CONFIG['tree_view']['columns_no_diff']
        if with_changes:
            columns = CONFIG['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, raw_units=True, highlight_changes=False, make_bold=True)

        print(row)

    def print_csv(self):
        raise RuntimeError('ContainerListLine is not expected to be exported to CSV')


class ContainerListHeader(ContainerListItem):
    def __init__(self):
        super().__init__()

    def reset(self):
        self.fields = OrderedDict([
            ("appKey", "App Key"),
            ("appIndex", "AppN"),
            ("appName", "Application"),
            ("workloadType", "Workload"),

            ("podKey", "Pod Key"),
            ("podIndex", "#"),
            ("podLocalIndex", "PodLN"),
            ("podName", "Pod"),

            ("key", "Container Key"),
            ("index", "ContN"),
            ("localIndex", "LN"),
            ("type", "Type"),
            ("name", "Container"),

            ("CPURequests", "CPU_R"),
            ("CPULimits", "CPU_L"),
            ("memoryRequests", "Mem_R"),
            ("memoryLimits", "Mem_L"),
            ("ephStorageRequests", "Eph_R"),
            ("ephStorageLimits", "Eph_L"),

            ("PVCList", "PVC List"),
            ("PVCQuantity", "PVC_Q"),
            ("PVCRequests", "PVC_R"),
            ("PVCList_not_found", "PVC List (not found)"),

            ("change", "Change"),
            ("changedFields", "Changed Fields"),

            ("ref_CPURequests", "rCPU_R"),
            ("ref_CPULimits", "rCPU_L"),
            ("ref_memoryRequests", "rMem_R"),
            ("ref_memoryLimits", "rMem_L"),
            ("ref_ephStorageRequests", "rEph_R"),
            ("ref_ephStorageLimits", "rEph_L"),

            ("ref_PVCList", "rPVC List"),
            ("ref_PVCQuantity", "rPVC_Q"),
            ("ref_PVCRequests", "rPVC_R"),
            ("ref_PVCList_not_found", "rPVC List (not found)"),

            ("_tree_branch", "Resource")  # Note: this will be moved to _tree_branch_header and added with indent
        ])

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        global CONFIG

        # First column
        dynamic_fields: Dict = self.get_dynamic_fields(raw_units=raw_units)
        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_header']

        # Print row
        columns = CONFIG['tree_view']['columns_no_diff']
        if with_changes:
            columns = CONFIG['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, raw_units=True, highlight_changes=False, make_bold=True)

        print(row)

    def print_csv(self):
        for key in self.fields.keys():
            self.fields[key] = key
        super().print_csv()


class ContainerListSummary(ContainerListItem):
    def __init__(self):
        super().__init__()

    def print_tree(self, raw_units: bool, prev_container, with_changes: bool):
        # First column
        dynamic_fields: Dict = self.get_dynamic_fields(raw_units=raw_units)
        if with_changes:
            columns = ['_tree_branch', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_ephStorageRequests', 'ref_ephStorageLimits', 'ref_PVCRequests', 'changedFields']

        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_summary']

        # Print row
        columns = CONFIG['tree_view']['columns_no_diff']
        if with_changes:
            columns = CONFIG['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, raw_units=raw_units, highlight_changes=True, make_bold=True)

        print(row)

    def print_csv(self):
        raise RuntimeError('ContainerListSummary is not expected to be exported to CSV')


class PVCListItem:
    fields: OrderedDict = OrderedDict()  # Preserving elements order is important for exporting CSV

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.fields = OrderedDict([
            ('key', ''),
            ('index', 0),  # int - global numeration of PVCs
            ('uid', ''),  # str

            ('name', ''),  # str - used for binding with containers
            ('storageClassName', ''),  # str

            ('containerList', set()),  # List of strings - keys of containers using this PVC
            ('containerQuantity', int),  # int, containers using this PVC

            ('requests', 0),  # int, bytes

            ("change", "Unchanged"),  # str: Unchanged, Deleted, New, Modified
            ("changedFields", set()),  # If change == 'Modified" - list of fields modified

            ('ref_requests', 0),  # int, bytes
        ])

    def generate_keys(self) -> None:
        self.fields['key'] = self.fields['name']

    def is_used(self) -> bool:
        return len(self.fields['containerList']) != 0

    def is_deleted(self) -> bool:
        return self.fields['change'] == 'Deleted'

    def is_new(self) -> bool:
        return self.fields['change'] == 'New'

    def check_if_modified(self):
        for res_field in ['requests']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'
                self.fields['changedFields'].add(res_field)


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
                        pod_local_index = pod_local_index + 1
            container.fields['podLocalIndex'] = pod_local_index + 1

            if container_index > 0:
                if not self.containers[container_index - 1].is_same_pod(container, trust_key=False):
                    container_local_index = 0
                else:
                    container_local_index = container_local_index + 1
            container.fields['localIndex'] = container_local_index + 1

            container.fields['index'] = container_index + 1

            container_index = container_index + 1

            # Generate keys
            container.generate_keys()

        # Regenerate indices and keys: PVCs
        pvc_index: int = 0
        for pvc in self.pvcs:
            pvc.fields['index'] = pvc_index
            pvc_index = pvc_index + 1
            pvc.generate_keys()

    # Note: not touching deleted entities
    def renew_relations(self) -> None:
        container: ContainerListItem
        pvc: PVCListItem

        # Managing PVCs
        for pvc in self.pvcs:
            if pvc.is_deleted():
                continue

            pvc.fields['containerList'] = set()
            pvc.fields['containerQuantity'] = 0

        for container in self.containers:
            if container.is_deleted():
                continue

            container.fields['PVCQuantity'] = 0
            container.fields['PVCRequests'] = 0
            container.fields['PVCList_not_found'] = set()

            for pvc_name in container.fields['PVCList']:
                pvc = self.get_pvc_by_name(name=pvc_name, allow_deleted=False, allow_new=True)

                if pvc is not None:
                    container.fields['PVCQuantity'] = container.fields['PVCQuantity'] + 1
                    container.fields['PVCRequests'] = container.fields['PVCRequests'] + pvc.fields['requests']

                    pvc.fields['containerList'].add(container.fields['key'])
                    pvc.fields['containerQuantity'] = len(pvc.fields['containerList'])
                else:
                    logging.debug("Container '{}' refers to PVC '{}' that does not exist".format(
                        container.fields['key'], pvc_name
                    ))
                    container.fields['PVCList_not_found'].add(pvc_name)

    def sort(self) -> None:
        self.containers = sorted(self.containers, key=lambda c: c.fields['key'])
        self.pvcs = sorted(self.pvcs, key=lambda p: p.fields['key'])

    # Note: each field in criteria is a regex
    def filter(self, criteria: ContainerListItem):
        r = KubernetesResourceSet()

        r.pvcs = self.pvcs  # TODO: Think if filter is to be applied here. May be not.

        for container in self.containers:
            matches = True
            for field in ["workloadType", "podName", "type", "name"]:
                matches = matches and bool(re.search(criteria.fields[field], container.fields[field]))

            if matches:
                r.containers.append(container)

        return r

    def get_used_pvc_names(self) -> Set:
        r = set()

        for pvc in self.pvcs:
            if pvc.is_used():
                r.add(pvc.fields['name'])

        return r

    def get_resources_total(self, with_changes: bool, pod_name_suffix: str = '', container_name_suffix: str = '') -> ContainerListItem:
        r = ContainerListSummary()

        pod_quantity = 0
        container_quantity = 0

        # Pod quantity, container quantity, sum of all resources (except PVC)
        prev_container = None
        for container in self.containers:
            if container.is_deleted():
                continue

            container_quantity = container_quantity + 1
            if not container.is_same_pod(prev_container):
                pod_quantity = pod_quantity + 1

            r.fields["CPURequests"] = r.fields["CPURequests"] + container.fields["CPURequests"]
            r.fields["CPULimits"] = r.fields["CPULimits"] + container.fields["CPULimits"]
            r.fields["memoryRequests"] = r.fields["memoryRequests"] + container.fields["memoryRequests"]
            r.fields["memoryLimits"] = r.fields["memoryLimits"] + container.fields["memoryLimits"]
            r.fields["ephStorageRequests"] = r.fields["ephStorageRequests"] + container.fields["ephStorageRequests"]
            r.fields["ephStorageLimits"] = r.fields["ephStorageLimits"] + container.fields["ephStorageLimits"]

            r.fields["ref_CPURequests"] = r.fields["ref_CPURequests"] + container.fields["ref_CPURequests"]
            r.fields["ref_CPULimits"] = r.fields["ref_CPULimits"] + container.fields["ref_CPULimits"]
            r.fields["ref_memoryRequests"] = r.fields["ref_memoryRequests"] + container.fields["ref_memoryRequests"]
            r.fields["ref_memoryLimits"] = r.fields["ref_memoryLimits"] + container.fields["ref_memoryLimits"]
            r.fields["ref_ephStorageRequests"] = r.fields["ref_ephStorageRequests"] + container.fields["ref_ephStorageRequests"]
            r.fields["ref_ephStorageLimits"] = r.fields["ref_ephStorageLimits"] + container.fields["ref_ephStorageLimits"]

            r.fields["PVCList"] = r.fields["PVCList"].union(container.fields["PVCList"])
            r.fields["PVCList_not_found"] = r.fields["PVCList_not_found"].union(container.fields["PVCList_not_found"])
            r.fields["ref_PVCList"] = r.fields["ref_PVCList"].union(container.fields["ref_PVCList"])
            r.fields["ref_PVCList_not_found"] = r.fields["ref_PVCList_not_found"].union(container.fields["ref_PVCList_not_found"])

            prev_container = container

        # PVC quantity
        r.fields['PVCQuantity'] = len(r.fields['PVCList'])
        r.fields['ref_PVCQuantity'] = len(r.fields['ref_PVCList'])

        # Sum of all USED PVC storage sizes
        for pvc_name in r.fields['PVCList']:
            pvc = self.get_pvc_by_name(pvc_name, allow_deleted=False, allow_new=True)
            if pvc is not None:
                r.fields['PVCRequests'] = r.fields['PVCRequests'] + pvc.fields['requests']

        for pvc_name in r.fields['ref_PVCList']:
            pvc = self.get_pvc_by_name(pvc_name, allow_deleted=True, allow_new=False)
            if pvc is not None:
                r.fields['ref_PVCRequests'] = r.fields['ref_PVCRequests'] + pvc.fields['ref_requests']

        # PVC statistic
        used_pvc_names = self.get_used_pvc_names()
        used_pvc_quantity = len(used_pvc_names)
        pvc_quantity = self.get_pvc_quantity()
        # TODO: Show quantity of reference PVCs vs quantity of subject PVCs

        # Other fields
        r.fields["key"] = ""
        r.fields["podKey"] = ""
        r.fields["podIndex"] = ""
        r.fields["podName"] = "{} pods using {} of {} PVCs{}".format(pod_quantity, used_pvc_quantity, pvc_quantity, pod_name_suffix)
        r.fields["name"] = "{} containers{}".format(container_quantity, container_name_suffix)

        r.fields["change"] = "Unchanged"
        r.fields["changedFields"] = set()
        if with_changes:
            r.check_if_modified()

        return r

    def set_optimal_field_width(self, raw_units: bool) -> None:
        ContainerListItem.reset_field_widths()

        for container in self.containers + [ContainerListHeader()]:  # Taking maximum length of values of all containers plus header
            str_fields = container.get_formatted_fields(raw_units=raw_units)
            for k, v in str_fields.items():
                ContainerListItem.fields_width[k] = max(ContainerListItem.fields_width[k], len(v))

        # Special about dynamic fields: they rely on values and width of main fields
        for container in self.containers + [ContainerListHeader()]:  # Taking maximum length of values of all containers plus header
            str_fields = container.get_dynamic_fields(raw_units=raw_units)
            for k, v in str_fields.items():
                ContainerListItem.fields_width[k] = max(ContainerListItem.fields_width[k], len(v))

    def print(self, output_format: str, raw_units: bool, with_changes: bool):
        global logger

        # Columns width (not needed for CSV)
        self.set_optimal_field_width(raw_units)

        # Summary lines
        summary = list()

        summary.append(self.get_resources_total(with_changes=with_changes))

        running = self.filter(ContainerListItem(  # Non-jobs, non-init containers
            {
                "workloadType": '^(?!Job).*$',
                "type": "^(?!init).*$"
            }
        ))
        summary.append(running.get_resources_total(with_changes=with_changes, pod_name_suffix=' (non-jobs)', container_name_suffix=' (non-init)'))

        # Printing
        if output_format == "table":
            logger.debug("Output format: table")
            self.print_table(raw_units=raw_units, with_changes=with_changes, summary=summary)
        elif output_format == "tree":
            logger.debug("Output format: tree")
            self.print_tree(raw_units=raw_units, with_changes=with_changes, summary=summary)
        elif output_format == "csv":
            logger.debug("Output format: csv")
            self.print_csv()
        else:
            raise RuntimeError("Invalid output format: {}".format(output_format))

    def print_table(self, raw_units: bool, with_changes: bool, summary: Optional[List] = False):

        ContainerListHeader().print_table(raw_units=raw_units, with_changes=with_changes)
        ContainerListLine().print_table(raw_units=raw_units, with_changes=with_changes)

        for container in self.containers:
            container.print_table(raw_units=raw_units, with_changes=with_changes)

        ContainerListLine().print_table(raw_units=raw_units, with_changes=with_changes)

        for summary_item in summary:
            summary_item.print_table(raw_units=raw_units, with_changes=with_changes)

    def print_tree(self, raw_units: bool, with_changes: bool, summary: Optional[List] = False):
        self.set_optimal_field_width(raw_units=raw_units)

        ContainerListHeader().print_tree(raw_units=raw_units, prev_container=None, with_changes=with_changes)
        ContainerListLine().print_tree(raw_units=raw_units, prev_container=None, with_changes=with_changes)

        prev_container = None
        for container in self.containers:
            container.print_tree(raw_units=raw_units, prev_container=prev_container, with_changes=with_changes)
            prev_container = container

        ContainerListLine().print_tree(raw_units=raw_units, prev_container=None, with_changes=with_changes)

        for summary_item in summary:
            summary_item.print_tree(raw_units=raw_units, prev_container=None, with_changes=with_changes)

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

    def add_pvc(self) -> PVCListItem:
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

        try:
            container.fields["ephStorageRequests"] = res_mem_str_to_bytes(container_desc["resources"]["requests"]["ephemeral-storage"])
        except KeyError:
            pass

        try:
            container.fields["ephStorageLimits"] = res_mem_str_to_bytes(container_desc["resources"]["limits"]["ephemeral-storage"])
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

        container.fields["appName"] = pod_desc["metadata"]["ownerReferences"][0]["name"]
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
    def get_container_by_key(self, key: str) -> Union[ContainerListItem, None]:
        for container in self.containers:
            if container.fields['key'] == key:
                return container

        return None

    def get_pvc_by_key(self, key: str) -> Union[PVCListItem, None]:
        for pvc in self.pvcs:
            if pvc.fields['key'] == key:
                return pvc

        return None

    def get_pvc_by_name(self, name: str, allow_deleted: bool, allow_new: bool) -> Union[PVCListItem, None]:
        for pvc in self.pvcs:
            if pvc.fields['name'] == name:
                if pvc.is_deleted() and not allow_deleted:
                    continue
                if pvc.is_new() and not allow_new:
                    continue
                return pvc

        return None

    def get_pvc_quantity(self) -> int:
        return len(self.pvcs)

    def compare(self, ref_res):
        # TODO: Clear previous comparison

        self.compare_pvcs(ref_res=ref_res)
        self.compare_containers(ref_res=ref_res)

        self.sort()

    def compare_pvcs(self, ref_res):
        # Added and modified
        for pvc in self.pvcs:
            ref_pvc = ref_res.get_pvc_by_key(pvc.fields['key'])

            if ref_pvc is None:
                pvc.fields['change'] = 'New'
                pvc.fields['changedFields'] = set()
            else:
                for res_field in ['requests']:
                    pvc.fields['ref_' + res_field] = ref_pvc.fields[res_field]
                pvc.check_if_modified()

        # Deleted
        for ref_pvc in ref_res.pvcs:
            pvc = self.get_pvc_by_key(ref_pvc.fields['key'])

            if pvc is None:
                self.pvcs.append(ref_pvc)

                deleted_pvc = self.pvcs[-1]

                deleted_pvc.fields['change'] = 'Deleted'
                deleted_pvc.fields['changedFields'] = set()
                deleted_pvc.fields['index'] = 0
                deleted_pvc.fields['uid'] = ''

                deleted_pvc.fields["ref_requests"] = deleted_pvc.fields["requests"]

                deleted_pvc.fields["requests"] = 0

    def compare_containers(self, ref_res):
        # Added and modified
        for container in self.containers:
            ref_container = ref_res.get_container_by_key(container.fields['key'])

            if ref_container is None:
                container.fields['change'] = 'New Container'
                container.fields['changedFields'] = set()
            else:
                for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCList', 'PVCQuantity', 'PVCRequests', 'PVCList_not_found']:
                    container.fields['ref_' + res_field] = ref_container.fields[res_field]
                container.check_if_modified()

        # Deleted
        for ref_container in ref_res.containers:
            container = self.get_container_by_key(ref_container.fields['key'])

            if container is None:
                self.containers.append(ref_container)

                deleted_container = self.containers[-1]

                deleted_container.fields['change'] = 'Deleted Container'
                deleted_container.fields['changedFields'] = set()
                deleted_container.fields['podIndex'] = 0

                for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCList', 'PVCQuantity', 'PVCRequests', 'PVCList_not_found']:
                    deleted_container.fields['ref_' + res_field] = deleted_container.fields[res_field]
                    if type(deleted_container.fields[res_field]) is int:
                        deleted_container.fields[res_field] = 0
                    elif type(deleted_container.fields[res_field]) is str:
                        deleted_container.fields[res_field] = ''
                    elif type(deleted_container.fields[res_field]) is set:
                        deleted_container.fields[res_field] = set()
                    else:
                        raise RuntimeError("Invalid type of reference field {}: {}".format(
                            res_field,
                            type(deleted_container.fields[res_field])
                        ))

        # Containers -> Pods
        pods_change = dict()
        change_mix = 'mix'  # Constant
        for c in self.containers:
            pod_key = c.fields['podKey']
            change = c.fields['change']

            if pod_key in pods_change:
                if change != pods_change[pod_key]:
                    pods_change[pod_key] = change_mix
            else:
                pods_change[pod_key] = change

        # TODO: validate assumption: pod cannot exist without containers
        for c in self.containers:
            pod_key = c.fields['podKey']

            if pods_change[pod_key] != change_mix:
                if c.fields['change'] == 'Deleted Container':
                    c.fields['change'] = 'Deleted Pod'
                if c.fields['change'] == 'New Container':
                    c.fields['change'] = 'New Pod'


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
    parser.add_argument('-o', '--output', dest='output_format', type=str, default='tree',
                        help='Specify output format: tree, table, csv')
    parser.add_argument('-r', metavar='FILE', dest='references', type=str, action='append',
                        help='Reference file(s) or @namespace to compare with')
    parser.add_argument('-u', dest='raw_units', action="store_true",
                        help="Don't convert CPU and Memory values in human-readable format")
    parser.add_argument(metavar="FILE", dest='inputs', type=str, nargs='+',
                        help='Input file(s) or @namespace')

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

    # Special case
    elif value[-1:] == "m":
        r = int(round(int(value[:-1]) / 1000, 0))

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

    all_resources = KubernetesResourceSet()
    ref_resources = KubernetesResourceSet()

    try:
        for source in args.inputs:
            all_resources.load(source=source)

        with_changes = False
        if args.references is not None:
            for source in args.references:
                ref_resources.load(source=source)
                with_changes = True

        if with_changes:
            all_resources.compare(ref_resources)

        resources = all_resources.filter(
            parse_filter_expression(args.filter_criteria)
        )

        resources.print(output_format=args.output_format, raw_units=args.raw_units, with_changes=with_changes)
    except Exception as e:
        logger.error(("{}".format(e)))
        traceback.print_exc()
        quit(1)


if __name__ == "__main__":
    main()
