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
import io
import shutil

from collections import OrderedDict

################################################################################
# Constants, global variables, types
################################################################################
# Types
JSON = TypeVar('JSON', Dict, List)

# Constants
SYM_LINE = '-'

# https://dev.to/ifenna__/adding-colors-to-bash-scripts-48g4
COLOR_NONE = ''
COLOR_RESET = '\033[0m'

COLOR_DEFAULT = '\033[0m'
COLOR_BLACK = '\033[0;30m'
COLOR_RED = '\033[0;31m'
COLOR_GREEN = '\033[0;32m'
COLOR_YELLOW = '\033[0;33m'
COLOR_BLUE = '\033[0;34m'
COLOR_MAGENTA = '\033[0;35m'
COLOR_CYAN = '\033[0;36m'
COLOR_LIGHT_GRAY = '\033[0;37m'
COLOR_GRAY = '\033[0;90m'
COLOR_LIGHT_RED = '\033[0;91m'
COLOR_LIGHT_GREEN = '\033[0;92m'
COLOR_LIGHT_YELLOW = '\033[0;93m'
COLOR_LIGHT_BLUE = '\033[0;94m'
COLOR_LIGHT_MAGENTA = '\033[0;95m'
COLOR_LIGHT_CYAN = '\033[0;96m'
COLOR_WHITE = '\033[0;97m'

COLOR_BOLD_DEFAULT = '\033[1m'
COLOR_BOLD_BLACK = '\033[1;30m'
COLOR_BOLD_RED = '\033[1;31m'
COLOR_BOLD_GREEN = '\033[1;32m'
COLOR_BOLD_YELLOW = '\033[1;33m'
COLOR_BOLD_BLUE = '\033[1;34m'
COLOR_BOLD_MAGENTA = '\033[1;35m'
COLOR_BOLD_CYAN = '\033[1;36m'
COLOR_BOLD_LIGHT_GRAY = '\033[1;37m'
COLOR_BOLD_GRAY = '\033[1;90m'
COLOR_BOLD_LIGHT_REDY = '\033[1;91m'
COLOR_BOLD_LIGHT_GREEN = '\033[1;92m'
COLOR_BOLD_LIGHT_YELLOW = '\033[1;93m'
COLOR_BOLD_LIGHT_BLUE = '\033[1;94m'
COLOR_BOLD_LIGHT_MAGENTA = '\033[1;95m'
COLOR_BOLD_LIGHT_CYAN = '\033[1;96m'
COLOR_BOLD_WHITE = '\033[1;97m'

config = {
    'units': '',  # Will be set by argparse
    'max_output_width': 0,  # Will be set by argparse
    'show_diff': False,  # Filled based on inputs. Selects variant of table view/tree view
    'table_view': {
        'columns_no_diff': ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests'],
        'columns_with_diff': ['podIndex', 'workloadType', 'podName', 'type', 'name', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_ephStorageRequests', 'ref_ephStorageLimits', 'ref_PVCRequests']
    },
    'tree_view': {
        # Make sure first field is '_tree_branch'
        'columns_no_diff': ['_tree_branch', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests'],
        'columns_with_diff': ['_tree_branch', 'CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCRequests', 'change', 'ref_CPURequests', 'ref_CPULimits', 'ref_memoryRequests', 'ref_memoryLimits', 'ref_ephStorageRequests', 'ref_ephStorageLimits', 'ref_PVCRequests'],

        'pod_branch': ['podIndex', 'workloadType', 'podName'],
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
    'summary': [
        {
            'filter': '',
            'pod_text': '{filtered_pods}/{all_pods} pods using {used_pvcs}/{all_pvcs} PVCs',
            'container_text': '{filtered_containers}/{all_containers} containers'
        },
        {
            'filter': 'workloadType=^((?!Job).)*$, type=^((?!init).)*$',
            'pod_text': '{filtered_pods}/{all_pods} non-job pods using {used_pvcs}/{all_pvcs} PVCs',
            'container_text': '{filtered_containers}/{all_containers} non-init containers'
        }
    ],
    'cluster_cmd': [  # List of argv: first element is command, other - arguments; '{}; - namespace
        # ['cat', '{}']
        ['kubectl', '--namespace={}', 'get', 'pods', '--output', 'json'],
        ['kubectl', '--namespace={}', 'get', 'pvc', '--output', 'json']

    ],
    'fields': {
        # Alignment: < (left) > (right) ^ (center) - see https://docs.python.org/3/library/string.html#grammar-token-format-string-format_spec
        'appKey': {
            'header': 'App Key',
            'alignment': '<'
        },
        'appIndex': {
            'header': 'AppN',
            'alignment': '>'
        },
        'appName': {
            'header': 'Application',
            'alignment': '<'
        },
        'workloadType': {
            'header': 'Kind',
            'alignment': '<'
        },

        'podKey': {
            'header': 'Pod Key',
            'alignment': '<'
        },
        'podIndex': {
            'header': '#',
            'alignment': '>'
        },
        'podLocalIndex': {
            'header': 'PodLN',
            'alignment': '>'
        },
        'podName': {
            'header': 'Pod',
            'alignment': '<',
            'min_width': 10
        },

        'key': {
            'header': 'Container Key',
            'alignment': '<'
        },
        'index': {
            'header': 'ContN',
            'alignment': '>'
        },
        'localIndex': {
            'header': 'ContLN',
            'alignment': '>'
        },
        'type': {
            'header': 'Type',
            'alignment': '<'
        },
        'name': {
            'header': 'Container',
            'alignment': '<',
            'min_width': 10
        },

        'CPURequests': {
            'header': 'CPU_R',
            'alignment': '>'
        },
        'CPULimits': {
            'header': 'CPU_L',
            'alignment': '>'
        },
        'memoryRequests': {
            'header': 'Mem_R',
            'alignment': '>'
        },
        'memoryLimits': {
            'header': 'Mem_L',
            'alignment': '>'
        },
        'ephStorageRequests': {
            'header': 'Eph_R',
            'alignment': '>'
        },
        'ephStorageLimits': {
            'header': 'Eph_L',
            'alignment': '>'
        },

        'PVCList': {
            'header': 'PVC List',
            'alignment': '<'
        },
        'PVCQuantity': {
            'header': 'PVC_Q',
            'alignment': '>'
        },
        'PVCRequests': {
            'header': 'PVC_R',
            'alignment': '>'
        },
        'PVCList_not_found': {
            'header': 'PVC List (not found)',
            'alignment': '<'
        },

        'change': {
            'header': 'Change',
            'alignment': '<'
        },
        'changedFields': {
            'header': 'Changed Fields',
            'alignment': '<'
        },

        'ref_CPURequests': {
            'header': 'rCPU_R',
            'alignment': '>'
        },
        'ref_CPULimits': {
            'header': 'rCPU_L',
            'alignment': '>'
        },
        'ref_memoryRequests': {
            'header': 'rMem_R',
            'alignment': '>'
        },
        'ref_memoryLimits': {
            'header': 'rMem_L',
            'alignment': '>'
        },
        'ref_ephStorageRequests': {
            'header': 'rEph_R',
            'alignment': '>'
        },
        'ref_ephStorageLimits': {
            'header': 'rEph_L',
            'alignment': '>'
        },

        'ref_PVCList': {
            'header': 'rPVC List',
            'alignment': '<'
        },
        'ref_PVCQuantity': {
            'header': 'rPVC_Q',
            'alignment': '>'
        },
        'ref_PVCRequests': {
            'header': 'rPVC_R',
            'alignment': '>'
        },
        'ref_PVCList_not_found': {
            'header': 'rPVC List (not found)',
            'alignment': '<'
        },

        # Special dynamically generated fields
        '_tree_branch': {
            'header': 'Resource',
            'alignment': '<',  # Combined
            'min_width': 20
        },
        '_tree_branch_pod': {
            'header': None,  # Not used
            'alignment': None  # Not used
        },
        '_tree_branch_container': {
            'header': None,  # Not used
            'alignment': None  # Not used
        },
        '_tree_branch_summary': {
            'header': None,  # Not used
            'alignment': None  # Not used
        },
        '_tree_branch_header': {
            'header': None,  # Not used
            'alignment': None  # Not used
        }
    }  # Header, alignment (no size)
}

# Global variables
logger: Optional[logging.Logger] = None  # Will be filled in setup_logging()
args: Optional[argparse.Namespace] = None  # Will be filled in parse_args()
dump: Dict = {
    'version': 'v1',  # Version of file format
    # Roles
    'input': [],
    'reference': [],
    'error': None
}


################################################################################
# Classes
################################################################################

class ContainerListItem:
    fields: OrderedDict = OrderedDict()  # Preserving elements order is important for exporting CSV

    sym_column_separator: str

    # Static variable
    fields_width: Dict = {}

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

        self.assert_fields()

    def assert_fields(self) -> None:
        global config

        for k in self.fields.keys():
            if k not in config['fields']:
                raise AssertionError("Field '{}' is not present in the config".format(k))

        for k in config['fields']:
            if k not in self.fields:
                raise AssertionError("Field '{}' (from config) is not present in the container".format(k))

    @staticmethod
    def reset_field_widths():
        global config

        for k in config['fields'].keys():
            ContainerListItem.fields_width[k] = 0

    def generate_keys(self):
        self.fields['appKey'] = self.fields['appName']
        self.fields['podKey'] = self.fields['appKey'] + '/' + str(self.fields['podLocalIndex'])

        # Note: 'type' is added to the key for sorting (which uses key), so that init containers would go first
        self.fields['key'] = self.fields['podKey'] + '/' + self.fields['type'] + '/' + self.fields['name']

    def has_pod(self) -> bool:
        return self.fields["podName"] != ""

    def has_container(self) -> bool:
        return self.fields["name"] != ""

    # To be overloaded
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

    def is_new(self) -> bool:
        return self.fields['change'] in ['New Pod', 'New Container']

    def check_if_modified(self):
        for res_field in ['CPURequests', 'CPULimits', 'memoryRequests', 'memoryLimits', 'ephStorageRequests', 'ephStorageLimits', 'PVCList', 'PVCRequests']:
            if self.fields[res_field] != self.fields['ref_' + res_field]:
                self.fields['change'] = 'Modified'
                self.fields['changedFields'].add(res_field)

    # raw_units is needed here because this function is used by print_csv
    def get_formatted_fields(self, raw_units: bool) -> Dict:
        formatted_fields = copy.deepcopy(self.fields)

        # Make human-readable values
        if not self.is_decoration() and not raw_units:
            # CPU fields
            for field in ["CPURequests", "CPULimits", "ref_CPURequests", "ref_CPULimits"]:
                if config['units'] == 'bin':
                    formatted_fields[field] = res_cpu_millicores_to_str(value=formatted_fields[field])
                elif config['units'] == 'si':
                    formatted_fields[field] = res_cpu_millicores_to_str(value=formatted_fields[field])  # Same as bin
                elif config['units'] == 'raw':
                    pass
                else:
                    raise RuntimeError("Unknown unit type: {}".format(config['units']))

            # Memory/storage fields
            for field in [
                "memoryRequests", "memoryLimits",
                "ephStorageRequests", "ephStorageLimits",
                "PVCRequests",
                "ref_memoryRequests", "ref_memoryLimits",
                "ref_ephStorageRequests", "ref_ephStorageLimits",
                "ref_PVCRequests"
            ]:
                if config['units'] == 'bin':
                    formatted_fields[field] = res_mem_bytes_to_str_1024(value=formatted_fields[field])
                elif config['units'] == 'si':
                    formatted_fields[field] = res_mem_bytes_to_str_1000(value=formatted_fields[field])
                elif config['units'] == 'raw':
                    pass
                else:
                    raise RuntimeError("Unknown unit type: {}".format(config['units']))

        # Make sure all fields are strings
        for k, v in formatted_fields.items():
            if type(v) is set:
                formatted_fields[k] = ', '.join(v)
            elif type(v) is not str:
                formatted_fields[k] = '{}'.format(v)

        return formatted_fields

    # Special about dynamic fields: they rely on values and width of main fields
    def get_dynamic_fields(self) -> Dict:
        global config

        dynamic_fields: Dict = dict()

        tree_branch_header_indent_width: int = config['tree_view']['header_indent']
        pod_indent_width: int = config['tree_view']['pod_indent']
        container_indent_width: int = config['tree_view']['container_indent']
        summary_indent_width: int = 4

        # Pod
        columns = config['tree_view']['pod_branch']
        value = self.fields_to_table(columns=columns, highlight_changes=False, make_bold=False)
        dynamic_fields['_tree_branch_pod'] = (' ' * pod_indent_width) + value

        # Container
        columns = config['tree_view']['container_branch']
        value = self.fields_to_table(columns=columns, highlight_changes=False, make_bold=False)
        dynamic_fields['_tree_branch_container'] = (' ' * container_indent_width) + value

        # # Summary - relevant only for summary items
        dynamic_fields['_tree_branch_summary'] = (' ' * summary_indent_width) + self.fields['podName'] + ', ' + self.fields['name']

        # Header - relevant only for header items
        dynamic_fields['_tree_branch_header'] = (' ' * tree_branch_header_indent_width) + self.fields['_tree_branch']

        # Result
        return dynamic_fields

    def fields_to_table(self, columns: List, highlight_changes: bool, make_bold: bool) -> str:
        global config

        template: str = ""

        color_map = config['colors']['changes']
        if make_bold:
            color_map = config['colors']['changes_bold']

        for column in columns:
            min_width = ContainerListItem.fields_width[column]
            max_width = ContainerListItem.fields_width[column]  # Note: this requires that all values were strings
            field_template = '{' + column + ':' + config['fields'][column]['alignment'] + str(min_width) + '.' + str(max_width) + '}'

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

            if make_bold:
                template = template + field_template + COLOR_BOLD_DEFAULT + self.sym_column_separator + COLOR_RESET
            else:
                template = template + field_template + self.sym_column_separator

        formatted_fields = self.get_formatted_fields(raw_units=False)

        return template.format(**formatted_fields)

    def make_table_lines(self, with_changes: bool) -> List[str]:
        global config

        r = list()

        highlight_changes: bool = True
        if self.is_decoration():
            highlight_changes = False

        columns = config['table_view']['columns_no_diff']
        if with_changes:
            columns = config['table_view']['columns_with_diff']

        row = self.fields_to_table(columns=columns, highlight_changes=highlight_changes, make_bold=False)

        r.append(row)
        return r

    def print_table(self, with_changes: bool) -> None:
        lines = self.make_table_lines(with_changes=with_changes)
        for line in lines:
            print(line)

    def make_tree_lines(self, prev_container, with_changes: bool) -> List[str]:
        global config

        r = list()

        dynamic_fields: Dict = self.get_dynamic_fields()

        if prev_container is None or not prev_container.is_same_pod(container=self):
            # Printing additional pod line

            pod_color_map = config['colors']['changes_tree_pod_branch']

            # First column (Pod)
            tree_branch: str = dynamic_fields['_tree_branch_pod']

            # Pod: table row
            row_template = '{:' + config['fields']['_tree_branch']['alignment'] + str(ContainerListItem.fields_width['_tree_branch']) + '}'

            row_template = \
                pod_color_map[self.fields['change']] + \
                row_template + \
                COLOR_RESET

            row = row_template.format(tree_branch)

            r.append(row)

        # First column (container)
        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_container']

        # Print row
        columns = config['tree_view']['columns_no_diff']
        if with_changes:
            columns = config['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, highlight_changes=True, make_bold=False)

        r.append(row)
        return r

    def print_tree(self, prev_container, with_changes: bool) -> None:
        lines = self.make_tree_lines(prev_container=prev_container, with_changes=with_changes)
        for line in lines:
            print(line)

    def make_csv_lines(self) -> List[str]:
        r = list()

        output = io.StringIO()

        csv_writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        values = self.get_formatted_fields(raw_units=True)
        csv_writer.writerow(values.values())

        r.append(output.getvalue().strip())
        return r

    def print_csv(self) -> None:
        lines = self.make_csv_lines()
        for line in lines:
            print(line)


class ContainerListLine(ContainerListItem):
    def __init__(self):
        super().__init__()
        self.sym_column_separator = '-' * len(self.sym_column_separator)

    def reset(self):
        for k, v in ContainerListItem.fields_width.items():
            self.fields[k] = SYM_LINE * ContainerListItem.fields_width[k]

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def make_tree_lines(self, prev_container, with_changes: bool) -> List[str]:
        global config

        r = list()

        # First column
        # Already filled with right value

        # Print row
        columns = config['tree_view']['columns_no_diff']
        if with_changes:
            columns = config['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, highlight_changes=False, make_bold=False)

        # Special: printing width
        if config['max_output_width'] != 0:
            width_str = ' (width: {}/{}) '.format(str(len(row)), str(config['max_output_width']))
            span = len(width_str)
            start = row.find(SYM_LINE) + 3  # May not be first symbol because of escape combination for colors
            row = row[:start] + width_str + row[(start + span):]

        row = COLOR_BOLD_DEFAULT + row + COLOR_RESET

        r.append(row)
        return r

    def make_csv_lines(self) -> List[str]:
        raise RuntimeError('ContainerListLine is not expected to be exported to CSV')


class ContainerListHeader(ContainerListItem):
    def __init__(self):
        super().__init__()

    def reset(self):
        global config

        for k in config['fields']:
            self.fields[k] = config['fields'][k]['header']

    def is_decoration(self) -> bool:  # Header, Line etc
        return True

    def make_tree_lines(self, prev_container, with_changes: bool) -> List[str]:
        global config

        r = list()

        # First column
        dynamic_fields: Dict = self.get_dynamic_fields()
        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_header']

        # Print row
        columns = config['tree_view']['columns_no_diff']
        if with_changes:
            columns = config['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, highlight_changes=False, make_bold=True)

        r.append(row)
        return r

    def make_csv_lines(self) -> List[str]:
        for key in self.fields.keys():
            self.fields[key] = key
        return super().make_csv_lines()


class ContainerListSummary(ContainerListItem):
    def __init__(self):
        super().__init__()

    def make_tree_lines(self, prev_container, with_changes: bool) -> List[str]:
        r = list()

        # First column
        dynamic_fields: Dict = self.get_dynamic_fields()
        self.fields['_tree_branch'] = dynamic_fields['_tree_branch_summary']

        # Print row
        columns = config['tree_view']['columns_no_diff']
        if with_changes:
            columns = config['tree_view']['columns_with_diff']

        row: str = self.fields_to_table(columns=columns, highlight_changes=True, make_bold=True)

        r.append(row)
        return r

    def make_csv_lines(self):
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

    all_resources = None

    def __init__(self):
        self.reset()

    def reset(self):
        self.containers = list()
        self.pvcs = list()
        self.all_resources = None

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
    def filter(self, criteria: ContainerListItem, inverse: bool):
        r = KubernetesResourceSet()

        r.all_resources = self
        if self.all_resources is not None:
            r.all_resources = self.all_resources

        r.pvcs = self.pvcs  # TODO: Think if filter is to be applied here. May be not.

        for container in self.containers:
            matches = True
            for field in ["workloadType", "podName", "type", "name"]:
                if criteria.fields[field] == '':
                    continue

                match_by_field = bool(re.search(criteria.fields[field], container.fields[field]))
                if inverse:
                    match_by_field = not match_by_field
                matches = matches and match_by_field

            if matches:
                r.containers.append(container)

        return r

    def get_resources_total(self, with_changes: bool, pod_name_template: str, container_name_template: str = '') -> ContainerListItem:
        r = ContainerListSummary()

        stat = {
            'filtered_pods': 0,
            'all_pods': 0,
            'filtered_containers': 0,
            'all_containers': 0,
            'used_pvcs': 0,
            'ref_used_pvcs': 0,
            'all_pvcs': 0,
            'ref_all_pvcs': 0
        }

        # FILTERED pods quantity, containers quantity, sum of all resources (except PVC)
        for container in self.containers:
            for field in [
                'CPURequests', 'CPULimits',
                'memoryRequests', 'memoryLimits',
                'ephStorageRequests', 'ephStorageLimits',
                'PVCList', 'PVCList_not_found',

                'ref_CPURequests', 'ref_CPULimits',
                'ref_memoryRequests', 'ref_memoryLimits',
                'ref_ephStorageRequests', 'ref_ephStorageLimits',
                'ref_PVCList', 'ref_PVCList_not_found'
            ]:
                if type(r.fields[field]) is int:
                    r.fields[field] = r.fields[field] + container.fields[field]
                elif type(r.fields[field]) is set:
                    r.fields[field] = r.fields[field].union(container.fields[field])
                else:
                    raise RuntimeError("Invalid type of field {} used for summary: {}".format(
                        field,
                        type(r.fields[field])
                    ))

        # Filtered pods quantity, containers quantity
        stat['filtered_pods'] = self.get_pod_quantity(allow_deleted=False, allow_new=True)
        stat['filtered_containers'] = self.get_container_quantity(allow_deleted=False, allow_new=True)

        # ALL pods quantity, containers quantity
        all_resources = self.all_resources
        if all_resources is None:
            all_resources = self

        stat['all_pods'] = all_resources.get_pod_quantity(allow_deleted=False, allow_new=True)
        stat['all_containers'] = all_resources.get_container_quantity(allow_deleted=False, allow_new=True)

        # PVC quantity
        stat['all_pvcs'] = self.get_pvc_quantity(allow_deleted=False, allow_new=True)
        stat['ref_all_pvcs'] = self.get_pvc_quantity(allow_deleted=True, allow_new=False)

        stat['used_pvcs'] = len(r.fields['PVCList'])
        r.fields['PVCQuantity'] = stat['used_pvcs']

        stat['ref_used_pvcs'] = len(r.fields['ref_PVCList'])
        r.fields['ref_PVCQuantity'] = stat['ref_used_pvcs']

        # Sum of all USED PVC storage sizes
        for pvc_name in r.fields['PVCList']:
            pvc = self.get_pvc_by_name(pvc_name, allow_deleted=False, allow_new=True)
            if pvc is not None:
                r.fields['PVCRequests'] = r.fields['PVCRequests'] + pvc.fields['requests']

        for pvc_name in r.fields['ref_PVCList']:
            pvc = self.get_pvc_by_name(pvc_name, allow_deleted=True, allow_new=False)
            if pvc is not None:
                r.fields['ref_PVCRequests'] = r.fields['ref_PVCRequests'] + pvc.fields['ref_requests']

        # Other fields
        r.fields["key"] = ""
        r.fields["podKey"] = ""
        r.fields["podIndex"] = ""
        r.fields["podName"] = pod_name_template.format(**stat)
        r.fields["name"] = container_name_template.format(**stat)

        r.fields["change"] = "Unchanged"
        r.fields["changedFields"] = set()
        if with_changes:
            r.check_if_modified()

        return r

    def scale_optimal_field_width(self, scalable_fields: List, sample_line: str) -> None:
        actual_scalable_width = 0
        for field in scalable_fields:
            actual_scalable_width = actual_scalable_width + ContainerListItem.fields_width[field]

        actual_fixed_width = len(sample_line) - actual_scalable_width

        target_scalable_width = config['max_output_width'] - actual_fixed_width

        ratio: float = float(target_scalable_width) / float(actual_scalable_width)

        if ratio > 1.0:
            ratio = 1.0

        for field in scalable_fields:
            ContainerListItem.fields_width[field] = int(ContainerListItem.fields_width[field] * ratio)

            if 'min_width' in config['fields'][field]:
                ContainerListItem.fields_width[field] = max(
                    ContainerListItem.fields_width[field],
                    config['fields'][field]['min_width']
                )

    def set_optimal_field_width(self) -> None:
        global config

        ContainerListItem.reset_field_widths()

        header = ContainerListHeader()
        summary_items = self.make_summary_items(with_changes=True)  # with_changes is not important for width calculation

        # Static fields
        for container in self.containers + [header] + summary_items:  # Taking maximum length of values of all containers plus header
            str_fields = container.get_formatted_fields(raw_units=False)
            for k, v in str_fields.items():
                ContainerListItem.fields_width[k] = max(ContainerListItem.fields_width[k], len(v))

        # Dynamic fields: they rely on values and width of static fields
        for container in self.containers:  # Taking maximum length of values of all containers plus header
            str_fields = container.get_dynamic_fields()
            for k, v in str_fields.items():
                ContainerListItem.fields_width[k] = max(ContainerListItem.fields_width[k], len(v))

        for si in summary_items:
            dynamic_fields: Dict = si.get_dynamic_fields()
            ContainerListItem.fields_width['_tree_branch'] = max(
                ContainerListItem.fields_width['_tree_branch'],
                len(dynamic_fields['_tree_branch_summary'])
            )

        ContainerListItem.fields_width['_tree_branch'] = max(
            ContainerListItem.fields_width['_tree_branch'],
            ContainerListItem.fields_width['_tree_branch_pod'],
            ContainerListItem.fields_width['_tree_branch_container'],
            len(header.fields['_tree_branch'])
        )

        # Considering max_output_width
        if config['max_output_width'] < 0:  # Get terminal size
            term_cols, term_rows = shutil.get_terminal_size((999, 999))
            if term_cols == 999:  # Failure of autodetection
                term_cols = 0  # Unlimited
            config['max_output_width'] = term_cols

        if config['max_output_width'] > 0:  # 0 means do not scale
            self.scale_optimal_field_width(
                scalable_fields=['podName', 'name'],
                sample_line=header.make_table_lines(with_changes=config['show_diff'])[0]
            )
            self.scale_optimal_field_width(
                scalable_fields=['_tree_branch'],
                sample_line=header.make_tree_lines(prev_container=None, with_changes=config['show_diff'])[0]
            )

    def make_summary_items(self, with_changes: bool) -> List[ContainerListItem]:
        summary = list()

        for summary_cfg in config['summary']:
            criteria: ContainerListItem = parse_filter_expression(criteria=summary_cfg['filter'])
            filtered_subset: KubernetesResourceSet = self.filter(criteria=criteria, inverse=False)
            summary_item = filtered_subset.get_resources_total(
                with_changes=with_changes,
                pod_name_template=summary_cfg['pod_text'],
                container_name_template=summary_cfg['container_text']
            )
            summary.append(summary_item)

        return summary

    def print(self, output_format: str, with_changes: bool):
        global logger
        global config

        # Columns width (not needed for CSV)
        self.set_optimal_field_width()

        # Summary lines
        summary = self.make_summary_items(with_changes=with_changes)

        # Printing
        if output_format == "table":
            logger.debug("Output format: table")
            self.print_table(with_changes=with_changes, summary=summary)
        elif output_format == "tree":
            logger.debug("Output format: tree")
            self.print_tree(with_changes=with_changes, summary=summary)
        elif output_format == "csv":
            logger.debug("Output format: csv")
            self.print_csv()
        else:
            raise RuntimeError("Invalid output format: {}".format(output_format))

    def print_table(self, with_changes: bool, summary: Optional[List] = False):
        ContainerListHeader().print_table(with_changes=with_changes)
        ContainerListLine().print_table(with_changes=with_changes)

        for container in self.containers:
            container.print_table(with_changes=with_changes)

        ContainerListLine().print_table(with_changes=with_changes)

        for summary_item in summary:
            summary_item.print_table(with_changes=with_changes)

    def print_tree(self, with_changes: bool, summary: Optional[List] = False):
        ContainerListHeader().print_tree(prev_container=None, with_changes=with_changes)
        ContainerListLine().print_tree(prev_container=None, with_changes=with_changes)

        prev_container = None
        for container in self.containers:
            container.print_tree(prev_container=prev_container, with_changes=with_changes)
            prev_container = container

        ContainerListLine().print_tree(prev_container=None, with_changes=with_changes)

        for summary_item in summary:
            summary_item.print_tree(prev_container=None, with_changes=with_changes)

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

    def read_res_desc_from_cluster(self, namespace: str, role: str) -> List[JSON]:
        global config
        global dump

        r = list()

        dump[role].append(
            {
                'command': None,
                'return_code': None,
                'content': None,
                'stderr': None
            }
        )
        dump_item = dump[role][-1]

        for cmd_template in config['cluster_cmd']:
            cmd = list()
            for argv in cmd_template:
                cmd.append(argv.format(namespace))

            dump_item['command'] = ' '.join(cmd)  # Used for exceptions / error messages

            result: subprocess.CompletedProcess = subprocess.run(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            dump_item['return_code'] = result.returncode

            if result.returncode != 0:
                raise RuntimeError(
                    "Cannot get namespace content, return code is {}. Command: `{}`. Error: {}".format(result.returncode, dump_item['command'], result.stderr.decode('utf-8')))

            content = result.stdout.decode('utf-8')

            dump_item['content'] = content  # Needed to store text representation of the content for case when JSON parsing fails
            dump_item['stderr'] = result.stderr.decode('utf-8')

            res_desc = json.loads(content)
            dump_item['content'] = res_desc  # JSON format

            r.append(res_desc)

        return r

    def read_res_desc_from_file(self, filename: str, role: str) -> List[JSON]:
        global dump

        r = list()

        dump[role].append(
            {
                'filename': filename,
                'content': None,
            }
        )
        dump_item = dump[role][-1]

        with open(filename) as file:
            content = file.read()

        dump_item['content'] = content  # Needed to store text representation of the content for case when JSON parsing fails

        res_desc = json.loads(content)
        dump_item['content'] = res_desc

        r.append(res_desc)

        return r

    def read_res_desc(self, source: str, role: str) -> List[JSON]:
        if source[:1] == "@":
            res_desc_list = self.read_res_desc_from_cluster(namespace=source[1:], role=role)
        else:
            res_desc_list = self.read_res_desc_from_file(filename=source, role=role)

        try:
            for res_desc in res_desc_list:
                if res_desc['apiVersion'] != 'v1':
                    raise RuntimeError("Unsupported input format: expecting 'apiVersion': 'v1', but '{}' is given".format(res_desc['apiVersion']))
        except KeyError:
            raise RuntimeError("Unsupported input format: expecting apiVersion 'v1', but no apiVersion is given")

        return res_desc_list

    def load(self, source: str, role: str) -> None:
        global logger

        context = {'source': source}

        logger.debug("Parsing {}".format(context))

        res_desc_list: List[JSON] = self.read_res_desc(source=source, role=role)

        for res_desc in res_desc_list:
            res_index = 0  # TODO: Index in which res_desc?
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

        container.fields["workloadType"] = pod_desc["metadata"]["ownerReferences"][0]["kind"]

        container.fields["appName"] = pod_desc["metadata"]["ownerReferences"][0]["name"]
        if container.fields["workloadType"] == 'ReplicaSet':
            container.fields["appName"] = container.fields["appName"][:container.fields["appName"].rfind('-')]  # Delete all symbols after last '-'

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

    def get_container_quantity(self, allow_deleted: bool, allow_new: bool) -> int:
        r = 0
        for container in self.containers:
            if not allow_deleted and container.is_deleted():
                continue
            if not allow_new and container.is_new():
                continue
            r = r + 1

        return r

    def get_pod_quantity(self, allow_deleted: bool, allow_new: bool) -> int:
        pods = set()
        for container in self.containers:
            if not allow_deleted and container.is_deleted():  # Assuming no pods without containers
                continue
            if not allow_new and container.is_new():  # Assuming no pods without containers
                continue
            pods.add(container.fields['podKey'])  # Note: Cannot use name, since one pod may have different names during compare/diff

        return len(pods)

    def get_pvc_quantity(self, allow_deleted: bool, allow_new: bool) -> int:
        r = 0
        for pvc in self.pvcs:
            if not allow_deleted and pvc.is_deleted():
                continue
            if not allow_new and pvc.is_new():
                continue
            r = r + 1

        return r

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


def parse_filter_expression(criteria: str) -> ContainerListItem:
    r = ContainerListItem()

    if criteria is None:
        criteria = ''

    criteria = criteria.strip(' ')
    if criteria != '':
        for criterion in criteria.split(','):
            parts = criterion.split("=", 1)
            if len(parts) == 1:
                parts = ["podName", parts[0]]

            parts[0] = parts[0].strip(' ')

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
        r = int(value[:-1]) * 1000
    elif value[-1:] == "M":
        r = int(value[:-1]) * 1000 * 1000
    elif value[-1:] == "G":
        r = int(value[:-1]) * 1000 * 1000 * 1000
    elif value[-1:] == "T":
        r = int(value[:-1]) * 1000 * 1000 * 1000 * 1000
    elif value[-1:] == "P":
        r = int(value[:-1]) * 1000 * 1000 * 1000 * 1000 * 1000
    elif value[-1:] == "E":
        r = int(value[:-1]) * 1000 * 1000 * 1000 * 1000 * 1000 * 1000

    # Special case
    elif value[-1:] == "m":
        r = int(round(int(value[:-1]) / 1000, 0))

    elif value[-2:] == "ki":
        r = int(value[:-2]) * 1024
    elif value[-2:] == "Mi":
        r = int(value[:-2]) * 1024 * 1024
    elif value[-2:] == "Gi":
        r = int(value[:-2]) * 1024 * 1024 * 1024
    elif value[-2:] == "Ti":
        r = int(value[:-2]) * 1024 * 1024 * 1024 * 1024
    elif value[-2:] == "Pi":
        r = int(value[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024
    elif value[-2:] == "Ei":
        r = int(value[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024

    else:
        r = int(value)

    return r


def res_cpu_millicores_to_str(value: int) -> str:
    r = str(value) + "m"

    if value > 10 * 1000 - 1:
        r = str(round(float(value) / 1000, 1))

    if value == 0:
        r = '0'

    return r


def res_mem_bytes_to_str_1024(value: int) -> str:
    r = str(value)

    if value > 1 * 1024 - 1:
        r = str(round(float(value) / 1024, 1)) + "ki"

    if value > 1 * 1024 * 1024 - 1:
        r = str(round(float(value) / 1024 / 1024, 1)) + "Mi"

    if value > 1 * 1024 * 1024 * 1024 - 1:
        r = str(round(float(value) / 1024 / 1024 / 1024, 1)) + "Gi"

    if value > 1 * 1024 * 1024 * 1024 * 1024 - 1:
        r = str(round(float(value) / 1024 / 1024 / 1024 / 1024, 1)) + "Ti"

    if value > 1 * 1024 * 1024 * 1024 * 1024 * 1024 - 1:
        r = str(round(float(value) / 1024 / 1024 / 1024 / 1024 / 1024, 1)) + "Pi"

    if value > 1 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 - 1:
        r = str(round(float(value) / 1024 / 1024 / 1024 / 1024 / 1024 / 1024, 1)) + "Ei"

    if value == 0:
        r = '0'

    return r


def res_mem_bytes_to_str_1000(value: int) -> str:
    r = str(value)

    if value > 1 * 1000 - 1:
        r = str(round(float(value) / 1000, 1)) + "k"

    if value > 1 * 1000 * 1000 - 1:
        r = str(round(float(value) / 1000 / 1000, 1)) + "M"

    if value > 1 * 1000 * 1000 * 1000 - 1:
        r = str(round(float(value) / 1000 / 1000 / 1000, 1)) + "G"

    if value > 1 * 1000 * 1000 * 1000 * 1000 - 1:
        r = str(round(float(value) / 1000 / 1000 / 1000 / 1000, 1)) + "T"

    if value > 1 * 1000 * 1000 * 1000 * 1000 * 1000 - 1:
        r = str(round(float(value) / 1000 / 1000 / 1000 / 1000 / 1000, 1)) + "P"

    if value > 1 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 - 1:
        r = str(round(float(value) / 1000 / 1000 / 1000 / 1000 / 1000 / 1000, 1)) + "E"

    if value == 0:
        r = '0'

    return r


def cfg_disable_colors() -> None:
    global config
    global COLOR_NONE, COLOR_RESET, COLOR_BOLD_DEFAULT

    category: Dict
    for category_name, category in config['colors'].items():
        for k, v in category.items():
            category[k] = COLOR_NONE

    COLOR_BOLD_DEFAULT = COLOR_NONE
    COLOR_RESET = COLOR_NONE


def write_dump(filename: str):
    global dump

    content = json.dumps(dump, indent=2)
    with open(file=filename, mode='w') as file:
        file.write(content)


def parse_args():
    global args

    epilog = \
        """
        Example:
        
        ./kubestat.py -o table -r ref_pods.json -r rev_pvcs.json pods.json pvcs.json
        
        Filter criteria is a comma-separated list of 'field=regex' tokens. Fields can be specified as full names or as aliases: workloadType (kind), podName (pod), type, name (container). If field is not specified, podName is assumed. Regular expressions are case-sensitive.
        
        Examples:
        
        Filter all pods having 'abc' in the name:
        -f abc
        -f pod=abc
        -f podName=abc
        
        Filter all pods having '-abc' in the name (pattern started with '-'):
        -f (-abc)
        
        Filter all ReplicaSets:
        -f kind=Replica
        -f workloadType=Replica
        
        Filter all ReplicaSets and StatefulSets
        -f kind=Replica\\|State
        -f kind='Replica|State'
        
        Filter all pods NOT having 'abc' in the name:
        -f 'pod=^((?!abc).)*$'
        -F 'pod=abc'
        
        Filter all regular (non-init) containers in all ReplicaSets with "abc" in pod name:
        -f abc,kind=R,type=reg
        """

    parser = argparse.ArgumentParser(
        description='Provides statistics for resources from `kubectl describe pods -o json`',
        epilog=epilog,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('-d', '--dump', metavar='DUMP_FILE', dest='dump_file', type=str,
                        help='Write dump to the file')
    parser.add_argument('--color', dest='colors', action="store_true",
                        help="Enable colors")

    filter_args_group = parser.add_mutually_exclusive_group(required=False)
    filter_args_group.add_argument('-f', '--filter', dest='filter_criteria', type=str,
                                   help='Match only pods/containers matching criteria. Refer below for details.')
    filter_args_group.add_argument('-F', '--filter-not', dest='inverse_filter_criteria', type=str,
                                   help='Match only pods/containers NOT matching criteria. Refer below for details.')

    parser.add_argument('-o', '--output', dest='output_format', type=str, default='tree', choices=['tree', 'table', 'csv'],
                        help='Specify output format')
    parser.add_argument('-r', '--reference', metavar='FILE', dest='references', type=str, action='append',
                        help='Reference file(s) or @namespace to compare with')
    parser.add_argument('-u', '--units', dest='units', type=str, default='bin', choices=['bin', 'si', 'raw'],
                        help="Units of measure suffixes to use: bin (default) - 1024 based (ki, Mi, Gi), si - 1000 based (k, M, G)', raw - do not use suffixes (also for CPU)")
    parser.add_argument('-w', '--width', dest='max_output_width', type=str, default="-1",
                        help="Set desired maximum output width. -1 (terminal width, default), 0 (unlimited), N (specific width)")
    parser.add_argument(metavar="FILE", dest='inputs', type=str, nargs='+',
                        help='Input file(s) or @namespace')

    # Show help if no arguments supplied
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    parser.parse_args()

    args = parser.parse_args()


################################################################################
# Main
################################################################################

def main():
    global logger
    global config

    setup_logging()

    parse_args()

    if not args.colors:
        cfg_disable_colors()

    config['units'] = args.units
    config['max_output_width'] = int(args.max_output_width)

    all_resources = KubernetesResourceSet()
    ref_resources = KubernetesResourceSet()

    try:
        for source in args.inputs:
            all_resources.load(source=source, role='input')

        with_changes = False
        if args.references is not None:
            for source in args.references:
                ref_resources.load(source=source, role='reference')
                with_changes = True

        config['show_diff'] = with_changes  # TODO: Replace all 'with_diff' to using this config variable

        if with_changes:
            all_resources.compare(ref_resources)

        # Filtering
        resources = all_resources
        if args.filter_criteria is not None:
            resources = all_resources.filter(
                parse_filter_expression(args.filter_criteria),
                inverse=False
            )
        if args.inverse_filter_criteria is not None:
            resources = all_resources.filter(
                parse_filter_expression(args.inverse_filter_criteria),
                inverse=True
            )

        # Output
        resources.print(output_format=args.output_format, with_changes=with_changes)
    except Exception as e:
        msg = "{}".format(e)
        logger.error(msg)
        traceback.print_exc()

        dump['error'] = msg
        if args.dump_file is not None:
            write_dump(args.dump_file)

        quit(1)

    if args.dump_file is not None:
        write_dump(args.dump_file)


if __name__ == "__main__":
    main()
