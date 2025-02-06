#!/usr/bin/env python3
import os
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import sys
import argparse
import textwrap
import zipfile
import tempfile


class QueryStats:
    """ Container and pretty-printer for statistics of a slow query """
    __slots__ = ["time", "avg", "mintime", "maxtime", "count"]

    def __init__(self, time=0, avg=0, mintime=0, maxtime=0, count=1):
        if count == 1:
            self.time = self.avg = self.mintime = self.maxtime = time
            self.count = 1
        else:
            self.avg = avg
            self.mintime = mintime
            self.maxtime = maxtime
            self.count = count
            self.time = time

    def __str__(self):
        if self.count == 1:
            return f"{self.time}ms;"
        else:
            return f"{self.avg}ms (Avg); Number of queries: {self.count}; Min: {self.mintime}ms; Max: {self.maxtime}ms;"


class SlowQuery:
    """ Container and pretty-printer for slow queries """
    __slots__ = ["operation", "stats", "timeout",
                 "keyspace", "table", "is_cross_node"]

    def __init__(self, operation, stats, timeout,
                 keyspace=None, table=None, is_cross_node=False):
        self.operation = operation
        self.stats = stats
        self.timeout = timeout
        self.keyspace = keyspace
        self.table = table
        self.is_cross_node = is_cross_node

    def __str__(self):
        return (f"  Time : {self.stats} {'(cross-node)' if self.is_cross_node else ''} "
                f"\n\t{self.operation}\n")


class LogParser:
    __slots__ = [
        "queries",
        "queriesCount",
        "omittedCount",
        "sort_attribute",
        "reverse",
        "top",
        "top_count",
        "grep",
        "pattern"
    ]
    regexes = {
        'start': re.compile('DEBUG.*- (\d+) operations were slow in the last (\d+) msecs:$'),
        'single': re.compile('<(.*)>, time (\d+) msec - slow timeout (\d+) msec(/cross-node)?$'),
        'multi': re.compile('<(.*)>, was slow (\d+) times: avg/min/max (\d+)/(\d+)/(\d+) msec - slow timeout (\d+) msec(/cross-node)?$'),
        'omitted': re.compile(r'\.\.\. \((\d+) were dropped\)')
    }
    sort_keys = {
        # Sort by total time
        't': 'time',
        # Sort by average time
        'at': 'avg',
        # Sort by count
        'c': 'count'
    }

    def __init__(self, args):
        self.queries = []
        self.queriesCount = 0
        self.omittedCount = 0
        self.sort_attribute = args.sort
        self.reverse = args.reverse
        self.top = args.top is not None
        self.top_count = args.top
        self.grep = args.grep is not None
        if self.grep:
            self.pattern = re.compile(args.grep)

    def process_query(self, query):
        """ Store or print the query based on sorting requirements."""
        # If we're not sorting, we can print the queries directly. If we are
        # sorting, save the query.
        # And if we have a pattern to match, we can test it now.
        if self.grep:
            if self.pattern.search(query.operation) is None:
                return
        if self.sort_attribute:
            self.queries.append(query)
        else:
            # If we have to print only N entries, exit after doing so
            if self.top:
                if self.top_count > 0:
                    self.top_count -= 1
                else:
                    sys.exit()
            print(query)

    def parse_slow_query_stats(self, line):
        """ Return stats for a single query from log file line."""
        match = LogParser.regexes['single'].match(line)
        if match is not None:
            self.process_query(SlowQuery(
                operation=match.group(1),
                stats=QueryStats(int(match.group(2))),
                timeout=int(match.group(3)),
                is_cross_node=(match.group(4) is None)
            ))
            return
        match = LogParser.regexes['multi'].match(line)
        if match is not None:
            self.process_query(SlowQuery(
                operation=match.group(1),
                stats=QueryStats(
                    count=int(match.group(2)),
                    avg=int(match.group(3)),
                    time=int(match.group(3)),
                    mintime=int(match.group(4)),
                    maxtime=int(match.group(5))
                ),
                timeout=match.group(6),
                is_cross_node=(match.group(7) is None)
            ))
            return
        print("Could not parse the following line:\n  " + line, file=sys.stderr)
        sys.exit(1)

    def parse_log(self, infile):
        """ Extract slow queries from the log. """
        current_count = 0
        for line in infile:
            line = line.rstrip()

            # If we detect a truncation message, reset current_count
            if LogParser.regexes['omitted'].match(line):
                self.omittedCount += int(LogParser.regexes['omitted'].match(line).group(1))
                current_count = 0
                continue

            if current_count > 0:
                self.parse_slow_query_stats(line)
                current_count -= 1
                self.queriesCount += 1
            else:
                match = LogParser.regexes['start'].match(line)
                if match is None:
                    continue
                current_count = int(match.group(1))

    @staticmethod
    def get_sort_attribute(key):
        """ Convert sort option to the corresponding attribute name."""
        return LogParser.sort_keys[key]

    def sort_queries(self):
        """ Sort the queries by the previously-set key."""
        self.queries.sort(key=lambda x: getattr(x.stats,
                                                self.sort_attribute),
                          reverse=self.reverse)
        return

    def end(self):
        """ Sort and print the appropriate number of entries."""
        if self.sort_attribute:
            self.sort_queries()
            if self.top:
                self.queries = self.queries[:self.top_count]
            for q in self.queries:
                print(q)




def extract_zip(zip_path, extract_dir):
    """Extract a .zip file into a specified directory and return all extracted file paths."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)  # Extract all files
        extracted_files = [os.path.join(extract_dir, f) for f in zip_ref.namelist()]

        if not extracted_files:
            print(f"Warning: No files extracted from {zip_path}. Skipping.", file=sys.stderr)
            return []

        return extracted_files  # Return all extracted files


def main():
    """
    Provide a summary of the slow queries listed in Cassandra debug logs.
    Multiple log files can be provided, in which case the logs are combined.
    """
    arg_parser = argparse.ArgumentParser(description=textwrap.dedent(main.__doc__),
                                         formatter_class=argparse.RawTextHelpFormatter,
                                         epilog="""Sorting types (TYPE):\n\tt\t- (avg) time\n\tc\t- count""")
    arg_parser.add_argument('-s', '--sort',
                            choices=LogParser.sort_keys.values(),
                            default=False,
                            type=LogParser.get_sort_attribute,
                            metavar='TYPE',
                            help='Sort the input by %(metavar)s')
    arg_parser.add_argument('-r', '--reverse',
                            action='store_true',
                            help='Reverse the sort order')
    arg_parser.add_argument('-t', '--top',
                            type=int,
                            metavar='N',
                            help='Print only the top %(metavar)s queries')
    arg_parser.add_argument('-o', '--output',
                            metavar='FILE',
                            type=argparse.FileType('a'),
                            help='Save output to %(metavar)s')
    arg_parser.add_argument('-g', '--grep',
                            metavar='REGEX',
                            help='Print queries matching %(metavar)s')

    default_log = ["logs/debug.log"] if os.path.exists("logs/debug.log") else []
    arg_parser.add_argument('files', nargs='*', metavar='FILE',
                            default=default_log,
                            help="Input files. Standard input is '-'. Can include zip files directly. "
                                 "If none provided, tries 'logs/debug.log'.")

    args = arg_parser.parse_args()
    # Exit if no valid log files are provided
    input_files = list(filter(os.path.exists, args.files))
    if not input_files:
        print("Error: No valid log files found. Please specify an existing file."
              "You can include zip files of debug logs.", file=sys.stderr)
        sys.exit(1)

    if args.output is not None:
        sys.stdout = args.output

    parser = LogParser(args)


    log_files = []
    with tempfile.TemporaryDirectory() as temp_dir:

        for filename in input_files:
            try:
                if filename.endswith('.zip'):
                    extracted_files = extract_zip(filename, temp_dir)
                    if not extracted_files:
                        continue  # Skip empty archives
                    log_files.extend(extracted_files)  # Add extracted files to processing list
                else:
                    log_files.append(filename)  # Add regular files as they are

            except FileNotFoundError:
                print(f"Warning: File '{filename}' not found. Skipping.", file=sys.stderr)
            except PermissionError:
                print(f"Error: No permission to read '{filename}'. Skipping.", file=sys.stderr)

        # Process all extracted and regular log files
        for log_file in log_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as infile:
                    if os.stat(log_file).st_size == 0:
                        print(f"Warning: File '{log_file}' is empty. Skipping.", file=sys.stderr)
                        continue
                    parser.parse_log(infile)
            except FileNotFoundError:
                print(f"Warning: File '{log_file}' not found. Skipping.", file=sys.stderr)
            except PermissionError:
                print(f"Error: No permission to read '{log_file}'. Skipping.", file=sys.stderr)

    parser.end()

    if parser.queriesCount == 0:
        print("\nNo slow queries found in the provided logs.", file=sys.stderr)
    else:
        print(f"\n{parser.queriesCount} slow queries found in the provided logs.")
    print(f"{parser.omittedCount} slow queries were omitted, due to the monitoring limit "
          f"(change via -Dcassandra.monitoring_max_operations=<integer>.")

    print("\nProcessed files:")
    for file in input_files:
        print(f" - {file}")


if __name__ == "__main__":
    main()
