import zipfile
import argparse
import os
import sys
import csv
from pprint import pprint

VPL_INSTANCE = 'VPL 3.3.1'

class Event:
    '''
    Attributes:
        event_type (str): Taken from parameter
        event_id (int): Assigned after all events are sorted.
        order (int|str): Taken from parameter
        subject_id (str): Taken from parameter
        tool_instances (str): Taken from global constant
        code_state_id (int): Assigned after all events are sorted.
    
    '''
    def __init__(self, order, subject_id, event_type, **kwargs):
        self.order = order
        self.subject_id = subject_id
        self.event_type = event_type
        self._optional_parameters = kwargs
    
    def finalize(self, default_parameter_values):
        '''
        Arguments:
            default_parameter_values (dict of str: Any): A dictionary of the
                                                         default values for
                                                         all of the optional
                                                         parameters.
        '''
        # Avoid mutating original
        parameter_values = dict(default_parameter_values)
        parameter_values.update(self._optional_parameters)
        sorted_parameters = sorted(parameter_values.items())
        ordered_values = [value for parameter, value in sorted_parameters]
        return list(self.event_type, self.event_id, self.order,
                    self.subject_id, self.tool_instances, self.code_state_id,
                    *ordered_values)

class ProgSnap2:
    '''
    A representation of the ProgSnap2 data file being generated.
    '''
    VERSION = 3
    def __init__(self, csv_writer_options=None):
        if csv_writer_options is None:
            csv_writer_options = {'delimiter': ',', 'quotechar': '"',
                                  'quoting': csv.QUOTE_MINIMAL}
        self.csv_writer_options = csv_writer_options
        # Actual data contents
        self.main_table_header = ['EventType', 'EventID', 'Order', 'SubjectID',
                                  'ToolInstances', 'CodeStateID']
        self.main_table = []
        # Keep track of IDs
        self.code_state_id = 0
        self.event_id = 0
    
    def export(self, directory):
        self.export_metadata(directory)
        self.export_main_table(directory)
    
    def export_metadata(self, directory):
        metadata_filename = os.path.join(directory, "DatasetMetadata.csv")
        with open(metadata_filename, 'w') as metadata_file:
            metadata_writer = csv.writer(metadata_file, 
                                         **self.csv_writer_options)
            metadata_writer.writerow(['Property', 'Value'])
            metadata_writer.writerow(['Version', self.VERSION])
            metadata_writer.writerow(['AreEventsOrdered', 'true'])
            metadata_writer.writerow(['IsEventOrderingConsistent', 'true'])
            metadata_writer.writerow(['CodeStateRepresentation', 'Table'])
    
    def export_main_table(self, directory):
        main_table_filename = os.path.join(directory, "MainTable.csv")
        with open(main_table_filename, 'w', newline='') as main_table_file:
            main_table_writer = csv.writer(main_table_file, 
                                           **self.csv_writer_options)
            main_table_writer.writerow(self.main_table_header)
            for row in self.main_table:
                main_table_writer.writerow(row)
    
    def finalize_table(self):
        # Combine the disparate lists
        # Sort the timestamps|users|events
        pass
    
    def log_event(self, when, subject_id, event_type):
        self.event_id += 1
        self.main_table.append({
            event_type, self.event_id, when, 
            subject_id, 'VPL', self.code_state_id
        })
    
    def log_code(self, when, subject_id, code):
        self.event_id += 1
        self.code_state_id += 1

def add_path(structure, path):
    components = path.split("/")
    while len(components) > 1:
        current = components.pop(0)
        if current not in structure:
            structure[current] = {}
        structure = structure[current]
    if components[0]:
        structure[components[0]] = path

def load_vpl_submissions(progsnap, submissions_filename):
    '''
    Arguments:
        submissions_filename (str): The file path to the zip file.
    '''
    if not zipfile.is_zipfile(submissions_filename):
        raise Exception("I expected a Zipfile for "+str(submissions_filename))
    zipped = zipfile.ZipFile(submissions_filename)
    filesystem = {}
    for name in zipped.namelist():
        add_path(filesystem, name)
    for student, student_directory in filesystem.items():
        for timestamp, submission_directory in student_directory.items():
            if timestamp.endswith('.ceg'):
                # Same Files
                progsnap.log_event(timestamp, student, 'Compile')
                'compilation.txt'
                'execution.txt' # might be missing
                'grade.txt'
                'gradecomments.txt' # might be missing
            else:
                # Student files
                pass
    #pprint(filesystem)

def load_vpl_events(progsnap, events_filename):
    pass

def load_vpl_logs(progsnap, events_filename, submissions_filename):
    '''
    Arguments:
        events_filename (str): The file path to the CSV file
        submissions_filename (str): The file path to the zip file.
    '''
    submissions = load_vpl_submissions(progsnap, submissions_filename)
    events = load_vpl_events(progsnap, events_filename)
    #return list(students)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from VPL into the progsnap2 format.')
    parser.add_argument('events', type=str,
                        help='The events CSV source filename for the course.')
    parser.add_argument('submissions', type=str,
                        help='The submissions zip file source filename for the assignment.')
    parser.add_argument('--target', dest='target',
                        default=None,
                        help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                        default=False, action='store_true',
                        help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    progsnap = ProgSnap2()
    data = load_vpl_logs(progsnap, args.events, args.submissions)
    progsnap.export('exported/')
    print(data)
