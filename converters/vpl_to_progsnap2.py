import zipfile
import argparse
import os
import sys
import csv
from pprint import pprint

VPL_INSTANCE = 'VPL 3.3.1'

# Some events trigger at distinct timestamps.
ARBITRARY_EVENT_ORDER = [
    'Submit',
    'Compile',
    'Compile.Error',
    'Program.Run',
    'Program.Test',
    'Feedback.Grade',
]

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
    def __init__(self, server_timestamp, subject_id, event_type, **kwargs):
        self.server_timestamp = server_timestamp
        self.subject_id = subject_id
        self.event_type = event_type
        self._optional_parameters = kwargs
        self.event_id = None
        self.code_state_id = None
        self.order = None
    
    def set_ordering(self, event_id, code_state_id=None):
        '''
        Ironically, this does not set the `order` attribute, which is a
        more or less absolute representation of time. Instead this method is
        meant to update the relative attributes after all the events have
        been processed and ordered appropriately.
        '''
        self.event_id = event_id
        self.order = event_id
        if code_state_id is not None:
            self.code_state_id = code_state_id
    
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
                    self.server_timestamp,
                    *ordered_values)
    
    @staticmethod
    def distill_parameters(events):
        '''
        Given a set of events, finds all of the optional parameters by
        unioning the parameters of all the events.
        '''
        optional_parameters = set()
        for event in events:
            optional_parameters.update(event._optional_parameters)
        return optional_parameters
    
    @staticmethod
    def get_order(event):
        return self.server_timestamp

class ProgSnap2:
    '''
    A representation of the ProgSnap2 data file being generated.
    
    Directory is a 2N-tuple of N files, with each file having a name and
        contents paired together. This allows us to hash directories of
        files and perform deduplication.
    
    Attributes:
        code_files (dict of Directory: str): The dictionary mapping the
                                             filename/contents to the code
                                             instance IDs.
    '''
    VERSION = 3
    def __init__(self, csv_writer_options=None):
        if csv_writer_options is None:
            csv_writer_options = {'delimiter': ',', 'quotechar': '"',
                                  'quoting': csv.QUOTE_MINIMAL}
        self.csv_writer_options = csv_writer_options
        # Actual data contents
        self.main_table_header = ['EventType', 'EventID', 'Order', 'SubjectID',
                                  'ToolInstances', 'CodeStateID',
                                  'ServerTimestamp']
        self.main_table = []
        
        self.code_files = {tuple(): 0}
        self.CODE_ID = 1
    
    def export(self, directory):
        '''
        Create a concrete, on-disk representation of this event database.
        
        Arguments:
            directory (str): The location to store the generated files.
        '''
        self.export_metadata(directory)
        self.export_main_table(directory)
    
    def export_metadata(self, directory):
        '''
        Create the metadata table, which is more or less a constant file.
        '''
        metadata_filename = os.path.join(directory, "DatasetMetadata.csv")
        with open(metadata_filename, 'w') as metadata_file:
            metadata_writer = csv.writer(metadata_file, 
                                         **self.csv_writer_options)
            metadata_writer.writerow(['Property', 'Value'])
            metadata_writer.writerow(['Version', self.VERSION])
            metadata_writer.writerow(['AreEventsOrdered', 'true'])
            metadata_writer.writerow(['IsEventOrderingConsistent', 'true'])
            metadata_writer.writerow(['CodeStateRepresentation', 'Directory'])
    
    def export_main_table(self, directory):
        '''
        Create the main table file.
        '''
        main_table_filename = os.path.join(directory, "MainTable.csv")
        with open(main_table_filename, 'w', newline='') as main_table_file:
            main_table_writer = csv.writer(main_table_file, 
                                           **self.csv_writer_options)
            self.finalize_table()
            optionals = Event.distill_parameters(self.main_table)
            header = self.main_table_header + list(optionals.keys())
            main_table_writer.writerow(header)
            for row in self.main_table:
                finalized_row = row.finalize(optionals)
                main_table_writer.writerow(finalized_row)
    
    def finalize_table(self):
        '''
        Sort the timestamps|users|events.
        Add in event_id (and code_state_id if it's missing)
        '''
        self.main_table.sort(key= Event.get_order)
        event_id = 0
        code_state_id = 0
        subject_code_states = {}
        for event in self.main_table:
            current_code_state = subject_code_states.get(event.subject, 0)
            if event.code_state_id is None:
                event.set_ordering(event_id, current_code_state)
            else:
                event.set_ordering(event_id)
            event_id += 1
    
    def log_event(self, when, subject_id, event_type):
        new_event = Event(when, subject_id, event_type)
        self.main_table.append(new_event)
        return new_event
    
    def log_submit(self, when, subject_id, code):
        new_event = log_event(when, subject_id, 'Submit')
        new_event.code_state_id = self.hash_code_directory(code)
    
    def log_submissions(self, student, timestamp, submission_directory):
        if 'execution.txt' not in submission_directory:
            if 'compilation.txt' in submission_directory:
                compile_message_data = load_file_contents(submission_directory['compilation.txt'])
            else:
                compile_message_data = ""
            self.log_event(timestamp, student, 'Compile.Error',
                               CompileMessageType='Error',
                               CompileMessageData=compile_message_data)
        else:
            intervention_message = load_file_contents(submission_directory['execution.txt'])
            self.log_event(timestamp, student, 'Run.Program',
                               InterventionType='Feedback',
                               InterventionMessage=intervention_message)
        if 'grade.txt' in submission_directory:
            grade = float(load_file_contents(submission_directory['grade.txt']))
            self.log_event(timestamp, student, 'Feedback.Grade',
                               InterventionType='Grade',
                               InterventionMessage=grade)
    
    def hash_code_directory(self, code):
        '''
        Currently hashing just based on order received - possibly need
        something more sophisticated?
        
        Arguments:
            code (tuple of tuple of str): A series of filename/contents paired
                                          into a tuple of tuples, sorted by
                                          filenames.
        '''
        if code in self.code_files:
            code_state_id = self.code_files[code]
        else:
            code_state_id = self.CODE_ID
            self.CODE_ID += 1
        return code_state_id

def add_path(structure, path):
    components = path.split("/")
    while len(components) > 1:
        current = components.pop(0)
        if current not in structure:
            structure[current] = {}
        structure = structure[current]
    if components[0]:
        structure[components[0]] = path
        
def load_file_contents(path):
    with open(path) as a_file:
        return a_file.read()

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
                progsnap.log_submissions(student, timestamp, submission_directory)
            else:
                progsnap.log_event(timestamp, student, 'Submit')
                # Student files
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
