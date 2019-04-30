# progsnap2
Standards, examples, and converters for the progsnap2 file format

## progsnap2 Cheat Sheet
This section contains a bare-bones minimum with which to understand and make a dataset into progsnap2

The first thing you need to do is create a file called DatasetMetadata.csv. It should look something like this:

| Property                  | Value     |
|---------------------------|-----------|
| Version                   | 5         |
| IsEventOrderingConsistent | false     |
| EventOrderScope           |   None    |
| EventOrderScopeColumns    |           |
| CodeStateRepresentation   | Directory |

* **Version**: The current progsnap specification version you are using
* **IsEventOrderingConsistent**: Are timestamps on your data set consistent (e.g. depends on a singular clock)
* **EventOrderScope**:
	- *Global*: All events are ordered
	- *Restricted*: Events are ordered, with caveats
	- *None*: Nothing is ordered
* **EventOrderScopeColumns**: This field should be left blank if **EventOrderScope** is **NOT** *Restricted*. SubjectID a good value for this if **EventOrderScope** is *Restricted*, as it means Event order is meaningful when they are from the same user. For more details on this, look at the main specification.
* **CodeStateRepresentation**: The value &#34;Directory&#34; is the easiest to work with, so the cheat sheet assumes this value. Look at the specification for more details

Not going into too much detail, but effectively the middle three rows equate to Events are not ordered.  We'll be using this for the cheat sheet.

The second thing you need to do is create a file called MainTable.csv. It should look something like this:

| EventID | SubjectID | EventType | ToolInstances        | CodeStateID | Score |
|---------|-----------|-----------|----------------------|-------------|--------|
| 1       | 12        |   Submit  | Python 3.6.5;BlockPy | 0           |  1.00  |
| 2       | 13        |   Submit  | Python 3.6.5;BlockPy | 1           |  0.74  |
| 3       | 14        |   Submit  | Python 3.6.5;BlockPy | 2           |  1.00  |
| 4       | 15        |   Submit  | Python 3.6.5;BlockPy | 3           |  0.98  |
| 5       | 13        |   Submit  | Python 3.6.5;BlockPy | 1           |  0.74  |
| 6       | 13        |   Submit  | Python 3.6.5;BlockPy | 4           |  1.00  |

Given the MainTable.csv example above, you should have a directory that looks like so:

- root/
	- DatasetMetadata.csv
	- MainTable.csv
	- CodeStates/
		- 0/
			- \_\_main\_\_.py
		- 1/
			- \_\_main\_\_.py
			- import_file.py
		- 2/
			- \_\_main\_\_.py
			- import_file.py
		- 3/
			- \_\_main\_\_.py
			- import_file.py
		- 4/
			- \_\_main\_\_.py
			- import_file.py
		- 5/
			- \_\_main\_\_.py
			- import_file.py
		- 6/
			- \_\_main\_\_.py
			- import_file.py


Note that the folders in the CodeStates directory are named based on the *CodeStateID* column. Within that folder there should be all the files and directories associated with a particular *snapshot* or *EventID*  should be within the specified  *CodeStateID* folder. So EventID 1 maps to folder 0 in the CodeStates folder, EventID 2 maps to folder 1 in the CodeStates folder, etc. etc. Also note that student 13 submitted the same files twice, and so they refer to the same code state. This is valid if the state of the code is the SAME!

In mocked examples, there's a folder that demonstrates what is discussed in this cheat sheet! The example files are blank, as only the structure of what a ProgSnap2 data set should look like is there.