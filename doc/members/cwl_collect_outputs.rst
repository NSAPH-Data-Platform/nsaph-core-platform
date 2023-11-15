Utility to generate outputs sections for a CWL Tool
============================================================================

Command line utility to generate output sections from
a CWL tool or sub-workflow.

Generated sections then can be copied and pasted into
any calling workflow

.. automodule:: nsaph.util.cwl_collect_outputs
   :members:
   :undoc-members:


Usage
-----

::

cwl_collect_outputs.py [-h] [--name NAME] step path

Positional arguments:
  step         Step name in the outer (calling) workflow
  path         Path to sub-workflow CWL file

Options:
  -h, --help   show this help message and exit
  --name NAME  Name to be used as output prefix, defaults to the step name
::
