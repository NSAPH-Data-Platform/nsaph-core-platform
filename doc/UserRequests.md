# Preliminary Considerations for Handling User Requests

```{contents}
---
local:
---
```

(user-req-intro)=
## Introduction 

This document describes some considerations and
a very preliminary format
of a potential user request. It describes
possible implementations of exporting and packaging
data.

A very crude and basic
[example](members/example_request.yaml)
describes a request to export climate data for a set
of places and dates.

## Structure

 * **name**. This is a name of the request
 * **source**. Source of the request, it can indicate
   a pipeline that has to be run to obtain the data
   or another way of building the data required for
   the request. Our software wil dispatch the actual
   reuqest based on this value. Each value has to be
   specifically implemented by us.
 * **variables**. List of variables that should be included
   in the package given to the user. Our software
   will figure out how to retrieve a specified variable.
   If a way to retrieve a variable cannot be determined,
   an error will be generated. Current prototype
   implementation searches for columns with appropriate
   names in the tables defined in the module specified
   in the **source** section.
 * **restrict**. This section specifies filters to restrict
   the data included in the package. Each filter is specified
   in the format `variable: value | list of values`. If a
   variable contains sub-variables (e.g., date has years,
   months, days, etc.). Then sub-variables and their values
   can be specified. Variables used in **restrict** section
   do not necessarily be included in the **variables** section.
 * **package**.  This section defines how to package the data
   for the user. In the example we package it as HDF5,
   grouping by state and date. Grouping variables must be
   included iin the **variables** section.

