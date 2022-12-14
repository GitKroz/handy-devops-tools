# Style Guide for Python Code

This document describes style guide for Python code used in this project

## General

Shall be compliant with official [PEP 8 – Style Guide for Python Code](https://peps.python.org/pep-0008/) **with exceptions/specifications** described in this document.

## Code Layout

### Tabs or Spaces

Spaces.

### Maximum Line Length

Not limited.

Motivations: everyone has different size of monitor and can set individual soft-word wrap.

PyCharm 2022.2:
* File > Settings > Editor > Code Style:
  * Hard wrap at: 1000 
* View > Active Editor > Soft-Wrap

### Multiple Spaces before Operator

Allowed when it helps to read the code.

Example:

```python
COLOR_LIGHT_YELLOW  = '\033[0;93m'
COLOR_LIGHT_BLUE    = '\033[0;94m'
COLOR_LIGHT_MAGENTA = '\033[0;95m'
COLOR_LIGHT_CYAN    = '\033[0;96m'
```

PyCharm 2022.2:
* File > Settings > Editor > Inspections > Python
  * \[X\] PEP8 coding style violation
    * Options, Ignored errors - add 'E221'

### Programming

#### Type Hints

Use [type hints of Python v3.6](https://docs.python.org/3.6/library/typing.html) as much as possible, including return types of functions/methods.

Exception: 'self' parameter is class methods

Motivation:
* More easy code reading
* Enabling type validation by PyCharm

#### Static Methods

May not contain `@staticmethod` annotation

Motivation: to have uniform declaration of all methods

PyCharm 2022.2:
* File > Settings > Editor > Inspections > Python
   * \[ \] Method is not declared static

 