# Evagram Input

## Getting Started

### Prerequisites

The prerequisite modules required to install are the `psycopg2-binary` and `python-dotenv` libraries, and the `evagram_input` module.

```sh
pip install psycopg2-binary python-dotenv evagram_input
```

Note: Make sure you are running in a Linux environment for usage.

## Usage

Import the `evagram_input` module into your workflow:

```python
from evagram_input import *
```

The central endpoint to the module is through the `input_data` function. An example of usage is shown here:

```python
input_data(owner='postgres', experiment='experiment1', eva_directory='tests/eva')
```
