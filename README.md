# "Utility-aware Semantics for Alternative Service Expressions in Federated SPARQL Queries" Supplemental Material - ICWS



This repository comprises the data for the supplemental material of the paper "Utility-aware Semantics for Alternative Service Expressions in Federated SPARQL Queries" published at ICWS 2022.
This includes the scripts to generate queries under set and utility-aware semantics for the queries from FedBench.
Furthermore, the quality descriptions as well as the resulting queries from the experimental study are provided as well.

## Installation

The scripts are written in Python 3.
The scripts require the packages `numpy` and `pandas`.
Furthermore, as the queries are executed using the [ARQ CLI](https://jena.apache.org/documentation/query/cmds.html) , the binary must be available as well and set correctly when running the `execute_query.py` script.

## Prepare Queries

The query preparation step creates the (optimized) SPARQL 1.1 queries for the query decompositions generated by FedX.

### Example Usage

The script can be run using Python. Note that the [ARQ CLI](https://jena.apache.org/documentation/query/cmds.html) must be installed to run the script.

Create a SPARQL 1.1 under set semantics query from a decomposition: 
```bash
 python3 prepare_query.py -d decompositions/type1/CD1_decomp.txt 
 -q quality_descriptions/1_qm/description_qm_1_v_1.json
``` 

Create a SPARQL 1.1 under utility-aware semantics query from a decomposition: 
```bash
 python3 prepare_query.py -o -d decompositions/type1/CD1_decomp.txt 
 -q quality_descriptions/1_qm/description_qm_1_v_1.json
``` 

The option `-v` can be used to obtain more metadata about the query.

Get Help:
```bash
python3 prepare_query.py --help
```

## Execute Queries

The query execution `execute_query.py` script is a simple interface to run queries using the ARQ CLI.
It writes the results to a CSV File and also stores statistics about the execution. 

### Example Usage

The script can be run using Python. Note that the ARQ CLI must be installed to run the script.

Execute a query:
```bash
python3 execute_query.py -q queries/examples/example_query.rq 
-a ~/apache-jena-3.6.0/bin/arq
``` 

Get Help:
```bash
python3 execute_query.py --help
``` 

## How to Cite
```
Lars Heling, Maribel Acosta. 
"Utility-aware Semantics for Alternative Service Expressions in Federated SPARQL Queries" 
IEEE International Conference On Web Services 2022.
```

## License

This project is licensed under the MIT License.
