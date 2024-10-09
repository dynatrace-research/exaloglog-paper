# ExaLogLog: Space-Efficient and Practical Approximate Distinct Counting up to the Exa-Scale

This repository contains the source code to reproduce the results and figures presented in the paper "ExaLogLog: Space-Efficient and Practical Approximate Distinct Counting up to the Exa-Scale".

## Abstract
This work introduces ExaLogLog, a new data structure for approximate distinct counting, which has the same practical properties as the popular HyperLogLog algorithm. It is commutative, idempotent, mergeable, reducible, has a constant-time insert operation, and supports distinct counts up to the exa-scale. At the same time, as theoretically derived and experimentally verified, it requires 43% less space to achieve the same estimation error.

## Steps to reproduce the results and figures presented in the paper
1. Create an Amazon EC2 [c5.4xlarge](https://aws.amazon.com/ec2/instance-types/c5/) instance with Ubuntu Server 24.04 LTS and 20GiB of storage.
2. Clone the repository including submodules:
   ```
   git clone https://github.com/dynatrace-research/exaloglog-paper.git && cd exaloglog-paper && git submodule init && git submodule update
   ```
3. Install all required packages:
   ```
   sudo apt update && sudo apt --yes install openjdk-21-jdk python-is-python3 python3-pip texlive texlive-latex-extra texlive-fonts-extra texlive-science && pip install -r python/requirements.txt --break-system-packages
   ```
4. To reproduce the estimation error results `results/error/*.csv` run the `simulateEstimationErrors` task (takes ~3.5h):
   ```
   ./gradlew simulateEstimationErrors
   ```
5. To reproduce the empirically determined memory-variance product (MVP) values based on the actual allocated memory and the serialization size of different data structure implementations for approximate distinct counting run the `runEmpiricalMVPComputation` task (takes ~2.5h, not needed for the figures):
   ```
   ./gradlew runEmpiricalMVPComputation
   ```
   The results can be found in the `results\comparison-empirical-mvp` folder.
6. To calculate theoretical MVP constants as well as constants used in the Java implementation run the `calculateConstants` task (takes ~10min, not needed for the figures):
   ```
   ./gradlew calculateConstants
   ```
   The ouput can then be found in the `results/constants` folder.
7. To (re-)generate all figures in the `paper` directory execute the `pdfFigures` task (takes ~1.5min):
   ```
   ./gradlew pdfFigures
   ```
