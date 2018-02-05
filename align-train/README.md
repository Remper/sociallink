# align-train

Training and model generation code for pokedem-plus

## Examples

### API startup

```api.py``` implements an entry point for the API that exposes a single trained model via ```/predict?features=[x1,x2,x3,...]``` method

```
python api.py --input model
```

Where ```model``` is produced by training scripts described below


### PAI18 neural network

```train.py``` implements training of entity-candidate matching neural network. 
It expects the new json-based dataset produced by Evaluate [script][eval-script] as input. 

```
python train.py --train features.svm
```

## Installation

1. Install following ubuntu packets (or use their counterparts in other systems/build from sources):
    * libblas-dev
    * liblapack-dev
    * gfortran
    * python-numpy
    * python-scipy
    * python3-numpy
    * python3-scipy
    
2. Install python dependencies:

    ```pip install -r requirements.txt```
    
3. Install Tensorflow for your system using the official [guide][tf-guide]

[tf-guide]: https://www.tensorflow.org/install/
[eval-script]: https://github.com/Remper/sociallink/blob/master/align/src/main/java/eu/fbk/fm/alignments/Evaluate.java