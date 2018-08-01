import json
import re

from pairwise_models.model import Model
from pairwise_models.shared_w_trans_layer import SharedWeightTransLayer
from pairwise_models.emb_extra_layer import EmbExtraLayer
from pairwise_models.simple import SimpleModel
from pairwise_models.smt import SMTModel


def get_custom_models() -> dict:
    return {
        "emb_extra_layer": EmbExtraLayer,
        "smt": SMTModel,
        "simple": SimpleModel
    }


def restore_definition(filename: str) -> Model:
    params = json.load(open(filename + '.json', 'r'))
    models = get_custom_models()
    model_type = params["name"]
    if model_type not in models:
        custom_type = re.search('\([0-9]+\)(.+)@', model_type)
        if custom_type:
            model_type = custom_type.group(1)
    if model_type not in models:
        raise Exception("%s is not in a list of valid models" % model_type)
    Mdl = models[model_type]
    return Mdl.restore_definition(params)