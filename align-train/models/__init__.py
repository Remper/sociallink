import json

from models.model import Model
from models.simple import SimpleModel
from models.emb_extra_layer import EmbExtraLayer
from models.emb_extra_layer_multiplication import EmbExtraLayerMul


def get_custom_models() -> dict:
    return {
        "emb_extra_layer": EmbExtraLayer,
        "emb_extra_layer_mul": EmbExtraLayerMul
    }


def restore_definition(filename: str) -> Model:
    params = json.load(open(filename + '.json', 'r'))
    models = get_custom_models()
    Mdl = SimpleModel
    if params["name"] in models:
        Mdl = models[params["name"]]
    return Mdl.restore_definition(params)