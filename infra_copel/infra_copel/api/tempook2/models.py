"""Models da API da TempoOK."""

import pandas as pd
from pydantic import BaseModel


class SudesteModel(BaseModel):
    lim: float | None
    factor: float
    model: str


class SulModel(BaseModel):
    lim: float | None
    factor: float
    model: str


class NordesteModel(BaseModel):
    lim: float | None
    factor: float
    model: str


class NorteModel(BaseModel):
    lim: float | None
    factor: float
    model: str


class FieldModel(BaseModel):
    Sudeste: SudesteModel
    Sul: SulModel
    Nordeste: NordesteModel
    Norte: NorteModel


class DiaModel(BaseModel):
    fields: FieldModel


class CenarioModel(BaseModel):
    name: str
    prevs: bool
    favorite: bool
    configuration: list[DiaModel]


def create_dia_model(row: pd.Series) -> DiaModel:
    """
    Encapsula

    Parameters
    ----------
    row : pd.Series
        _description_

    Returns
    -------
    DiaModel
        _description_
    """
    # Mapeando os nomes das regiões para as classes correspondentes
    submercados_models = {
        "Sudeste": SudesteModel,
        "Sul": SulModel,
        "Nordeste": NordesteModel,
        "Norte": NorteModel,
    }

    # Criando um dicionário para armazenar os modelos de cada região
    models = {}

    # Iterando sobre as regiões e criando os modelos dinamicamente
    for submercado, classe_submercado in submercados_models.items():
        models[submercado] = classe_submercado(
            lim=row[(submercado, "lim")],
            factor=row[(submercado, "factor")],
            model=row[(submercado, "model")],
        )

    # Criando a instância de FieldModel com os modelos regionais
    field_model = FieldModel(
        Sudeste=models["Sudeste"],
        Sul=models["Sul"],
        Nordeste=models["Nordeste"],
        Norte=models["Norte"],
    )

    # Criando a instância de DiaModel
    dia_model = DiaModel(fields=field_model)

    return dia_model


def create_cenario_model(
    df: pd.DataFrame, name: str, prevs: bool, favorite: bool
) -> CenarioModel:
    """


    Parameters
    ----------
    df : pd.DataFrame
        _description_
    name : str
        _description_
    prevs : bool
        _description_
    favorite : bool
        _description_

    Returns
    -------
    CenarioModel
        _description_
    """
    # Gerando a lista de DiaModel a partir do DataFrame
    dia_models: list[DiaModel] = df.apply(create_dia_model, axis=1).tolist()

    # Criando a instância de CenarioModel
    cenario_model = CenarioModel(
        name=name,
        prevs=prevs,
        favorite=favorite,
        configuration=dia_models,  # Passando a lista de DiaModel
    )

    return cenario_model
