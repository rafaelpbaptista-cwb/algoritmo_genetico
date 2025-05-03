import pandas as pd

import re

RE_DC_DICT = re.compile(
    r"DC(?P<ano>\d{4})(?P<mes>\d{2})-sem(?P<semana>\d)_s(?P<estagio>\d)"
)
RE_NW_DICT = re.compile(r"NW(?P<ano>\d{4})(?P<mes>\d{2})")


def periodo_semana_dc(
    ano: str | int, mes: str | int, semana: str | int = 1, estagio: str | int = 1
):
    """
    Transforma dados de um deck do DECOMP em semana.

    Parameters
    ----------
    ano : str | int
        Ano relativo ao deck
    mes : str | int
        Mês relativo ao deck
    semana : str | int, optional
        Número da semana relativa ao deck, by default 1
    estagio : str | int, optional
        Número do estágio relativo à informação, by default 1

    Returns
    -------
    period[W-FRI]
        Semana do deck do DECOMP.
    """

    per_semanas = pd.period_range(f"{ano}-{int(mes):02}-01", periods=6, freq="W-FRI")
    n_sem = int(semana) + int(estagio) - 2

    return per_semanas[n_sem]


def periodo_nome_deck(nome_deck: str) -> pd.Period:
    """
    Transforma o nome do deck em período (semana ou mês).

    Parameters
    ----------
    nome_deck : str
        String que representa um deck do NEWAVE/DECOMP.

    Returns
    -------
    period[W-FRI] | period[M]
        Semana do deck do DECOMP ou mês do deck do NEWAVE.
    """

    if match := RE_DC_DICT.match(nome_deck):
        return periodo_semana_dc(**match.groupdict())
    elif match := RE_NW_DICT.match(nome_deck):
        data = match.groupdict()
        return pd.Period(year=int(data["ano"]), month=int(data["mes"]), freq="M")
    return None
