"""Coordenadas aproximadas dos estados brasileiros, usadas pelo mapa de pontos do painel.

Cada UF tem um ponto só, perto do centro do território. O ponto serve para posicionar um marcador por estado
no mapa e não representa o endereço de nenhuma unidade. Este módulo não depende do Spark, para poder ser testado
fora do Databricks.
"""

# UF -> (latitude, longitude), em graus decimais.
UF_CENTERS = {
    "AC": (-9.0238, -70.8120),
    "AL": (-9.5713, -36.7820),
    "AP": (1.4102, -51.7700),
    "AM": (-3.4168, -65.8561),
    "BA": (-12.5797, -41.7007),
    "CE": (-5.4984, -39.3206),
    "DF": (-15.7998, -47.8645),
    "ES": (-19.1834, -40.3089),
    "GO": (-15.8270, -49.8362),
    "MA": (-4.9609, -45.2744),
    "MT": (-12.6819, -56.9211),
    "MS": (-20.7722, -54.7852),
    "MG": (-18.5122, -44.5550),
    "PA": (-3.9843, -52.5806),
    "PB": (-7.2399, -36.7820),
    "PR": (-24.8944, -51.5500),
    "PE": (-8.8137, -36.9541),
    "PI": (-7.7183, -42.7289),
    "RJ": (-22.2500, -42.6600),
    "RN": (-5.4026, -36.9541),
    "RS": (-29.7000, -53.5000),
    "RO": (-11.5057, -63.5806),
    "RR": (2.7376, -62.0751),
    "SC": (-27.2423, -50.2189),
    "SP": (-22.1950, -48.7945),
    "SE": (-10.5741, -37.3857),
    "TO": (-10.1753, -48.2982),
}


def center_rows():
    """Linhas (uf, latitude, longitude) para montar o DataFrame de coordenadas."""
    return [(uf, latitude, longitude) for uf, (latitude, longitude) in sorted(UF_CENTERS.items())]
