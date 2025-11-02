# Importações
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

# --- Conexão ---
load_dotenv()

user = quote_plus(os.getenv("MONGO_USER"))
password = quote_plus(os.getenv("MONGO_PASS"))

# Conexão com banco
uri = f"mongodb+srv://{user}:{password}@nutriamdb.zb8v6ic.mongodb.net/?retryWrites=true&w=majority"
conn = MongoClient(uri)
db = conn["NutriaMDB"]

# --- Coleções ---
Ingrediente = db["ingrediente"]
Tabela = db["tabela"]
Produto = db["produto"]

# --- DataFrames ---
ingredientes = pd.DataFrame(list(Ingrediente.find()))
tabelas_nutricionais = pd.DataFrame(list(Tabela.find()))
produtos = pd.DataFrame(list(Produto.find()))
# ingredienteTabela
print("\n ----------------------------------------------")
# --- Mapeamento de tipos ---
mapa_tipos = {
    "milk": {"nome": "Milk"},
    "yogurt": {"nome": "Yogurt"},
    "dips": {"nome": "Dips"},
    "frozen dairy desserts": {"nome": "Frozen Dairy Desserts"},
    "smoothies": {"nome": "Smoothies"},
    "formula": {"nome": "Formula"},
    "cream": {"nome": "Cream"},
    "shakes": {"nome": "Shakes"},
    "pudding": {"nome": "Pudding"},
    "cakes": {"nome": "Cakes"},
    "cheese": {"nome": "Cheese"},
    "pizza": {"nome": "Pizza"},
    "eggs": {"nome": "Eggs"},
    "sandwiches": {"nome": "Sandwiches"},
    "soups": {"nome": "Soups"},
    "coleslaw": {"nome": "Coleslaw"},
    "baby food": {"nome": "Baby Food"},
    "beef": {"nome": "Beef"},
    "pork": {"nome": "Pork"},
    "bacon": {"nome": "Bacon"},
    "lamb": {"nome": "Lamb"}
}
def detectar_tipo(categoria):
    if not isinstance(categoria, str):
        return "Outro"  # ou None, se preferir
    categoria_lower = categoria.lower()
    for chave, valor in mapa_tipos.items():
        if chave in categoria_lower:
            return valor["nome"]
    return "Outro"  # caso não bata com nenhuma palavra-chave

# --- Aplicar função em todas as linhas ---
ingredientes["ctipo"] = ingredientes["cCategoria"].apply(detectar_tipo)
# -----------------------------------------------
# Criar relacionamentos ingrediente <-> tabela
# -----------------------------------------------

relacionamentos = []
i = 0
for _, tabela in tabelas_nutricionais.iterrows():
    tabela_id = tabela["_id"]
    ingredientes_lista = tabela.get("lIngredientes", [])
    if isinstance(ingredientes_lista, list):
        for item in ingredientes_lista:
            i += 1
            nCd = item.get("nCdIngrediente")
            quantidade = item.get("iQuantidade")
            if nCd is not None:
                # tenta usar o índice do DataFrame como correspondência
                if nCd < len(ingredientes):
                    ingrediente_id = ingredientes.iloc[nCd]["_id"]
                    relacionamentos.append({
                        "_id":i,
                        "ingredienteId": ingrediente_id,
                        "Quantidade":quantidade,
                        "tabelaId": tabela_id
                    })


df_relacao = pd.DataFrame(relacionamentos)

#   
# --- Preparar documentos detalhados por nutriente ---
informacoes_nutricionais = []
contador = 1  # Para gerar _id sequencial

for _, tabela in tabelas_nutricionais.iterrows():
    id_tabela = tabela["_id"]
    nutrientes = tabela.get("lNutrientes", [])
    lTotal = tabela.get("lTotal", []) 
    lPorcao = tabela.get("lPorcao", [])
    lVd = tabela.get("lVd", [])

    for i, nutriente in enumerate(nutrientes):
        total = lTotal[i] if i < len(lTotal) else None
        porcao = lPorcao[i] if i < len(lPorcao) else None
        vd = lVd[i] if i < len(lVd) else None

        doc = {
            "_id": contador,
            "idtabela": id_tabela,
            "Nutriente": nutriente,
            "total": total,
            "porcao": porcao,
            "lvd": vd
        }
        informacoes_nutricionais.append(doc)
        contador += 1

df_info_nutri = pd.DataFrame(informacoes_nutricionais)