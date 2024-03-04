#Importation des bibliotheques
import random
import string
from pyspark.sql import SparkSession


# Création  d'une instance spark session
spark = SparkSession.builder.appName("Tp1").getOrCreate()

def generate_spending(spending_categories):
    return [random.randint(100, 1000) for _ in spending_categories]

def generate_codes(n):
    codes = []
    for _ in range(n):
        letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        numbers = ''.join(random.choices(string.digits, k=8))
        code = letters + numbers
        codes.append(code)
    return codes

def fake_source():
    users = ['BAKA67300004', 'BOUT79360000', 'CONV09089808', 'DIAS03299509', 'DICH19079502',
             'FOFM64270305', 'GBEH24279505', 'JEAE20118602', 'LAFG13039809', 'LOXS25369509',
             'MEDY29339203', 'NDIA68270100', 'NIAK12339405', 'NOME15269503', 'SONJ86350009',
             'SONJ86350009', 'SOWM19289605', 'TOHD13369601']
    users += generate_codes(20)

    spending_categories = ['Compute', 'Storage', 'Networking', 'Database', 'Analytics']
    user_spendings = [(user, *generate_spending(spending_categories)) for user in users]
    return user_spendings, ['userID'] + spending_categories

def read_data_source(spark):
    data, cols = fake_source()
    return spark.createDataFrame(data=data, schema=cols)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("YourAppName") \
        .getOrCreate()
    # Appel à la fonction read_data_source
    df = read_data_source(spark)

    # Ajout d'une colonne 'Total' contenant la somme des depenses par ligne
    df = df.withColumn('Total', sum(df[col] for col in df.columns[1:]))
    # Affichage des 20 premieres lignes du DataFrame avec Pandas
    df.show(n=20)

    # Creation  d'une table temporaire pour pouvoir utiliser des requetes SQL
    resultat = df.createOrReplaceTempView("depenses")
    # Exécution d'une requête SQL pour sélectionner les 20 premières lignes
    resultat = spark.sql("SELECT * FROM depenses LIMIT 20")
    # Affichage des résultats
    resultat.show()
    

    # Afficher le nombre d'entrees de votre table resultante de votre ETL
    Nombre_entrees = spark.sql("SELECT COUNT(*) FROM depenses")
    Nombre_entrees.show()

    # Afficher la moyenne de la somme totale des dépenses
    Moyenne_totale = spark.sql("SELECT AVG(Total) FROM depenses")
    Moyenne_totale.show()

    #La liste des dépenses (incluant la sommes) de l'utilisateur dont le userID est mon code permanent
    user_id = 'NOME15269503'
    # Afficher la liste des dépenses (incluant la somme) de l'utilisateur dont le userID est mon code permanent
    user_depenses = spark.sql(f"SELECT * FROM depenses WHERE userID = '{user_id}'")
    user_depenses.show()


    spark.stop()