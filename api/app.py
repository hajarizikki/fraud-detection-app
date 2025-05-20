from flask import Flask, request, render_template
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexerModel, OneHotEncoderModel, VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
import traceback

app = Flask(__name__)

# Spark session
spark = SparkSession.builder.appName("FraudAPI").getOrCreate()

# Modèles
MODEL_PATH = "hdfs://namenode:9000/user/jovyan/models/RF_fraud_model"
INDEXER_PATH = "hdfs://namenode:9000/user/jovyan/models/type_indexer"
ENCODER_PATH = "hdfs://namenode:9000/user/jovyan/models/type_encoder"

model = RandomForestClassificationModel.load(MODEL_PATH)
indexer = StringIndexerModel.load(INDEXER_PATH)
encoder = OneHotEncoderModel.load(ENCODER_PATH)

# Colonnes utilisées dans le modèle
FEATURES = ["typeVec", "step", "amount", "oldbalanceOrg", "newbalanceOrig"]

@app.route("/", methods=["GET"])
def form():
    return render_template("form.html")

@app.route("/predict", methods=["POST"])
def predict():
    try:
        # Récupération des données du formulaire
        data = {
            "type": request.form["type"],
            "step": int(request.form["step"]),
            "amount": float(request.form["amount"]),
            "oldbalanceOrg": float(request.form["oldbalanceOrg"]),
            "newbalanceOrig": float(request.form["newbalanceOrig"])
        }

        # Préparation des données pour Spark
        df = spark.createDataFrame([data])
        df = indexer.transform(df)
        df = encoder.transform(df)
        df = df.select(*FEATURES)

        assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
        df = assembler.transform(df)

        # Prédiction
        result = model.transform(df).select("prediction", "probability").collect()[0]
        prediction = int(result["prediction"])
        probability = [float(x) for x in result["probability"]]

        # Affichage du résultat
        return render_template(
            "result.html",
            prediction=prediction,
            probability=probability,
            type=data["type"],
            step=data["step"],
            amount=data["amount"],
            oldbalanceOrg=data["oldbalanceOrg"],
            newbalanceOrig=data["newbalanceOrig"]
        )

    except Exception as e:
        traceback.print_exc()
        return render_template("result.html", error=str(e))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
